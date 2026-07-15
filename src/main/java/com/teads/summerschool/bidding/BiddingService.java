package com.teads.summerschool.bidding;

import com.teads.summerschool.bidding.dto.BidRequest;
import com.teads.summerschool.bidding.dto.BidResponse;
import com.teads.summerschool.bidding.dto.CreativeDto;
import com.teads.summerschool.config.BidderProperties;
import com.teads.summerschool.creative.Creative;
import com.teads.summerschool.creative.CreativeCache;
import com.teads.summerschool.metrics.BidderMetrics;
import com.teads.summerschool.record.BidRecord;
import com.teads.summerschool.record.BidRecordRepository;
import com.teads.summerschool.record.BidderStatsCache;
import com.teads.summerschool.record.OwnBidCache;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

@Service
public class BiddingService {

    private static final Logger log = LoggerFactory.getLogger(BiddingService.class);

    private final Random random = new Random();

    private final BidderProperties properties;
    private final CreativeCache creativeCache;
    private final BidRecordRepository bidRecordRepository;
    private final BidderStatsCache statsCache;
    private final BidderMetrics metrics;
    private final OwnBidCache ownBidCache;

    // Last successfully computed budget.remaining, served when a scrape's
    // computation times out instead of blocking the scrape thread forever.
    private volatile double lastKnownBudget = 0.0;

    public BiddingService(BidderProperties properties,
                          CreativeCache creativeCache,
                          BidRecordRepository bidRecordRepository,
                          BidderStatsCache statsCache,
                          BidderMetrics metrics,
                          OwnBidCache ownBidCache) {
        this.properties = properties;
        this.creativeCache = creativeCache;
        this.bidRecordRepository = bidRecordRepository;
        this.statsCache = statsCache;
        this.metrics = metrics;
        this.ownBidCache = ownBidCache;
    }

    @PostConstruct
    void registerBudgetGauge() {
        metrics.registerGauge("budget.remaining", this::getRemainingBudgetSafe);
    }

    /**
     * getRemainingBudget() does a DB query plus one Redis call per creative — under
     * DB/Redis pool contention (e.g. remote backing services with WAN latency) it can
     * queue for a connection indefinitely. /actuator/prometheus has no timeout of its
     * own, so an unbounded gauge supplier here stalls the entire scrape response.
     * Bound it the same way /api/bid bounds biddingService.bid(), and fall back to the
     * last known value instead of blocking Prometheus forever.
     *
     * <p>Micrometer's Gauge contract takes a plain synchronous Supplier<Number>, polled by the
     * Prometheus scrape thread — there's no reactive variant, so this is the one sanctioned
     * .block() outside of startup/Kafka-listener boundaries elsewhere in this codebase.
     */
    private double getRemainingBudgetSafe() {
        try {
            Double value = getRemainingBudget()
                    .timeout(Duration.ofMillis(properties.getTimeoutMs()))
                    .onErrorReturn(lastKnownBudget)
                    .block();
            lastKnownBudget = value;
            return value;
        } catch (Exception ex) {
            return lastKnownBudget;
        }
    }

    public Mono<Optional<BidResponse>> bid(BidRequest request) {
        metrics.summerschool_bids.increment();
        // TODO: implement your bidding strategy
        // Hints:
        //   1. Record the request with buildRecord(request)
        //   2. Find matching creatives with matchingCreatives(request, creativeCache.getAll())
        //   3. Filter creatives whose maxBidPrice covers this floor: c.isWithinMaxBid(request.floorPrice())
        //   4. Filter creatives that still have budget: statsCache.getRemainingBudget(c.getId()) > 0
        //      (returns a Mono<Double> — flatMap/filterWhen into it)
        //   5. Compute a bid price with computeBidPrice(request)
        //   6. Record metrics: metrics.recordRequest(), metrics.recordBid(), metrics.recordNoBid(reason)
        //   7. Call ownBidCache.record(requestId, creativeId, bidPrice) so AuctionNoticeConsumer
        //      can look this bid up without a DB round trip
        //   8. Save the BidRecord with bidRecordRepository.save(record) and return
        //      Optional.of(new BidResponse(...)) or Optional.empty()
        metrics.recordRequest();
        metrics.recordNoBid("not_implemented");
        BidRecord record = buildRecord(request);
        record.setNoBidReason("not_implemented");
        long start = System.nanoTime();
        record.setLatencyMs((int) ((System.nanoTime() - start) / 1_000_000));
        metrics.recordLatency(0);
        return bidRecordRepository.save(record).thenReturn(Optional.empty());
    }

    /**
     * Compute dynamic bid price based on segment win/loss history.
     * Strategy:
     * - Cold start (< windowSize auctions): bid floorPrice * coldStartMultiplier
     * - High win rate (>70%): shade down (winning too easily, save budget)
     * - Low win rate (<30%): bid up (losing too much, need to be more competitive)
     * - Healthy win rate (30-70%): bid base * marketMultiplier
     * - 5% exploration: random ±10% offset to prevent convergence
     */
    private double computeBidPrice(BidRequest request, Creative creative) {
        double floorPrice = request.floorPrice();
        BidderProperties.Strategy strategy = properties.getStrategy();

        // Build segment key from targeting
        String segmentKey = BidderStatsCache.buildSegmentKey(
            request.targeting().geo(),
            request.targeting().deviceType(),
            request.targeting().audienceSegment()
        );

        // Load segment stats from Redis (hot path - must be fast)
        BidderStatsCache.SegmentStats stats = statsCache.getSegmentStats(segmentKey)
            .block(Duration.ofMillis(properties.getTimeoutMs()));

        // COLD START: insufficient data, use conservative multiplier
        if (stats == null || stats.totalAuctions() < strategy.getWindowSize()) {
            double bid = floorPrice * strategy.getColdStartMultiplier();
            log.debug("COLD-START segment={} auctions={} bid={}",
                    segmentKey, stats != null ? stats.totalAuctions() : 0, bid);
            return enforceConstraints(bid, floorPrice, creative);
        }

        // WARM STATE: compute base from historical clearing prices
        double base = stats.avgClearingPrice();
        double winRate = stats.winRate();
        double marketMultiplier = strategy.getMarketMultiplier();

        double bid;
        if (winRate > 0.7) {
            // Winning too easily - shade down to save budget
            bid = base * (marketMultiplier - 0.1);
            log.debug("HIGH-WIN segment={} winRate={} base={} bid={}",
                    segmentKey, winRate, base, bid);
        } else if (winRate < 0.3) {
            // Losing too much - bid more aggressively
            bid = base * (marketMultiplier + 0.1);
            log.debug("LOW-WIN segment={} winRate={} base={} bid={}",
                    segmentKey, winRate, base, bid);
        } else {
            // Healthy win rate - maintain steady strategy
            bid = base * marketMultiplier;
            log.debug("HEALTHY segment={} winRate={} base={} bid={}",
                    segmentKey, winRate, base, bid);
        }

        // EXPLORATION: 5% of time, apply random ±10% offset
        if (Math.random() < 0.05) {
            double originalBid = bid;
            double explorationOffset = (Math.random() * 0.2) - 0.1;  // -0.1 to +0.1
            bid = bid * (1 + explorationOffset);
            log.debug("EXPLORE segment={} original={} offset={}% new={}",
                    segmentKey, originalBid, (int)(explorationOffset * 100), bid);
        }

        return enforceConstraints(bid, floorPrice, creative);
    }

    private double enforceConstraints(double bid, double floorPrice, Creative creative) {
        // Must exceed floor
        bid = Math.max(bid, floorPrice + 0.01);

        // Respect creative's max bid cap if set
        if (creative.getMaxBidPrice() != null) {
            bid = Math.min(bid, creative.getMaxBidPrice());
        }

        return bid;
    }

    /** Total remaining budget across all this bidder's creatives. */
    public Mono<Double> getRemainingBudget() {
        return creativeCache.getAll()
                .flatMap(c -> statsCache.getRemainingBudget(c.getId()))
                .reduce(0.0, Double::sum);
    }

    /** Remaining budget per creative id. */
    public Mono<Map<String, Double>> getRemainingBudgets() {
        return creativeCache.getAll()
                .flatMap(c -> statsCache.getRemainingBudget(c.getId()).map(budget -> Map.entry(c.getId(), budget)))
                .collectMap(Map.Entry::getKey, Map.Entry::getValue, LinkedHashMap::new);
    }

    private Flux<Creative> matchingCreatives(BidRequest request, Flux<Creative> all) {
        return all.filter(c -> c.matches(
                        request.targeting().geo(),
                        request.targeting().deviceType(),
                        request.targeting().audienceSegment()));
    }

    private CreativeDto toCreativeDto(Creative creative) {
        return new CreativeDto(
                creative.getId(),
                creative.getName(),
                creative.getDescription(),
                creative.getImageUrl(),
                creative.getCallToAction(),
                splitCsv(creative.getAllowedGeos()),
                splitCsv(creative.getAllowedDevices()),
                splitCsv(creative.getAudienceSegments())
        );
    }

    private List<String> splitCsv(String csv) {
        if (csv == null || csv.isBlank()) return List.of();
        return Arrays.stream(csv.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();
    }

    private BidRecord buildRecord(BidRequest request) {
        BidRecord record = new BidRecord();
        record.setRequestId(request.requestId());
        record.setFloorPrice(request.floorPrice());
        if (request.targeting() != null) {
            record.setGeo(request.targeting().geo());
            record.setDeviceType(request.targeting().deviceType());
            record.setAudienceSegment(request.targeting().audienceSegment());
        }
        return record;
    }
}
