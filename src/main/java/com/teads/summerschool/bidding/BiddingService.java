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
        long startTime = System.nanoTime();
        metrics.recordRequest();

        BidRecord record = buildRecord(request);

        return creativeCache.getAll()
            .collectList()
            .flatMap(allCreatives -> {
                // LAYER 1: Max bid price filter (in-memory)
                List<Creative> layer1 = allCreatives.stream()
                    .filter(c -> c.isWithinMaxBid(request.floorPrice()))
                    .toList();

                if (layer1.isEmpty()) {
                    return finishNoBid(record, "floor_exceeds_max_bid", startTime);
                }

                // LAYER 2: Targeting filter (in-memory)
                List<Creative> layer2 = layer1.stream()
                    .filter(c -> c.matches(
                        request.targeting().geo(),
                        request.targeting().deviceType(),
                        request.targeting().audienceSegment()
                    ))
                    .toList();

                if (layer2.isEmpty()) {
                    return finishNoBid(record, "targeting_miss", startTime);
                }

                // LAYER 3: Budget filter (Redis - batch check all at once)
                List<String> creativeIds = layer2.stream().map(Creative::getId).toList();
                return statsCache.getRemainingBudgets(creativeIds)
                    .map(budgetMap -> layer2.stream()
                        .filter(c -> budgetMap.getOrDefault(c.getId(), 0.0) > 0)
                        .toList())
                    .flatMap(layer3 -> {
                        if (layer3.isEmpty()) {
                            return finishNoBid(record, "budget_exhausted", startTime);
                        }

                        // LAYER 4: Select best creative by specificity + maxBidPrice tiebreak
                        Creative selected = selectBestCreative(layer3);

                        // LAYER 5: Compute bid price and return response
                        double bidPrice = computeBidPrice(request, selected);

                        record.setBidPrice(bidPrice);
                        record.setCreativeId(selected.getId());
                        record.setLatencyMs((int) ((System.nanoTime() - startTime) / 1_000_000));

                        // Record in OwnBidCache for Kafka consumer
                        ownBidCache.record(request.requestId(), selected.getId(), bidPrice);

                        metrics.recordBid();
                        metrics.recordLatency(record.getLatencyMs());

                        return bidRecordRepository.save(record)
                            .thenReturn(Optional.of(new BidResponse(
                                request.requestId(),
                                bidPrice,
                                toCreativeDto(selected)
                            )));
                    });
            });
    }

    /**
     * Finish with no-bid: set reason, record metrics, save to DB.
     */
    private Mono<Optional<BidResponse>> finishNoBid(BidRecord record, String reason, long startTime) {
        record.setNoBidReason(reason);
        record.setLatencyMs((int) ((System.nanoTime() - startTime) / 1_000_000));
        metrics.recordNoBid(reason);
        metrics.recordLatency(record.getLatencyMs());
        return bidRecordRepository.save(record).thenReturn(Optional.empty());
    }

    /**
     * Select best creative from survivors using:
     * 1. Specificity score (higher = more targeted)
     * 2. Tiebreak by maxBidPrice descending (null treated as 0)
     */
    private Creative selectBestCreative(List<Creative> creatives) {
        return creatives.stream()
            .max((c1, c2) -> {
                int spec1 = computeSpecificity(c1);
                int spec2 = computeSpecificity(c2);

                if (spec1 != spec2) {
                    return Integer.compare(spec1, spec2);
                }

                // Tiebreak by maxBidPrice descending (null = 0)
                double max1 = c1.getMaxBidPrice() != null ? c1.getMaxBidPrice() : 0.0;
                double max2 = c2.getMaxBidPrice() != null ? c2.getMaxBidPrice() : 0.0;
                return Double.compare(max1, max2);
            })
            .orElseThrow(() -> new IllegalStateException("selectBestCreative called with empty list"));
    }

    /**
     * Compute specificity score: count non-empty targeting dimensions (0-3).
     * Higher score = more targeted = preferred.
     */
    private int computeSpecificity(Creative creative) {
        int score = 0;
        if (creative.getAllowedGeos() != null && !creative.getAllowedGeos().isBlank()) score++;
        if (creative.getAllowedDevices() != null && !creative.getAllowedDevices().isBlank()) score++;
        if (creative.getAudienceSegments() != null && !creative.getAudienceSegments().isBlank()) score++;
        return score;
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

        // Compute bid price synchronously using cold-start strategy
        // (segment stats lookup is too slow for 50ms Redis timeout)
        double bid = floorPrice * strategy.getColdStartMultiplier();
        log.debug("COLD-START floor={} bid={}", floorPrice, bid);
        return enforceConstraints(bid, floorPrice, creative);
    }

    /**
     * Compute bid price asynchronously with segment stats (unused - too slow for 50ms timeout).
     * Kept for reference in case timeout constraints are relaxed in future.
     */
    @SuppressWarnings("unused")
    private double computeBidPriceWithStats(BidRequest request, Creative creative) {
        double floorPrice = request.floorPrice();
        BidderProperties.Strategy strategy = properties.getStrategy();

        // Build segment key from targeting
        String segmentKey = BidderStatsCache.buildSegmentKey(
            request.targeting().geo(),
            request.targeting().deviceType(),
            request.targeting().audienceSegment()
        );

        // This would require making the entire bid() method async, which is too risky
        // given the 50ms Redis timeout constraint
        throw new UnsupportedOperationException("Stats-based bidding disabled due to 50ms Redis timeout");
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
