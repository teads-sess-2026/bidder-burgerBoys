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

                // LAYER 3: Budget pre-filter (in-memory cache - fast reject of known-exhausted)
                List<String> creativeIds = layer2.stream().map(Creative::getId).toList();
                return statsCache.getRemainingBudgets(creativeIds)
                    .flatMap(budgetMap -> {
                        List<Creative> layer3 = layer2.stream()
                            .filter(c -> budgetMap.getOrDefault(c.getId(), 0.0) > 0)
                            .toList();

                        if (layer3.isEmpty()) {
                            return finishNoBid(record, "budget_exhausted", startTime);
                        }

                        // LAYER 4: Value-based skip — don't blow too much budget on one auction
                        double maxAffordable = layer3.stream()
                            .mapToDouble(c -> budgetMap.getOrDefault(c.getId(), 0.0))
                            .max().orElse(0.0)
                            * properties.getStrategy().getMaxBudgetFraction();
                        if (request.floorPrice() > maxAffordable) {
                            return finishNoBid(record, "too_expensive", startTime);
                        }

                        // LAYER 5: Rank creatives by specificity + maxBidPrice, try in order (fallback)
                        List<Creative> ranked = rankCreatives(layer3);

                        // LAYER 6: Compute bid price with stats-based shading
                        double bidPrice = computeBidPrice(request, ranked.get(0));

                        // LAYER 7: Try each creative in ranked order until one has budget
                        return tryReserveInOrder(ranked, bidPrice, request, record, startTime);
                    });
            });
    }

    /**
     * Try to reserve budget from creatives in ranked order. If the top pick is exhausted,
     * fall back to the next one instead of giving up.
     */
    private Mono<Optional<BidResponse>> tryReserveInOrder(
            List<Creative> ranked, double bidPrice, BidRequest request, BidRecord record, long startTime) {

        Mono<Optional<BidResponse>> chain = Mono.defer(() ->
            finishNoBid(record, "budget_exhausted", startTime));

        for (int i = ranked.size() - 1; i >= 0; i--) {
            final Creative candidate = ranked.get(i);
            final double candidateBid = enforceConstraints(bidPrice, request.floorPrice(), candidate);
            final Mono<Optional<BidResponse>> fallback = chain;

            chain = statsCache.reserveBudget(candidate.getId(), candidateBid)
                .flatMap(remaining -> {
                    record.setBidPrice(candidateBid);
                    record.setCreativeId(candidate.getId());
                    record.setLatencyMs((int) ((System.nanoTime() - startTime) / 1_000_000));

                    ownBidCache.record(request.requestId(), candidate.getId(), candidateBid);

                    metrics.recordBid();
                    metrics.recordLatency(record.getLatencyMs());

                    return bidRecordRepository.save(record)
                        .thenReturn(Optional.of(new BidResponse(
                            request.requestId(),
                            candidateBid,
                            toCreativeDto(candidate)
                        )));
                })
                .switchIfEmpty(Mono.defer(() -> fallback));
        }

        return chain;
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
     * Rank creatives from best to worst using:
     * 1. Specificity score (higher = more targeted)
     * 2. Tiebreak by maxBidPrice descending (null treated as 0)
     */
    private List<Creative> rankCreatives(List<Creative> creatives) {
        return creatives.stream()
            .sorted((c1, c2) -> {
                int spec1 = computeSpecificity(c1);
                int spec2 = computeSpecificity(c2);

                if (spec1 != spec2) {
                    return Integer.compare(spec2, spec1); // descending
                }

                double max1 = c1.getMaxBidPrice() != null ? c1.getMaxBidPrice() : 0.0;
                double max2 = c2.getMaxBidPrice() != null ? c2.getMaxBidPrice() : 0.0;
                return Double.compare(max2, max1); // descending
            })
            .toList();
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
     * Compute bid price using stats-based shading for first-price auctions.
     * In first-price, you pay exactly what you bid — so the optimal bid is the
     * minimum needed to win. Uses in-memory cached segment stats (no Redis call on hot path).
     */
    private double computeBidPrice(BidRequest request, Creative creative) {
        double floorPrice = request.floorPrice();
        BidderProperties.Strategy strategy = properties.getStrategy();

        String segmentKey = BidderStatsCache.buildSegmentKey(
            request.targeting().geo(),
            request.targeting().deviceType(),
            request.targeting().audienceSegment()
        );

        BidderStatsCache.SegmentStats stats = statsCache.getSegmentStatsCached(segmentKey);

        double multiplier;
        if (stats.totalAuctions() < strategy.getMinSamples()) {
            multiplier = strategy.getColdStartMultiplier();
        } else if (stats.winRate() > 0.60) {
            // Winning too easily — shade down aggressively to save budget
            multiplier = strategy.getMarketMultiplier();
        } else if (stats.winRate() < 0.30) {
            // Losing too much — bid higher to be competitive
            multiplier = strategy.getPacingBoost();
        } else {
            // Healthy win rate — bid just above floor
            multiplier = strategy.getColdStartMultiplier();
        }

        // 5% exploration: small random offset to discover price sensitivity
        if (random.nextDouble() < 0.05) {
            multiplier += (random.nextDouble() - 0.5) * 0.04; // ±2% jitter
        }

        double bid = floorPrice * multiplier;
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
