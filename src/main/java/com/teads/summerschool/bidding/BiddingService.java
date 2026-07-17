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
import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
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

        // Short-circuit if competition is over
        if (getPacingMultiplier() == 0.0) {
            return finishNoBid(record, "competition_over", startTime);
        }

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
                // Use a negative threshold to tolerate in-flight reservations that may be
                // refunded. Only reject creatives that are deeply negative (truly spent).
                List<String> creativeIds = layer2.stream().map(Creative::getId).toList();
                return statsCache.getRemainingBudgets(creativeIds)
                    .flatMap(budgetMap -> {
                        double reservationBuffer = -request.floorPrice() * 5;
                        List<Creative> layer3 = layer2.stream()
                            .filter(c -> budgetMap.getOrDefault(c.getId(), 0.0) > reservationBuffer)
                            .toList();

                        if (layer3.isEmpty()) {
                            return finishNoBid(record, "budget_exhausted", startTime);
                        }

                        // LAYER 4: Rank creatives by specificity + budget health
                        List<Creative> ranked = rankCreatives(layer3);

                        // LAYER 5: Try each creative in ranked order, computing bid price per candidate
                        return tryReserveInOrder(ranked, request, record, startTime);
                    });
            });
    }

    /**
     * Try to reserve budget from creatives in ranked order. Computes bid price
     * individually per candidate so each creative's constraints are respected.
     * If the top pick is exhausted, fall back to the next one.
     */
    private Mono<Optional<BidResponse>> tryReserveInOrder(
            List<Creative> ranked, BidRequest request, BidRecord record, long startTime) {

        Mono<Optional<BidResponse>> chain = Mono.defer(() ->
            finishNoBid(record, "budget_exhausted", startTime));

        for (int i = ranked.size() - 1; i >= 0; i--) {
            final Creative candidate = ranked.get(i);
            OptionalDouble constrained = enforceConstraints(bidPrice, request.floorPrice(), candidate);
            if (constrained.isEmpty()) {
                continue;
            }
            final double candidateBid = constrained.getAsDouble();
            final Mono<Optional<BidResponse>> fallback = chain;

            chain = statsCache.reserveBudget(candidate.getId(), candidateBid)
                .flatMap(remaining -> {
                    record.setBidPrice(candidateBid);
                    record.setCreativeId(candidate.getId());
                    record.setLatencyMs((int) ((System.nanoTime() - startTime) / 1_000_000));

                    ownBidCache.record(request.requestId(), candidate.getId(), candidateBid,
                        request.targeting() != null ? request.targeting().geo() : null,
                        request.targeting() != null ? request.targeting().deviceType() : null,
                        request.targeting() != null ? request.targeting().audienceSegment() : null);

                    metrics.recordBid();
                    metrics.recordLatency(record.getLatencyMs());

                    // Fire-and-forget: don't block the bid response on Postgres write
                    bidRecordRepository.save(record).subscribe();

                    return Mono.just(Optional.of(new BidResponse(
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
     * Finish with no-bid: set reason, record metrics, save to DB (fire-and-forget).
     */
    private Mono<Optional<BidResponse>> finishNoBid(BidRecord record, String reason, long startTime) {
        record.setNoBidReason(reason);
        record.setLatencyMs((int) ((System.nanoTime() - startTime) / 1_000_000));
        metrics.recordNoBid(reason);
        metrics.recordLatency(record.getLatencyMs());
        bidRecordRepository.save(record).subscribe();
        return Mono.just(Optional.empty());
    }

    /**
     * Rank creatives from best to worst using a composite score that balances
     * specificity against budget health. A nearly-exhausted high-specificity creative
     * yields to a healthier lower-specificity one, preventing drain-then-die behavior.
     */
    private List<Creative> rankCreatives(List<Creative> creatives) {
        double defaultBudget = properties.getCreativeBudget();
        return creatives.stream()
            .sorted((c1, c2) -> {
                double score1 = computeRankScore(c1, defaultBudget);
                double score2 = computeRankScore(c2, defaultBudget);
                return Double.compare(score2, score1); // descending
            })
            .toList();
    }

    private double computeRankScore(Creative creative, double defaultBudget) {
        int specificity = computeSpecificity(creative);
        double remaining = statsCache.getCachedRemainingBudget(creative.getId());
        double budgetHealth = Math.min(remaining / defaultBudget, 1.0);

        // When budget is critically low (<20%), penalize heavily regardless of specificity.
        // This forces traffic to shift to healthier creatives before atomic reserve fails.
        if (budgetHealth < 0.2) {
            return budgetHealth * 2.0;
        }

        // Specificity (0-3) weighted equally with budget health (0-3 range).
        // A spec-2 creative at 50% health (score=2+1.5=3.5) loses to
        // a spec-1 creative at 100% health (score=1+3.0=4.0) — load spreads.
        return specificity + budgetHealth * 3.0;
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
     * Returns a pacing multiplier based on how much time vs budget has been consumed.
     * > 1.0 means we're behind pace (spend more aggressively),
     * < 1.0 means we're ahead of pace (be more selective / bid lower).
     * Returns 1.0 if pacing is disabled (no startTime configured).
     */
    private double getPacingMultiplier() {
        BidderProperties.Competition comp = properties.getCompetition();
        if (comp == null || comp.getStartTime() == null || comp.getStartTime().isBlank()) {
            return 1.0;
        }

        Instant start;
        try {
            start = Instant.parse(comp.getStartTime());
        } catch (Exception e) {
            return 1.0;
        }

        long durationMs = comp.getDurationSeconds() * 1000;
        long elapsed = System.currentTimeMillis() - start.toEpochMilli();

        if (elapsed <= 0) return 1.0; // competition hasn't started
        if (elapsed >= durationMs) return 0.0; // competition is over, stop spending

        double timeFraction = (double) elapsed / durationMs;
        double initialBudget = statsCache.getInitialTotalBudget();
        if (initialBudget <= 0) return 1.0;
        double remaining = statsCache.getCachedTotalRemainingBudget();
        double spent = initialBudget - remaining;
        double budgetFraction = spent / initialBudget;

        // ratio > 1 means spending faster than time is passing (ahead of pace)
        // ratio < 1 means spending slower than time (behind pace)
        double ratio = (timeFraction > 0) ? budgetFraction / timeFraction : 0.0;

        // Urgency: in the last 20% of time, if we still have significant budget left, be aggressive
        double urgency = 1.0;
        if (timeFraction > 0.8) {
            double budgetRemaining = 1.0 - budgetFraction;
            urgency = 1.0 + budgetRemaining * (timeFraction / 1.0);
        }
        // Cap urgency to prevent overpay storms in endgame
        urgency = Math.min(urgency, 1.3);

        double pacingFactor;
        if (ratio > 1.3) {
            pacingFactor = properties.getStrategy().getPacingCut();
        } else if (ratio > 1.1) {
            // Slightly ahead — mild cut
            pacingFactor = 0.95;
        } else if (ratio < 0.5) {
            // Far behind pace — strong boost
            pacingFactor = 1.15;
        } else if (ratio < 0.8) {
            // Behind pace — moderate boost
            pacingFactor = 1.08;
        } else {
            pacingFactor = 1.0;
        }

        return pacingFactor * urgency;
    }

    /**
     * Compute bid price using stats-based shading for first-price auctions.
     * In first-price, you pay exactly what you bid — so the optimal bid is the
     * minimum needed to win. Uses observed clearing/loss prices to anchor bids to
     * actual market levels instead of blindly scaling the floor.
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

        double bid;
        if (stats.totalAuctions() < strategy.getMinSamples()) {
            // Cold start: no market data yet, bid aggressively to win early and gather data
            bid = floorPrice * strategy.getColdStartMultiplier();
        } else if (stats.winRate() > 0.60) {
            // Winning too easily — shade down toward observed clearing prices to save budget
            double target = stats.avgClearingPrice() > 0
                ? stats.avgClearingPrice() * strategy.getMarketMultiplier()
                : floorPrice * strategy.getMarketMultiplier();
            bid = Math.max(target, floorPrice + 0.01);
        } else if (stats.winRate() < 0.30) {
            // Losing too much — bid significantly above our losing bids
            double lossReference = stats.avgLossPrice() > 0 ? stats.avgLossPrice() : floorPrice;
            bid = lossReference * 1.20;
        } else {
            // Healthy win rate (30-60%) — bid just above observed clearing to stay competitive
            double target = stats.avgClearingPrice() > 0
                ? stats.avgClearingPrice() * 1.05
                : floorPrice * strategy.getColdStartMultiplier();
            bid = Math.max(target, floorPrice + 0.01);
        }

        // Apply pacing: shade bids down if ahead of budget pace, up if behind
        bid *= getPacingMultiplier();

        // Cap per-bid spend to maxBudgetFraction of remaining budget
        double remaining = statsCache.getCachedRemainingBudget(creative.getId());
        double maxAllowed = remaining * strategy.getMaxBudgetFraction();
        if (maxAllowed > floorPrice) {
            bid = Math.min(bid, maxAllowed);
        }

        // 5% exploration: small random offset to discover price sensitivity
        if (random.nextDouble() < 0.05) {
            double jitter = (random.nextDouble() - 0.5) * 0.04 * bid;
            bid += jitter;
        }

        return enforceConstraints(bid, floorPrice, creative).orElse(floorPrice + 0.01);
    }

    private OptionalDouble enforceConstraints(double bid, double floorPrice, Creative creative) {
        // Must exceed floor
        bid = Math.max(bid, floorPrice + 0.01);

        // Respect creative's max bid cap if set
        if (creative.getMaxBidPrice() != null) {
            bid = Math.min(bid, creative.getMaxBidPrice());
        }

        // Constraints are unsatisfiable if the cap pushed us at or below the floor
        if (bid <= floorPrice) {
            return OptionalDouble.empty();
        }

        return OptionalDouble.of(bid);
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
