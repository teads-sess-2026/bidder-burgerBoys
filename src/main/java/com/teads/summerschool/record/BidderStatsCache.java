package com.teads.summerschool.record;

import com.teads.summerschool.config.BidderProperties;
import com.teads.summerschool.creative.CreativeRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-creative budget cache backed by Redis.
 *
 * <p>Key format: {@code {bidderId}_{creativeId}_budget}, value = remaining budget.
 * Each creative has its own budget limit; remaining decreases on each Kafka-confirmed
 * win for that creative. Both this bidder and the SSP read these keys to decide whether
 * a creative can still spend. Postgres's {@code creatives.budget} column is kept in sync
 * with the same remaining value so it isn't lost if Redis is wiped.
 */
@Component
public class BidderStatsCache {

    /**
     * Win/loss statistics for a targeting segment.
     */
    public record SegmentStats(
        int totalAuctions,
        int totalWins,
        double avgClearingPrice,
        double winRate,
        double avgLossPrice
    ) {
        public static SegmentStats empty() {
            return new SegmentStats(0, 0, 0.0, 0.0, 0.0);
        }

        public static SegmentStats fromHashData(
            int totalAuctions,
            int totalWins,
            double sumClearingPrices,
            double sumLossPrices
        ) {
            if (totalAuctions == 0) return empty();

            double winRate = (double) totalWins / totalAuctions;
            double avgClearingPrice = totalWins > 0 ? sumClearingPrices / totalWins : 0.0;
            int losses = totalAuctions - totalWins;
            double avgLossPrice = losses > 0 ? sumLossPrices / losses : 0.0;

            return new SegmentStats(totalAuctions, totalWins, avgClearingPrice, winRate, avgLossPrice);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(BidderStatsCache.class);

    // KEYS[1] = budget key, ARGV[1] = default budget (lazy init), ARGV[2] = amount to reserve.
    // Returns new remaining if reservation succeeded (>= 0), or -1 if insufficient budget.
    private static final RedisScript<Double> RESERVE_BUDGET_SCRIPT = RedisScript.of("""
            if redis.call('EXISTS', KEYS[1]) == 0 then
                redis.call('SET', KEYS[1], ARGV[1])
            end
            local current = tonumber(redis.call('GET', KEYS[1]))
            local amount = tonumber(ARGV[2])
            if current < amount then
                return -1
            end
            return redis.call('INCRBYFLOAT', KEYS[1], -amount)
            """, Double.class);

    // KEYS[1] = budget key, ARGV[1] = amount to refund. Simple add-back, no guard needed.
    private static final RedisScript<Double> REFUND_BUDGET_SCRIPT = RedisScript.of("""
            return redis.call('INCRBYFLOAT', KEYS[1], tonumber(ARGV[1]))
            """, Double.class);

    private static final RedisScript<Void> RECORD_SEGMENT_WIN_SCRIPT = RedisScript.of("""
            redis.call('HINCRBY', KEYS[1], 'totalAuctions', 1)
            redis.call('HINCRBY', KEYS[1], 'totalWins', 1)
            redis.call('HINCRBYFLOAT', KEYS[1], 'sumClearingPrices', tonumber(ARGV[1]))
            """, Void.class);

    private static final RedisScript<Void> RECORD_SEGMENT_LOSS_SCRIPT = RedisScript.of("""
            redis.call('HINCRBY', KEYS[1], 'totalAuctions', 1)
            redis.call('HINCRBYFLOAT', KEYS[1], 'sumLossPrices', tonumber(ARGV[1]))
            """, Void.class);


    private final BidderProperties properties;
    private final ReactiveRedisTemplate<String, String> redis;
    private final CreativeRepository creativeRepository;

    private final AtomicLong winCount = new AtomicLong(0);
    private final Deque<Double> recentWinPrices = new ArrayDeque<>();

    // In-memory budget cache with 2-second TTL for hot path
    private final java.util.concurrent.ConcurrentHashMap<String, CachedBudget> budgetCache =
        new java.util.concurrent.ConcurrentHashMap<>();

    private static class CachedBudget {
        final double budget;
        final long timestamp;

        CachedBudget(double budget) {
            this.budget = budget;
            this.timestamp = System.currentTimeMillis();
        }

        boolean isExpired() {
            return System.currentTimeMillis() - timestamp > 2000; // 2 seconds TTL
        }
    }

    public BidderStatsCache(BidderProperties properties, ReactiveRedisTemplate<String, String> redis,
                             CreativeRepository creativeRepository) {
        this.properties = properties;
        this.redis = redis;
        this.creativeRepository = creativeRepository;
    }

    /** Redis key holding the remaining budget for one creative. */
    public String budgetKey(String creativeId) {
        return properties.getId() + "_" + creativeId + "_budget";
    }

    /** Redis key holding stats for a targeting segment. */
    public String segmentStatsKey(String segmentKey) {
        return properties.getId() + "_stats_" + segmentKey;
    }

    /** Build segment key from targeting dimensions. */
    public static String buildSegmentKey(String geo, String deviceType, String audienceSegment) {
        String g = geo == null ? "" : geo;
        String d = deviceType == null ? "" : deviceType;
        String a = audienceSegment == null ? "" : audienceSegment;
        return g + "_" + d + "_" + a;
    }

    private int parseInt(String value) {
        if (value == null || value.isEmpty()) return 0;
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private double parseDouble(String value) {
        if (value == null || value.isEmpty()) return 0.0;
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }

    /** Set a creative's remaining budget to its full limit. Called once per creative on startup. */
    public Mono<Boolean> initBudget(String creativeId, double budget) {
        String key = budgetKey(creativeId);
        return redis.opsForValue().set(key, String.valueOf(budget))
                .doOnNext(ok -> log.info("Creative budget initialized: {} = {}", key, budget));
    }

    /**
     * Atomically reserve bidPrice from a creative's budget before placing a bid.
     * Returns the new remaining budget if reservation succeeded, or empty Mono if insufficient funds.
     */
    public Mono<Double> reserveBudget(String creativeId, double bidPrice) {
        String key = budgetKey(creativeId);
        return redis.execute(RESERVE_BUDGET_SCRIPT,
                        List.of(key),
                        List.of(String.valueOf(properties.getCreativeBudget()), String.valueOf(bidPrice)))
                .next()
                .flatMap(result -> {
                    if (result < 0) {
                        budgetCache.put(creativeId, new CachedBudget(0.0));
                        return Mono.empty();
                    }
                    budgetCache.put(creativeId, new CachedBudget(result));
                    log.debug("RESERVE key={} amount={} remaining={}", key, bidPrice, result);
                    return Mono.just(result);
                });
    }

    /**
     * Refund a previously reserved amount back to the creative's budget (on loss or overpayment).
     */
    public Mono<Double> refundBudget(String creativeId, double amount) {
        if (amount <= 0) return Mono.just(0.0);
        String key = budgetKey(creativeId);
        return redis.execute(REFUND_BUDGET_SCRIPT,
                        List.of(key),
                        List.of(String.valueOf(amount)))
                .next()
                .doOnNext(after -> {
                    budgetCache.put(creativeId, new CachedBudget(after));
                    log.debug("REFUND  key={} amount={} remaining={}", key, amount, after);
                });
    }

    /**
     * On win: refund the difference between reserved bidPrice and actual clearingPrice,
     * then sync to Postgres.
     */
    public Mono<Double> recordWin(String creativeId, double clearingPrice, double bidPrice) {
        double refundAmount = bidPrice - clearingPrice;
        return refundBudget(creativeId, refundAmount)
                .doOnNext(after -> log.info("BUDGET  key={} clearing={} refund={} remaining={}",
                        budgetKey(creativeId), clearingPrice, refundAmount, after))
                .flatMap(after -> creativeRepository.findById(creativeId)
                        .flatMap(c -> {
                            c.setBudget(after);
                            return creativeRepository.save(c);
                        })
                        .thenReturn(after))
                .doOnNext(after -> {
                    winCount.incrementAndGet();
                    synchronized (recentWinPrices) {
                        recentWinPrices.addLast(clearingPrice);
                        if (recentWinPrices.size() > properties.getStrategy().getWindowSize()) {
                            recentWinPrices.pollFirst();
                        }
                    }
                });
    }

    /**
     * On loss: refund the entire reserved bidPrice back to the creative's budget.
     */
    public Mono<Double> recordLoss(String creativeId, double bidPrice) {
        return refundBudget(creativeId, bidPrice)
                .doOnNext(after -> log.info("LOSS-REFUND key={} refund={} remaining={}",
                        budgetKey(creativeId), bidPrice, after))
                .flatMap(after -> creativeRepository.findById(creativeId)
                        .flatMap(c -> {
                            c.setBudget(after);
                            return creativeRepository.save(c);
                        })
                        .thenReturn(after));
    }

    /** Remaining budget for a creative. Lazily initializes to the flat creative budget if missing. */
    public Mono<Double> getRemainingBudget(String creativeId) {
        String key = budgetKey(creativeId);
        double defaultBudget = properties.getCreativeBudget();
        return redis.opsForValue().get(key)
                .flatMap(val -> {
                    try {
                        return Mono.just(Double.parseDouble(val));
                    } catch (NumberFormatException e) {
                        return Mono.just(defaultBudget);
                    }
                })
                .switchIfEmpty(redis.opsForValue().setIfAbsent(key, String.valueOf(defaultBudget))
                        .thenReturn(defaultBudget));
    }

    /**
     * Batch check remaining budgets for multiple creatives using in-memory cache first, then MGET.
     * Returns a map of creativeId -> remainingBudget.
     * HOT PATH: must complete in <10ms for typical creative counts (2-4).
     */
    public Mono<java.util.Map<String, Double>> getRemainingBudgets(List<String> creativeIds) {
        if (creativeIds.isEmpty()) {
            return Mono.just(java.util.Map.of());
        }

        java.util.Map<String, Double> result = new java.util.HashMap<>();
        List<String> needsFetch = new java.util.ArrayList<>();

        // Fast path: check in-memory cache
        for (String creativeId : creativeIds) {
            CachedBudget cached = budgetCache.get(creativeId);
            if (cached != null && !cached.isExpired()) {
                result.put(creativeId, cached.budget);
            } else {
                needsFetch.add(creativeId);
            }
        }

        // If all found in cache, return immediately
        if (needsFetch.isEmpty()) {
            return Mono.just(result);
        }

        // Slow path: fetch missing budgets from Redis
        double defaultBudget = properties.getCreativeBudget();
        List<String> keys = needsFetch.stream().map(this::budgetKey).toList();

        return redis.opsForValue().multiGet(keys)
            .map(values -> {
                for (int i = 0; i < needsFetch.size(); i++) {
                    String creativeId = needsFetch.get(i);
                    String value = values != null && i < values.size() ? values.get(i) : null;

                    double budget;
                    if (value != null) {
                        try {
                            budget = Double.parseDouble(value);
                        } catch (NumberFormatException e) {
                            budget = defaultBudget;
                        }
                    } else {
                        budget = defaultBudget;
                    }

                    result.put(creativeId, budget);
                    budgetCache.put(creativeId, new CachedBudget(budget));
                }
                return result;
            })
            .defaultIfEmpty(result)
            .map(map -> {
                // Fill in defaults for any still missing
                for (String id : needsFetch) {
                    map.putIfAbsent(id, defaultBudget);
                }
                return map;
            });
    }

    public long getWinCount() {
        return winCount.get();
    }

    public double getRollingAverageWinPrice() {
        synchronized (recentWinPrices) {
            if (recentWinPrices.isEmpty()) return 0.0;
            return recentWinPrices.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
        }
    }

    public long getSampleCount() {
        return winCount.get();
    }

    /**
     * Record a win for a targeting segment, updating aggregate stats.
     * Called from Kafka consumer (async, not hot path).
     */
    public Mono<Void> recordWinForSegment(String segmentKey, double clearingPrice, double ourBidPrice) {
        String key = segmentStatsKey(segmentKey);
        return redis.execute(
                RECORD_SEGMENT_WIN_SCRIPT,
                List.of(key),
                List.of(String.valueOf(clearingPrice), String.valueOf(ourBidPrice))
        )
        .then()
        .doOnSuccess(v -> log.debug("SEG-WIN  key={} clearing={}", key, clearingPrice));
    }

    /**
     * Record a loss for a targeting segment, updating aggregate stats.
     * Called from Kafka consumer (async, not hot path).
     */
    public Mono<Void> recordLossForSegment(String segmentKey, double ourBidPrice) {
        String key = segmentStatsKey(segmentKey);
        return redis.execute(
                RECORD_SEGMENT_LOSS_SCRIPT,
                List.of(key),
                List.of(String.valueOf(ourBidPrice))
        )
        .then()
        .doOnSuccess(v -> log.debug("SEG-LOSS key={} bid={}", key, ourBidPrice));
    }

    // In-memory cache for segment stats with 10-second TTL
    private final java.util.concurrent.ConcurrentHashMap<String, CachedSegmentStats> segmentStatsCache =
        new java.util.concurrent.ConcurrentHashMap<>();

    private static class CachedSegmentStats {
        final SegmentStats stats;
        final long timestamp;

        CachedSegmentStats(SegmentStats stats) {
            this.stats = stats;
            this.timestamp = System.currentTimeMillis();
        }

        boolean isExpired() {
            return System.currentTimeMillis() - timestamp > 10000; // 10 seconds TTL
        }
    }

    /**
     * Retrieve aggregate win/loss stats for a targeting segment.
     * HOT PATH: called on every bid request. Must complete in <5ms.
     * Uses in-memory cache with 10s TTL to minimize Redis calls.
     */
    public Mono<SegmentStats> getSegmentStats(String segmentKey) {
        // Fast path: return from cache if fresh
        CachedSegmentStats cached = segmentStatsCache.get(segmentKey);
        if (cached != null && !cached.isExpired()) {
            return Mono.just(cached.stats);
        }

        // Slow path: load from Redis
        String key = segmentStatsKey(segmentKey);

        return redis.opsForHash().entries(key)
            .collectMap(e -> e.getKey().toString(), e -> e.getValue().toString())
            .map(data -> {
                SegmentStats stats;
                if (data.isEmpty()) {
                    stats = SegmentStats.empty();
                } else {
                    int totalAuctions = parseInt(data.get("totalAuctions"));
                    int totalWins = parseInt(data.get("totalWins"));
                    double sumClearingPrices = parseDouble(data.get("sumClearingPrices"));
                    double sumLossPrices = parseDouble(data.get("sumLossPrices"));
                    stats = SegmentStats.fromHashData(totalAuctions, totalWins, sumClearingPrices, sumLossPrices);
                }

                // Cache the result
                segmentStatsCache.put(segmentKey, new CachedSegmentStats(stats));
                return stats;
            })
            .defaultIfEmpty(SegmentStats.empty())
            .doOnNext(stats -> segmentStatsCache.put(segmentKey, new CachedSegmentStats(stats)));
    }
}
