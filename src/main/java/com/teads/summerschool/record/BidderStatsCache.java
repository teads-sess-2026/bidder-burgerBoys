package com.teads.summerschool.record;

import com.teads.summerschool.config.BidderProperties;
import com.teads.summerschool.creative.CreativeRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-creative budget cache backed by Redis.
 *
 * <p>Key format: {@code {bidderId}_{creativeId}_budget}, value = remaining budget.
 * The SSP is the single owner of these keys and atomically decrements them on each win.
 * This bidder never writes spend to them: it only seeds them once with SETNX/setIfAbsent
 * (so a restart can't refill an already-spent budget) and reads them. The in-memory
 * budget tracking below is the bidder's own local view used for bid decisions.
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

    // In-memory budget store: creativeId -> remaining budget (doubles stored as long bits for atomicity)
    private final ConcurrentHashMap<String, double[]> budgets = new ConcurrentHashMap<>();

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
    private volatile double initialTotalBudget = 0.0;
    // Cached total remaining budget — updated atomically on every reserve/refund
    private volatile double cachedTotalRemaining = 0.0;
    private final Object totalBudgetLock = new Object();


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

    /**
     * Seed a creative's budget key in Redis with its full limit. Called once per creative on startup.
     * Uses setIfAbsent (SETNX) so a bidder restart never refills a budget the SSP has already spent.
     */
    public Mono<Boolean> initBudget(String creativeId, double budget) {
        budgets.put(creativeId, new double[]{budget});
        synchronized (totalBudgetLock) {
            initialTotalBudget += budget;
            cachedTotalRemaining += budget;
        }
        log.info("Creative budget initialized in-memory: {} = {}", creativeId, budget);
        String key = budgetKey(creativeId);
        return redis.opsForValue().setIfAbsent(key, String.valueOf(budget))
                .timeout(java.time.Duration.ofSeconds(2))
                .doOnNext(seeded -> {
                    if (seeded) {
                        log.info("Budget key seeded in Redis: {} = {}", key, budget);
                    } else {
                        log.info("Budget key already exists in Redis, left untouched (SSP-owned): {}", key);
                    }
                })
                .onErrorResume(e -> {
                    log.warn("Failed to seed budget key in Redis for {} — will lazy-seed on next reserve/refund: {}",
                            creativeId, e.getMessage());
                    return Mono.just(true);
                });
    }

    /**
     * Atomically reserve bidPrice from a creative's local budget before placing a bid.
     * Returns the new remaining budget if reservation succeeded, or empty Mono if insufficient funds.
     * Decision is in-memory (instant); the Redis budget key is never written here — the SSP owns it.
     */
    public Mono<Double> reserveBudget(String creativeId, double bidPrice) {
        double[] holder = budgets.computeIfAbsent(creativeId,
                k -> new double[]{properties.getCreativeBudget()});

        double remaining;
        synchronized (holder) {
            if (holder[0] < bidPrice) {
                return Mono.empty();
            }
            holder[0] -= bidPrice;
            remaining = holder[0];
        }
        synchronized (totalBudgetLock) {
            cachedTotalRemaining -= bidPrice;
        }
        log.debug("RESERVE creative={} amount={} remaining={}", creativeId, bidPrice, remaining);
        seedBudgetKeyIfMissing(creativeId);
        return Mono.just(remaining);
    }

    /**
     * Refund a previously reserved amount back to the creative's local budget (on loss or overpayment).
     * In-memory only — the Redis budget key is owned and decremented by the SSP.
     */
    public Mono<Double> refundBudget(String creativeId, double amount) {
        if (amount <= 0) return Mono.just(0.0);
        double[] holder = budgets.computeIfAbsent(creativeId,
                k -> new double[]{properties.getCreativeBudget()});

        double remaining;
        synchronized (holder) {
            holder[0] += amount;
            remaining = holder[0];
        }
        synchronized (totalBudgetLock) {
            cachedTotalRemaining += amount;
        }
        log.debug("REFUND  creative={} amount={} remaining={}", creativeId, amount, remaining);
        seedBudgetKeyIfMissing(creativeId);
        return Mono.just(remaining);
    }

    /**
     * Fire-and-forget lazy seed of the budget key in Redis via setIfAbsent (SETNX), in case
     * startup seeding failed. Never overwrites an existing value — the SSP owns the key.
     */
    private void seedBudgetKeyIfMissing(String creativeId) {
        String key = budgetKey(creativeId);
        double budget = properties.getCreativeBudget();
        redis.opsForValue().setIfAbsent(key, String.valueOf(budget))
                .subscribe(seeded -> {
                    if (Boolean.TRUE.equals(seeded)) {
                        log.info("Budget key lazily seeded in Redis: {} = {}", key, budget);
                    }
                }, err -> log.warn("Failed to lazy-seed budget key in Redis for {}: {}",
                        creativeId, err.getMessage()));
    }

    /**
     * On win: refund the difference between reserved bidPrice and actual clearingPrice
     * in the local in-memory budget, and update local win statistics. The Redis budget
     * key is not touched — the SSP decrements it atomically on each win.
     */
    public Mono<Double> recordWin(String creativeId, double clearingPrice, double bidPrice) {
        double refundAmount = bidPrice - clearingPrice;
        return refundBudget(creativeId, refundAmount)
                .doOnNext(after -> {
                    log.info("BUDGET  creative={} clearing={} refund={} remaining={}",
                            creativeId, clearingPrice, refundAmount, after);
                    winCount.incrementAndGet();
                    synchronized (recentWinPrices) {
                        recentWinPrices.addLast(clearingPrice);
                        if (recentWinPrices.size() > properties.getStrategy().getWindowSize()) {
                            recentWinPrices.pollFirst();
                        }
                    }
                    // Async Postgres sync — fire-and-forget so it doesn't block the hot path
                    // final double remaining = after;
                    // creativeRepository.findById(creativeId)
                    //         .flatMap(c -> {
                    //             c.setBudget(remaining);
                    //             return creativeRepository.save(c);
                    //         })
                    //         .subscribeOn(Schedulers.boundedElastic())
                    //         .subscribe(
                    //                 ok -> {},
                    //                 err -> log.warn("Failed to sync budget to Postgres for {}: {}",
                    //                         creativeId, err.getMessage())
                    //         );
                });
    }

    /**
     * On loss: refund the entire reserved bidPrice back to the creative's local budget.
     * In-memory only — the Redis budget key stays untouched.
     */
    public Mono<Double> recordLoss(String creativeId, double bidPrice) {
        return refundBudget(creativeId, bidPrice)
                .doOnNext(after -> {
                    log.info("LOSS-REFUND creative={} refund={} remaining={}", creativeId, bidPrice, after);
                    // PG write-back intentionally removed (same as recordWin above):
                    // creatives.budget keeps the initial seed value; Redis alone
                    // tracks remaining budget.
                    // final double remaining = after;
                    // creativeRepository.findById(creativeId)
                    //         .flatMap(c -> {
                    //             c.setBudget(remaining);
                    //             return creativeRepository.save(c);
                    //         })
                    //         .subscribeOn(Schedulers.boundedElastic())
                    //         .subscribe(
                    //                 ok -> {},
                    //                 err -> log.warn("Failed to sync budget to Postgres for {}: {}",
                    //                         creativeId, err.getMessage())
                    //         );
                });
    }

    /** Remaining budget for a creative. Lazily initializes to the flat creative budget if missing. */
    public Mono<Double> getRemainingBudget(String creativeId) {
        double[] holder = budgets.computeIfAbsent(creativeId,
                k -> new double[]{properties.getCreativeBudget()});
        synchronized (holder) {
            return Mono.just(holder[0]);
        }
    }

    /**
     * Batch check remaining budgets for multiple creatives.
     * Fully in-memory — O(1) per creative, no Redis round trip.
     */
    public Mono<java.util.Map<String, Double>> getRemainingBudgets(List<String> creativeIds) {
        if (creativeIds.isEmpty()) {
            return Mono.just(java.util.Map.of());
        }

        double defaultBudget = properties.getCreativeBudget();
        java.util.Map<String, Double> result = new java.util.HashMap<>(creativeIds.size());
        for (String creativeId : creativeIds) {
            double[] holder = budgets.computeIfAbsent(creativeId, k -> new double[]{defaultBudget});
            synchronized (holder) {
                result.put(creativeId, holder[0]);
            }
        }
        return Mono.just(result);
    }

    /** Total remaining budget across all creatives. Fully in-memory, no I/O. */
    public double getTotalRemainingBudget() {
        double total = 0.0;
        for (double[] holder : budgets.values()) {
            synchronized (holder) {
                total += holder[0];
            }
        }
        return total;
    }

    /** O(1) cached total remaining budget — updated on every reserve/refund. Use on hot path. */
    public double getCachedTotalRemainingBudget() {
        return cachedTotalRemaining;
    }

    /** Synchronous O(1) read of a single creative's remaining budget. */
    public double getCachedRemainingBudget(String creativeId) {
        double[] holder = budgets.computeIfAbsent(creativeId,
                k -> new double[]{properties.getCreativeBudget()});
        synchronized (holder) {
            return holder[0];
        }
    }

    /** Total budget at startup across all creatives (sum of all initBudget calls). */
    public double getInitialTotalBudget() {
        return initialTotalBudget;
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
     * Also refreshes the in-memory cache so computeBidPrice sees fresh data immediately.
     */
    public Mono<Void> recordWinForSegment(String segmentKey, double clearingPrice, double ourBidPrice) {
        String key = segmentStatsKey(segmentKey);
        return redis.execute(
                RECORD_SEGMENT_WIN_SCRIPT,
                List.of(key),
                List.of(String.valueOf(clearingPrice), String.valueOf(ourBidPrice))
        )
        .then()
        .doOnSuccess(v -> {
            log.debug("SEG-WIN  key={} clearing={}", key, clearingPrice);
            updateCachedSegmentStats(segmentKey, true, clearingPrice, ourBidPrice);
        });
    }

    /**
     * Record a loss for a targeting segment, updating aggregate stats.
     * Also refreshes the in-memory cache so computeBidPrice sees fresh data immediately.
     */
    public Mono<Void> recordLossForSegment(String segmentKey, double ourBidPrice) {
        String key = segmentStatsKey(segmentKey);
        return redis.execute(
                RECORD_SEGMENT_LOSS_SCRIPT,
                List.of(key),
                List.of(String.valueOf(ourBidPrice))
        )
        .then()
        .doOnSuccess(v -> {
            log.debug("SEG-LOSS key={} bid={}", key, ourBidPrice);
            updateCachedSegmentStats(segmentKey, false, 0.0, ourBidPrice);
        });
    }

    private void updateCachedSegmentStats(String segmentKey, boolean won, double clearingPrice, double bidPrice) {
        SegmentWindow window = segmentWindows.computeIfAbsent(segmentKey,
                k -> new SegmentWindow(properties.getStrategy().getWindowSize()));
        SegmentStats updated;
        synchronized (window) {
            window.add(won, clearingPrice, bidPrice);
            updated = window.toStats();
        }
        segmentStatsCache.put(segmentKey, new CachedSegmentStats(updated));
    }

    // Rolling window of recent auction results per segment for windowed win-rate calculation
    private final ConcurrentHashMap<String, SegmentWindow> segmentWindows = new ConcurrentHashMap<>();

    private static class SegmentWindow {
        private final boolean[] wins;
        private final double[] clearingPrices;
        private final double[] bidPrices;
        private final int capacity;
        private int head = 0;
        private int count = 0;

        SegmentWindow(int capacity) {
            this.capacity = capacity;
            this.wins = new boolean[capacity];
            this.clearingPrices = new double[capacity];
            this.bidPrices = new double[capacity];
        }

        void add(boolean won, double clearingPrice, double bidPrice) {
            wins[head] = won;
            clearingPrices[head] = clearingPrice;
            bidPrices[head] = bidPrice;
            head = (head + 1) % capacity;
            if (count < capacity) count++;
        }

        SegmentStats toStats() {
            if (count == 0) return SegmentStats.empty();
            int totalWins = 0;
            double sumClearing = 0.0;
            double sumLossBids = 0.0;
            for (int i = 0; i < count; i++) {
                if (wins[i]) {
                    totalWins++;
                    sumClearing += clearingPrices[i];
                } else {
                    sumLossBids += bidPrices[i];
                }
            }
            double winRate = (double) totalWins / count;
            double avgClearing = totalWins > 0 ? sumClearing / totalWins : 0.0;
            int losses = count - totalWins;
            double avgLoss = losses > 0 ? sumLossBids / losses : 0.0;
            return new SegmentStats(count, totalWins, avgClearing, winRate, avgLoss);
        }
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
     * Synchronous, non-blocking read of cached segment stats.
     * Returns the last known stats from in-memory cache, or empty stats if no data yet.
     * The cache is populated asynchronously by the Kafka consumer via recordWinForSegment/recordLossForSegment,
     * and refreshed by getSegmentStats() calls. This avoids any Redis round-trip on the hot bid path.
     */
    public SegmentStats getSegmentStatsCached(String segmentKey) {
        CachedSegmentStats cached = segmentStatsCache.get(segmentKey);
        if (cached != null) {
            return cached.stats;
        }
        // Trigger async refresh for next time, return empty for now
        getSegmentStats(segmentKey).subscribe();
        return SegmentStats.empty();
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
