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

    // KEYS[1] = budget key, ARGV[1] = default budget (used only if the key doesn't exist yet),
    // ARGV[2] = clearing price to subtract. Atomic on the Redis server itself, replacing the old
    // synchronized setIfAbsent()-then-increment() pair, which only ever guarded against
    // concurrent callers within this one JVM, not against Redis itself.
    private static final RedisScript<Double> RECORD_WIN_SCRIPT = RedisScript.of("""
            if redis.call('EXISTS', KEYS[1]) == 0 then
                redis.call('SET', KEYS[1], ARGV[1])
            end
            return redis.call('INCRBYFLOAT', KEYS[1], -tonumber(ARGV[2]))
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

    /** Decrement the winning creative's remaining budget by what it paid. */
    public Mono<Double> recordWin(String creativeId, double clearingPrice) {
        String key = budgetKey(creativeId);
        return redis.execute(RECORD_WIN_SCRIPT,
                        List.of(key),
                        List.of(String.valueOf(properties.getCreativeBudget()), String.valueOf(clearingPrice)))
                .next()
                .doOnNext(after -> log.info("BUDGET  key={} clearing={} remaining={}", key, clearingPrice, after))
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

    /**
     * Retrieve aggregate win/loss stats for a targeting segment.
     * HOT PATH: called on every bid request. Must complete in <5ms.
     */
    public Mono<SegmentStats> getSegmentStats(String segmentKey) {
        String key = segmentStatsKey(segmentKey);

        return redis.opsForHash().entries(key)
            .collectMap(e -> e.getKey().toString(), e -> e.getValue().toString())
            .map(data -> {
                if (data.isEmpty()) {
                    return SegmentStats.empty();
                }

                int totalAuctions = parseInt(data.get("totalAuctions"));
                int totalWins = parseInt(data.get("totalWins"));
                double sumClearingPrices = parseDouble(data.get("sumClearingPrices"));
                double sumLossPrices = parseDouble(data.get("sumLossPrices"));

                return SegmentStats.fromHashData(totalAuctions, totalWins, sumClearingPrices, sumLossPrices);
            })
            .defaultIfEmpty(SegmentStats.empty());
    }
}
