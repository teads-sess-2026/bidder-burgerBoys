package com.teads.summerschool.creative;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.teads.summerschool.config.BidderProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Redis-backed creative catalog cache for high-throughput bidding.
 *
 * <p>Stores complete Creative objects as JSON in Redis (one key per creative) to enable
 * sub-millisecond lookups during bid requests. Falls back to Postgres if Redis unavailable.
 *
 * <p>Key pattern: {@code {bidderId}_creative_{creativeId}}
 * <p>Example: {@code burgerBoys_creative_burgerBoys-creative-1}
 * <p>Value: JSON-serialized Creative object
 * <p>Set key: {@code {bidderId}_creative_ids} - tracks all creative IDs for O(1) lookup
 */
@Component
public class CreativeCache {

    private static final Logger log = LoggerFactory.getLogger(CreativeCache.class);

    private final CreativeRepository repository;
    private final BidderProperties properties;
    private final ReactiveRedisTemplate<String, String> redis;
    private final ObjectMapper objectMapper;

    // In-memory cache with 5-second TTL to avoid Redis roundtrip on every bid
    private final Map<String, CachedCreative> inMemoryCache = new ConcurrentHashMap<>();
    private static final long CACHE_TTL_MS = 5000;

    private static class CachedCreative {
        final Creative creative;
        final long timestamp;

        CachedCreative(Creative creative) {
            this.creative = creative;
            this.timestamp = System.currentTimeMillis();
        }

        boolean isExpired() {
            return System.currentTimeMillis() - timestamp > CACHE_TTL_MS;
        }
    }

    public CreativeCache(CreativeRepository repository,
                        BidderProperties properties,
                        ReactiveRedisTemplate<String, String> redis,
                        ObjectMapper objectMapper) {
        this.repository = repository;
        this.properties = properties;
        this.redis = redis;
        this.objectMapper = objectMapper;
    }

    /**
     * Load all creatives from in-memory cache first (fast path), then Redis, then Postgres.
     * Uses Redis SET + MGET for O(1) lookup instead of KEYS scan.
     */
    public Flux<Creative> getAll() {
        // Fast path: return from in-memory cache if fresh
        if (!inMemoryCache.isEmpty() && inMemoryCache.values().stream().noneMatch(CachedCreative::isExpired)) {
            return Flux.fromIterable(inMemoryCache.values()).map(c -> c.creative);
        }

        // Medium path: batch load from Redis using SMEMBERS + MGET
        String setKey = creativeIdsKey();

        return redis.opsForSet().members(setKey)
            .collectList()
            .flatMapMany(ids -> {
                if (ids.isEmpty()) {
                    log.warn("No creative IDs found in Redis set, falling back to Postgres");
                    return loadFromPostgresAndCache();
                }

                // Batch GET all creatives in one roundtrip using MGET
                List<String> keys = ids.stream().map(this::creativeKey).toList();
                return redis.opsForValue().multiGet(keys)
                    .flatMapMany(jsonList -> {
                        if (jsonList == null || jsonList.isEmpty()) {
                            log.warn("MGET returned empty, falling back to Postgres");
                            return loadFromPostgresAndCache();
                        }

                        return Flux.fromIterable(jsonList)
                            .index()
                            .flatMap(tuple -> {
                                String json = tuple.getT2();
                                String creativeId = ids.get(tuple.getT1().intValue());
                                if (json == null) {
                                    log.warn("Missing creative data for {}", creativeId);
                                    return Mono.empty();
                                }
                                return deserializeCreative(json, creativeId);
                            })
                            .doOnNext(creative -> inMemoryCache.put(creative.getId(), new CachedCreative(creative)));
                    });
            })
            .switchIfEmpty(Flux.defer(() -> {
                log.warn("Redis returned empty, falling back to Postgres");
                return loadFromPostgresAndCache();
            }))
            .onErrorResume(e -> {
                log.error("Redis error loading creatives, falling back to Postgres: {}", e.getMessage());
                return repository.findByBidderId(properties.getId());
            });
    }

    /**
     * Initialize/refresh Redis cache from Postgres. Called on startup by CreativeSeeder.
     * Loads all creatives from DB and stores them as JSON in Redis.
     * Also populates the creative IDs set for fast lookup.
     */
    public Mono<Void> refresh() {
        String setKey = creativeIdsKey();

        return repository.findByBidderId(properties.getId())
            .collectList()
            .flatMap(creatives -> {
                if (creatives.isEmpty()) {
                    log.warn("No creatives found in Postgres for bidder {}", properties.getId());
                    return Mono.empty();
                }

                // Populate in-memory cache
                creatives.forEach(c -> inMemoryCache.put(c.getId(), new CachedCreative(c)));

                // Store creative IDs in Redis SET for O(1) lookup
                Mono<Long> addToSet = redis.opsForSet().add(setKey,
                    creatives.stream().map(Creative::getId).toArray(String[]::new))
                    .doOnNext(count -> log.info("Added {} creative IDs to Redis set {}", count, setKey));

                // Store each creative as JSON
                Flux<Creative> cacheAll = Flux.fromIterable(creatives)
                    .flatMap(this::cacheCreative);

                return Mono.when(addToSet, cacheAll.then())
                    .doOnSuccess(v -> log.info("Creative catalog cached in Redis: {} creatives", creatives.size()));
            })
            .then();
    }

    /**
     * Store a single creative in Redis as JSON and add its ID to the set.
     */
    private Mono<Creative> cacheCreative(Creative creative) {
        String key = creativeKey(creative.getId());
        String setKey = creativeIdsKey();

        try {
            String json = objectMapper.writeValueAsString(creative);

            // Store JSON and add to set in parallel
            Mono<Boolean> setJson = redis.opsForValue().set(key, json)
                .timeout(Duration.ofMillis(500))
                .onErrorResume(e -> {
                    log.warn("Failed to cache creative {} in Redis: {}", creative.getId(), e.getMessage());
                    return Mono.just(false);
                });

            Mono<Long> addToSet = redis.opsForSet().add(setKey, creative.getId())
                .timeout(Duration.ofMillis(500))
                .onErrorResume(e -> {
                    log.warn("Failed to add creative {} to set: {}", creative.getId(), e.getMessage());
                    return Mono.just(0L);
                });

            return Mono.when(setJson, addToSet).thenReturn(creative);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize creative {}: {}", creative.getId(), e.getMessage());
            return Mono.just(creative);
        }
    }

    /**
     * Fallback: load from Postgres and populate Redis cache on-the-fly.
     */
    private Flux<Creative> loadFromPostgresAndCache() {
        return repository.findByBidderId(properties.getId())
            .flatMap(this::cacheCreative);
    }

    /**
     * Deserialize Creative from JSON. Returns empty Mono on error to skip corrupted entries.
     */
    private Mono<Creative> deserializeCreative(String json, String creativeId) {
        try {
            Creative creative = objectMapper.readValue(json, Creative.class);
            return Mono.just(creative);
        } catch (JsonProcessingException e) {
            log.warn("Failed to deserialize creative {}: {}", creativeId, e.getMessage());
            return Mono.empty();
        }
    }

    /**
     * Remove a creative from Redis cache (e.g., when creative is deleted).
     */
    public Mono<Void> evict(String creativeId) {
        String key = creativeKey(creativeId);
        String setKey = creativeIdsKey();

        // Remove from in-memory cache
        inMemoryCache.remove(creativeId);

        // Remove from Redis: both the JSON and the set entry
        return Mono.when(
            redis.delete(key).doOnNext(deleted -> log.info("Evicted creative {} from Redis cache", creativeId)),
            redis.opsForSet().remove(setKey, creativeId)
        ).then();
    }

    private String creativeKey(String creativeId) {
        return properties.getId() + "_creative_" + creativeId;
    }

    private String creativeIdsKey() {
        return properties.getId() + "_creative_ids";
    }
}
