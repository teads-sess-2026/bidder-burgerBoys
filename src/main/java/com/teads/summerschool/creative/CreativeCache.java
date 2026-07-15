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

/**
 * Redis-backed creative catalog cache for high-throughput bidding.
 *
 * <p>Stores complete Creative objects as JSON in Redis (one key per creative) to enable
 * sub-millisecond lookups during bid requests. Falls back to Postgres if Redis unavailable.
 *
 * <p>Key pattern: {@code {bidderId}_creative_{creativeId}}
 * <p>Example: {@code burgerBoys_creative_burgerBoys-creative-1}
 * <p>Value: JSON-serialized Creative object
 */
@Component
public class CreativeCache {

    private static final Logger log = LoggerFactory.getLogger(CreativeCache.class);

    private final CreativeRepository repository;
    private final BidderProperties properties;
    private final ReactiveRedisTemplate<String, String> redis;
    private final ObjectMapper objectMapper;

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
     * Load all creatives from Redis cache. Fast path for bid requests.
     * Falls back to Postgres if Redis keys missing or errors occur.
     */
    public Flux<Creative> getAll() {
        String pattern = creativeKeyPattern();

        return redis.keys(pattern)
            .flatMap(key -> redis.opsForValue().get(key)
                .flatMap(json -> deserializeCreative(json, key))
            )
            .switchIfEmpty(Flux.defer(() -> {
                log.warn("No creatives found in Redis cache, falling back to Postgres");
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
     */
    public Mono<Void> refresh() {
        return repository.findByBidderId(properties.getId())
            .flatMap(this::cacheCreative)
            .count()
            .doOnNext(n -> log.info("Creative catalog cached in Redis: {} creatives", n))
            .then();
    }

    /**
     * Store a single creative in Redis as JSON.
     */
    private Mono<Creative> cacheCreative(Creative creative) {
        String key = creativeKey(creative.getId());
        try {
            String json = objectMapper.writeValueAsString(creative);
            return redis.opsForValue().set(key, json)
                .timeout(Duration.ofMillis(500))
                .onErrorResume(e -> {
                    log.warn("Failed to cache creative {} in Redis: {}", creative.getId(), e.getMessage());
                    return Mono.just(false);
                })
                .thenReturn(creative);
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
    private Mono<Creative> deserializeCreative(String json, String key) {
        try {
            Creative creative = objectMapper.readValue(json, Creative.class);
            return Mono.just(creative);
        } catch (JsonProcessingException e) {
            log.warn("Failed to deserialize creative from Redis key {}: {}", key, e.getMessage());
            return Mono.empty();
        }
    }

    /**
     * Remove a creative from Redis cache (e.g., when creative is deleted).
     */
    public Mono<Void> evict(String creativeId) {
        String key = creativeKey(creativeId);
        return redis.delete(key)
            .doOnNext(deleted -> log.info("Evicted creative {} from Redis cache", creativeId))
            .then();
    }

    private String creativeKey(String creativeId) {
        return properties.getId() + "_creative_" + creativeId;
    }

    private String creativeKeyPattern() {
        return properties.getId() + "_creative_*";
    }
}
