package com.teads.summerschool.geolocation;

import com.teads.summerschool.geolocation.dto.GeolocationInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Component
@ConditionalOnProperty(prefix = "geolocation.cache", name = "enabled", havingValue = "true", matchIfMissing = true)
public class GeolocationCache {

    private static final Logger log = LoggerFactory.getLogger(GeolocationCache.class);

    private final ReactiveRedisTemplate<String, String> redis;
    private final GeolocationProperties properties;

    public GeolocationCache(ReactiveRedisTemplate<String, String> redis,
                            GeolocationProperties properties) {
        this.redis = redis;
        this.properties = properties;
    }

    public Mono<GeolocationInfo> get(String ipAddress) {
        String key = cacheKey(ipAddress);
        return redis.opsForValue().get(key)
                .flatMap(value -> {
                    try {
                        GeolocationInfo info = parseFromString(value);
                        log.debug("Cache HIT for IP: {}", ipAddress);
                        return Mono.just(new GeolocationInfo(
                                info.ip(), info.countryCode(), info.countryName(),
                                info.city(), info.region(), true
                        ));
                    } catch (Exception e) {
                        log.warn("Failed to deserialize cached geolocation for {}: {}", ipAddress, e.getMessage());
                        return Mono.empty();
                    }
                })
                .switchIfEmpty(Mono.defer(() -> {
                    log.debug("Cache MISS for IP: {}", ipAddress);
                    return Mono.empty();
                }));
    }

    public Mono<Void> put(String ipAddress, GeolocationInfo info) {
        String key = cacheKey(ipAddress);
        try {
            String value = toFormattedString(info);
            Duration ttl = Duration.ofSeconds(properties.getCache().getTtlSeconds());
            return redis.opsForValue().set(key, value, ttl)
                    .doOnSuccess(success -> log.debug("Cached geolocation: {} -> {}", ipAddress, info.countryCode()))
                    .then();
        } catch (Exception e) {
            log.warn("Failed to serialize geolocation for caching: {}", e.getMessage());
            return Mono.empty();
        }
    }

    private String cacheKey(String ipAddress) {
        return properties.getCache().getKeyPrefix() + ipAddress;
    }

    private String toFormattedString(GeolocationInfo info) {
        return String.format("%s|%s|%s|%s|%s",
                info.ip(),
                info.countryCode(),
                nullSafe(info.countryName()),
                nullSafe(info.city()),
                nullSafe(info.region()));
    }

    private GeolocationInfo parseFromString(String value) {
        String[] parts = value.split("\\|", -1);
        if (parts.length != 5) {
            throw new IllegalArgumentException("Invalid cached format");
        }
        return new GeolocationInfo(
                parts[0],
                parts[1],
                emptyToNull(parts[2]),
                emptyToNull(parts[3]),
                emptyToNull(parts[4]),
                false
        );
    }

    private String nullSafe(String value) {
        return value == null ? "" : value;
    }

    private String emptyToNull(String value) {
        return value.isEmpty() ? null : value;
    }
}
