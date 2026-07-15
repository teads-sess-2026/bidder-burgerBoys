package com.teads.summerschool.geolocation;

import com.teads.summerschool.geolocation.dto.GeolocationInfo;
import com.teads.summerschool.geolocation.dto.GeolocationResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Service
@ConditionalOnProperty(prefix = "geolocation", name = "enabled", havingValue = "true", matchIfMissing = true)
public class GeolocationService {

    private static final Logger log = LoggerFactory.getLogger(GeolocationService.class);

    private final RestClient restClient;
    private final GeolocationProperties properties;
    private final GeolocationCache cache;

    public GeolocationService(RestClient geolocationRestClient,
                              GeolocationProperties properties,
                              GeolocationCache cache) {
        this.restClient = geolocationRestClient;
        this.properties = properties;
        this.cache = cache;
    }

    public Mono<GeolocationInfo> lookup(String ipAddress) {
        if (!properties.isEnabled()) {
            return Mono.just(GeolocationInfo.unknown(ipAddress));
        }

        if (ipAddress == null || ipAddress.isBlank()) {
            log.warn("Geolocation lookup called with null/empty IP");
            return Mono.just(GeolocationInfo.unknown(ipAddress));
        }

        if (properties.getCache().isEnabled()) {
            return cache.get(ipAddress)
                    .switchIfEmpty(Mono.defer(() -> fetchAndCache(ipAddress)))
                    .onErrorResume(ex -> {
                        log.warn("Geolocation lookup failed for IP {}: {} - {}",
                                ipAddress, ex.getClass().getSimpleName(), ex.getMessage());
                        return Mono.just(GeolocationInfo.unknown(ipAddress));
                    });
        }

        return fetchFromApi(ipAddress)
                .onErrorResume(ex -> {
                    log.warn("Geolocation API call failed for IP {}: {} - {}",
                            ipAddress, ex.getClass().getSimpleName(), ex.getMessage());
                    return Mono.just(GeolocationInfo.unknown(ipAddress));
                });
    }

    private Mono<GeolocationInfo> fetchAndCache(String ipAddress) {
        return fetchFromApi(ipAddress)
                .flatMap(info -> cache.put(ipAddress, info).thenReturn(info));
    }

    private Mono<GeolocationInfo> fetchFromApi(String ipAddress) {
        return Mono.fromCallable(() -> {
            try {
                GeolocationResponse response = restClient.get()
                        .uri(uriBuilder -> uriBuilder
                                .queryParam("apiKey", properties.getApiKey())
                                .queryParam("ip", ipAddress)
                                .build())
                        .retrieve()
                        .body(GeolocationResponse.class);

                log.debug("Geolocation lookup successful: {} -> {}",
                        ipAddress, response != null ? response.getCountryCode() : "null");

                return GeolocationInfo.fromResponse(response, false);
            } catch (Exception e) {
                throw new GeolocationException("API call failed: " + e.getMessage(), e);
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }

    public static class GeolocationException extends RuntimeException {
        public GeolocationException(String message) {
            super(message);
        }

        public GeolocationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
