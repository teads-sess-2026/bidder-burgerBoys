package com.teads.summerschool.bidding;

import com.teads.summerschool.bidding.dto.BidRequest;
import com.teads.summerschool.config.BidderProperties;
import com.teads.summerschool.geolocation.GeolocationService;
import com.teads.summerschool.geolocation.dto.GeolocationInfo;
import com.teads.summerschool.metrics.BidderMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

@RestController
public class BidController {

    private static final Logger log = LoggerFactory.getLogger(BidController.class);

    private final BiddingService biddingService;
    private final BidderProperties properties;
    private final Optional<GeolocationService> geolocationService;
    private final BidderMetrics metrics;

    public BidController(BiddingService biddingService,
                        BidderProperties properties,
                        @Autowired(required = false) GeolocationService geolocationService,
                        BidderMetrics metrics) {
        this.biddingService = biddingService;
        this.properties = properties;
        this.geolocationService = Optional.ofNullable(geolocationService);
        this.metrics = metrics;
    }

    @PostMapping("/api/bid")
    public Mono<ResponseEntity<?>> bid(@RequestBody BidRequest request) {

        return biddingService.bid(request)
                .map(opt -> {
                    if (opt.isPresent()) {
                        return (ResponseEntity<?>) ResponseEntity.ok(opt.get());
                    }
                    return (ResponseEntity<?>) ResponseEntity.noContent().build();
                })
                .timeout(Duration.ofMillis(properties.getTimeoutMs()))
                .onErrorResume(ex -> {
                    Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
                    log.warn("<< BID TIMEOUT  id={} — {}: {}", request.requestId(),
                            cause.getClass().getSimpleName(), cause.getMessage(), cause);
                    metrics.recordNoBid("timeout");
                    return Mono.just(ResponseEntity.noContent().build());
                });
    }

    @GetMapping("/api/budget")
    public Mono<ResponseEntity<Map<String, Object>>> budget() {
        return Mono.zip(biddingService.getRemainingBudget(), biddingService.getRemainingBudgets())
                .map(tuple -> ResponseEntity.ok(Map.of(
                        "remaining", tuple.getT1(),
                        "creatives", tuple.getT2()
                )));
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of("status", "UP"));
    }

    @GetMapping("/api/geolocation/{ip}")
    public Mono<ResponseEntity<GeolocationInfo>> lookupIp(@PathVariable String ip) {
        return geolocationService
                .map(service -> service.lookup(ip)
                        .map(ResponseEntity::ok)
                        .defaultIfEmpty(ResponseEntity.notFound().build()))
                .orElse(Mono.just(ResponseEntity.status(501)
                        .body(GeolocationInfo.unknown(ip))));
    }
}
