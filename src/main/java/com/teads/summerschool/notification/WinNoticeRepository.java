package com.teads.summerschool.notification;

import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

public interface WinNoticeRepository extends R2dbcRepository<WinNotice, Long> {

    @Query("SELECT COALESCE(SUM(clearing_price), 0) FROM win_notice")
    Mono<Double> sumClearingPrice();

    @Query("SELECT AVG(clearing_price) FROM win_notice")
    Mono<Double> avgClearingPrice();

    Flux<WinNotice> findByReceivedAtAfter(LocalDateTime since);
}
