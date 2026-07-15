package com.teads.summerschool.record;

import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.Collection;

public interface BidRecordRepository extends R2dbcRepository<BidRecord, Long> {

    Mono<BidRecord> findByRequestId(String requestId);

    Mono<Long> countByBidPriceIsNotNull();

    Flux<BidRecord> findByBidPriceIsNotNull();

    Flux<BidRecord> findByCreatedAtAfter(LocalDateTime since);

    Flux<BidRecord> findAllByRequestIdIn(Collection<String> requestIds);

    @Query("SELECT no_bid_reason, COUNT(*) AS cnt FROM bid_record WHERE no_bid_reason IS NOT NULL GROUP BY no_bid_reason")
    Flux<NoBidReasonCount> countGroupByNoBidReason();

    @Query("SELECT COALESCE(AVG(bid_price), 0) FROM bid_record WHERE bid_price IS NOT NULL")
    Mono<Double> avgBidPrice();

    @Query("SELECT latency_ms FROM bid_record WHERE latency_ms IS NOT NULL ORDER BY latency_ms")
    Flux<Integer> findAllLatenciesSorted();

    @Query("SELECT MIN(created_at) FROM bid_record")
    Mono<LocalDateTime> findFirstCreatedAt();

    record NoBidReasonCount(String noBidReason, long cnt) {}
}
