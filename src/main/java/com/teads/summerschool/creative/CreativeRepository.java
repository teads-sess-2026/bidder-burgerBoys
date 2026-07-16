package com.teads.summerschool.creative;

import org.springframework.data.r2dbc.repository.R2dbcRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface CreativeRepository extends R2dbcRepository<Creative, String> {

    Flux<Creative> findByBidderId(String bidderId);
    Mono<Void> deleteByBidderId(String bidderId);
}
