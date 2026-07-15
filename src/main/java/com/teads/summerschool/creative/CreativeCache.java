package com.teads.summerschool.creative;

import com.teads.summerschool.config.BidderProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Lookup for this bidder's creative catalog. Originally snapshotted the catalog once
 * (right after CreativeSeeder seeded it) to save a Postgres round trip per bid() call,
 * but creatives can be added/removed after startup faster than any cache could track —
 * a stale snapshot then either hides a just-added creative or keeps matching one that's
 * already gone. getAll() reads straight from Postgres each time to stay correct;
 * revisit only with a cache invalidated by the write path itself, not time.
 */
@Component
public class CreativeCache {

    private static final Logger log = LoggerFactory.getLogger(CreativeCache.class);

    private final CreativeRepository repository;
    private final BidderProperties properties;

    public CreativeCache(CreativeRepository repository, BidderProperties properties) {
        this.repository = repository;
        this.properties = properties;
    }

    public Flux<Creative> getAll() {
        return repository.findByBidderId(properties.getId());
    }

    /** Kept for CreativeSeeder, which logs the catalog size right after seeding. */
    public Mono<Void> refresh() {
        return getAll().count()
                .doOnNext(n -> log.info("Creative catalog seeded: {} creatives", n))
                .then();
    }
}
