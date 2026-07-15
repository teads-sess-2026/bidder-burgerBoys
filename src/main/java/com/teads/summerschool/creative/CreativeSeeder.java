package com.teads.summerschool.creative;

import com.teads.summerschool.config.BidderProperties;
import com.teads.summerschool.record.BidderStatsCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.List;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 1)
public class CreativeSeeder implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(CreativeSeeder.class);

    private final CreativeRepository repository;
    private final BidderProperties properties;
    private final BidderStatsCache statsCache;
    private final CreativeCache creativeCache;

    public CreativeSeeder(CreativeRepository repository, BidderProperties properties,
                           BidderStatsCache statsCache, CreativeCache creativeCache) {
        this.repository = repository;
        this.properties = properties;
        this.statsCache = statsCache;
        this.creativeCache = creativeCache;
    }

    @Override
    public void run(ApplicationArguments args) {
        String id = properties.getId();

        // TODO: customize your creatives — adjust targeting (geos, devices, segments) and
        // maxBidPrice (the highest floor this creative will bid on; null = unbounded) to
        // match your strategy
        Creative usSports = creative(id + "-creative-4", "US Sports", "US sports audience", "US", "", "sports,health");
        usSports.setMaxBidPrice(0.30);
        List<Creative> seedCreatives = List.of(
                creative(id + "-creative-1", "Universal",    "No restrictions — serves everywhere",   "", "", ""),
                creative(id + "-creative-2", "Mobile Tech",  "Mobile-optimized for tech audience",     "", "mobile", "tech,gaming"),
                creative(id + "-creative-3", "EU Premium",   "European markets, all devices",          "DE,FR,GB,ES,IT,NL", "", ""),
                usSports
        );

        repository.findByBidderId(id).hasElements()
                .flatMap(hasAny -> hasAny ? Mono.empty() : repository.saveAll(seedCreatives).then())
                .thenMany(repository.findByBidderId(id))
                // Reset each creative's remaining budget in Redis to its configured limit on startup.
                // A transient Redis timeout on one creative shouldn't crash the whole app: getRemainingBudget
                // already falls back to the flat creative budget for a missing key, and recordWin's
                // setIfAbsent lazily initializes it on first win, so skipping a failed creative here is
                // safe, not silently wrong.
                .flatMap(c -> statsCache.initBudget(c.getId(), c.getBudget())
                        .onErrorResume(e -> {
                            log.warn("Failed to init budget for creative {} — will lazy-init on first read/win: {}",
                                    c.getId(), e.getMessage());
                            return Mono.empty();
                        }))
                .thenMany(Mono.defer(creativeCache::refresh))
                .blockLast();
    }

    private Creative creative(String creativeId, String name, String description,
                               String geos, String devices, String segments) {
        Creative c = new Creative();
        c.setId(creativeId);
        c.setName(name);
        c.setDescription(description);
        c.setImageUrl("https://placeholder.com/" + creativeId + ".jpg");
        c.setCallToAction("Learn More");
        c.setBidderId(properties.getId());
        c.setAllowedGeos(geos);
        c.setAllowedDevices(devices);
        c.setAudienceSegments(segments);
        c.setBudget(properties.getCreativeBudget());
        return c;
    }
}
