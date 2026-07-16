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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 1)
public class CreativeSeeder implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(CreativeSeeder.class);

    private static final String[] GEOS     = {"US", "DE", "FR", "GB", "ES", "IT", "NL", "SE", "PL", "BR"};
    private static final String[] DEVICES  = {"mobile", "desktop", "tablet"};
    private static final String[] SEGMENTS = {"sports", "tech", "fashion", "gaming", "travel", "food", "finance", "health"};

    private static final int    CREATIVE_COUNT    = 200;
    private static final long   SEED              = 42;
    private static final double MAX_BID_PRICE_MIN = 0.05;
    private static final double MAX_BID_PRICE_MAX = 0.30;

    private final CreativeRepository repository;
    private final BidderProperties   properties;
    private final BidderStatsCache   statsCache;
    private final CreativeCache      creativeCache;

    public CreativeSeeder(CreativeRepository repository, BidderProperties properties,
                          BidderStatsCache statsCache, CreativeCache creativeCache) {
        this.repository    = repository;
        this.properties    = properties;
        this.statsCache    = statsCache;
        this.creativeCache = creativeCache;
    }

    @Override
    public void run(ApplicationArguments args) {
        String id = properties.getId();
        Random rnd = new Random(SEED);
        List<Creative> creatives = new ArrayList<>(CREATIVE_COUNT);
        for (int i = 1; i <= CREATIVE_COUNT; i++) {
            Creative c = creative(id + "-creative-" + i, "Creative " + i,
                    "Auto-generated creative #" + i,
                    pickSubset(GEOS, rnd), pickSubset(DEVICES, rnd), pickSubset(SEGMENTS, rnd));
            c.setMaxBidPrice(MAX_BID_PRICE_MIN + rnd.nextDouble() * (MAX_BID_PRICE_MAX - MAX_BID_PRICE_MIN));
            creatives.add(c);
        }

        repository.deleteByBidderId(id)
                .then(repository.saveAll(creatives).then())
                .thenMany(repository.findByBidderId(id))
                .flatMap(c -> statsCache.initBudget(c.getId(), c.getBudget())
                        .onErrorResume(e -> {
                            log.warn("Failed to init budget for creative {} — will lazy-init on first read/win: {}",
                                    c.getId(), e.getMessage());
                            return Mono.empty();
                        }))
                .thenMany(Mono.defer(creativeCache::refresh))
                .blockLast();
    }

    private String pickSubset(String[] options, Random rnd) {
        if (rnd.nextDouble() < 0.3) return "";
        int n = 1 + rnd.nextInt(options.length);
        List<String> pool = new ArrayList<>(List.of(options));
        Collections.shuffle(pool, rnd);
        return String.join(",", pool.subList(0, n));
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
