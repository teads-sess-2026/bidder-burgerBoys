package com.teads.summerschool.metrics;

import com.teads.summerschool.config.BidderProperties;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.util.function.Supplier;
import java.util.concurrent.TimeUnit;

/**
 * Prometheus/Micrometer metrics for the bidder, exposed at /actuator/prometheus.
 * Every meter is tagged with the bidder id so multiple bidders can be told apart.
 */
@Component
public class BidderMetrics {

    private final MeterRegistry registry;
    private final Counter requests;
    private final Counter bids;
    private final Counter wins;
    private final Counter losses;
    private final Counter spend;
    private final Timer bidLatency;

    private final Counter noBidFloor;
    private final Counter noBidTargeting;
    private final Counter noBidBudget;
    private final Counter noBidExpensive;
    private final Counter noBidTimeout;

    private final String prefix;

    public BidderMetrics(MeterRegistry registry, BidderProperties properties) {
        this.registry = registry;
        // Every metric name is prefixed with the group (bidder id), e.g.
        // team-alpha -> "team_alpha.bids" -> Prometheus "team_alpha_bids_total".
        this.prefix = properties.getId().toLowerCase().replaceAll("[^a-z0-9]+", "_") + ".";
        registry.config().commonTags("bidder", properties.getId());
        // Note: no ".total" suffix — Micrometer appends "_total" to counters for Prometheus.
        this.requests = Counter.builder(prefix + "requests")
                .description("Bid requests received").register(registry);
        this.bids = Counter.builder(prefix + "bids")
                .description("Bids submitted").register(registry);
        this.wins = Counter.builder(prefix + "wins")
                .description("Auctions won").register(registry);
        this.losses = Counter.builder(prefix + "losses")
                .description("Auctions lost after bidding").register(registry);
        this.spend = Counter.builder(prefix + "spend")
                .description("Total clearing price paid").register(registry);
        this.bidLatency = Timer.builder(prefix + "bid.latency")
                .description("Bid handling latency").register(registry);
        this.noBidFloor = Counter.builder(prefix + "nobid.floor_exceeds_max_bid")
                .description("No-bids: floor price exceeds creative max bid").register(registry);
        this.noBidTargeting = Counter.builder(prefix + "nobid.targeting_miss")
                .description("No-bids: no creative matches targeting").register(registry);
        this.noBidBudget = Counter.builder(prefix + "nobid.budget_exhausted")
                .description("No-bids: all matching creatives out of budget").register(registry);
        this.noBidExpensive = Counter.builder(prefix + "nobid.too_expensive")
                .description("No-bids: floor price too high relative to remaining budget").register(registry);
        this.noBidTimeout = Counter.builder(prefix + "nobid.timeout")
                .description("No-bids: bid processing timed out").register(registry);
    }

    public void recordRequest() { requests.increment(); }

    public void recordBid() { bids.increment(); }

    public void recordNoBid(String reason) {
        registry.counter(prefix + "nobids", "reason", reason == null ? "unknown" : reason).increment();
        switch (reason == null ? "" : reason) {
            case "floor_exceeds_max_bid" -> noBidFloor.increment();
            case "targeting_miss" -> noBidTargeting.increment();
            case "budget_exhausted" -> noBidBudget.increment();
            case "too_expensive" -> noBidExpensive.increment();
            case "timeout" -> noBidTimeout.increment();
        }
    }

    public void recordLatency(long ms) { bidLatency.record(ms, TimeUnit.MILLISECONDS); }

    public void recordWin(double clearingPrice) {
        wins.increment();
        spend.increment(clearingPrice);
    }

    public void recordLoss() { losses.increment(); }

    /** Register a live gauge (e.g. remaining budget); the group prefix is applied. */
    public void registerGauge(String name, Supplier<Number> supplier) {
        Gauge.builder(prefix + name, supplier).register(registry);
    }
}
