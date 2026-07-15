package com.teads.summerschool.stats.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.LocalDateTime;

public record StatsResponse(
        @JsonProperty("bidder_id") String bidderId,
        @JsonProperty("generated_at") LocalDateTime generatedAt,
        @JsonProperty("total_auctions") long totalAuctions,
        long bids,
        @JsonProperty("no_bids") long noBids,
        @JsonProperty("bid_rate") double bidRate,
        long wins,
        @JsonProperty("win_rate") double winRate,
        @JsonProperty("win_rate_per_auction") double winRatePerAuction,
        @JsonProperty("avg_bid_price") double avgBidPrice,
        @JsonProperty("avg_win_price") double avgWinPrice,
        @JsonProperty("total_spend") double totalSpend,
        @JsonProperty("remaining_budget") double remainingBudget,
        double budget,
        @JsonProperty("latency_ms") LatencyStats latencyMs,
        @JsonProperty("no_bid_reasons") NoBidReasons noBidReasons,
        PacingStats pacing
) {
    public record LatencyStats(double avg, int p50, int p95, int max, long count) {}

    public record NoBidReasons(
            @JsonProperty("budget_exhausted") int budgetExhausted,
            @JsonProperty("no_eligible_creative") int noEligibleCreative,
            @JsonProperty("targeting_miss") int targetingMiss
    ) {}

    public record PacingStats(
            @JsonProperty("spend_per_minute") double spendPerMinute,
            @JsonProperty("elapsed_minutes") double elapsedMinutes,
            @JsonProperty("projected_minutes_to_exhaustion") Double projectedMinutesToExhaustion,
            @JsonProperty("budget_utilization") double budgetUtilization
    ) {}
}
