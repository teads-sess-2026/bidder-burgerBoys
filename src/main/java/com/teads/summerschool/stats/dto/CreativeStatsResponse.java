package com.teads.summerschool.stats.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public record CreativeStatsResponse(
        @JsonProperty("bidder_id") String bidderId,
        List<CreativeStat> creatives
) {
    public record CreativeStat(
            @JsonProperty("creative_id") String creativeId,
            @JsonProperty("creative_name") String creativeName,
            long bids,
            long wins,
            @JsonProperty("win_rate") double winRate,
            @JsonProperty("avg_bid_price") double avgBidPrice,
            @JsonProperty("avg_win_price") double avgWinPrice,
            double spend
    ) {}
}
