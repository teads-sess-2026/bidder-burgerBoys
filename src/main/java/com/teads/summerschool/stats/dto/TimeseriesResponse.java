package com.teads.summerschool.stats.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.LocalDateTime;
import java.util.List;

public record TimeseriesResponse(
        @JsonProperty("bidder_id") String bidderId,
        @JsonProperty("window_minutes") int windowMinutes,
        @JsonProperty("bucket_seconds") int bucketSeconds,
        List<Point> points
) {
    public record Point(
            LocalDateTime time,
            long auctions,
            long bids,
            long wins,
            @JsonProperty("bid_rate") double bidRate,
            @JsonProperty("win_rate") double winRate,
            @JsonProperty("avg_bid_price") double avgBidPrice,
            double spend
    ) {}
}
