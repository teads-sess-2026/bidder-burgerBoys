package com.teads.summerschool.stats.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public record TargetingResponse(
        @JsonProperty("bidder_id") String bidderId,
        @JsonProperty("by_geo") List<TargetingBucket> byGeo,
        @JsonProperty("by_device") List<TargetingBucket> byDevice,
        @JsonProperty("by_segment") List<TargetingBucket> bySegment
) {
    public record TargetingBucket(
            String key,
            long bids,
            long wins,
            @JsonProperty("win_rate") double winRate,
            @JsonProperty("avg_bid_price") double avgBidPrice
    ) {}
}
