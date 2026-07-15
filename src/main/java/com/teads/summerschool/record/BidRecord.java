package com.teads.summerschool.record;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;
import java.time.LocalDateTime;

@Table("bid_record")
public class BidRecord {

    @Id
    private Long id;

    private String requestId;

    private double floorPrice;
    private String geo;
    private String deviceType;
    private String audienceSegment;

    // Null if we did not bid
    private Double bidPrice;
    private String creativeId;

    // Our own processing time for this request, in milliseconds
    private Integer latencyMs;

    // Why we did not bid: budget_exhausted | no_eligible_creative | targeting_miss | floor_exceeds_max_bid
    // Null when we submitted a bid.
    private String noBidReason;

    private LocalDateTime createdAt = LocalDateTime.now();

    public BidRecord() {}

    public Long getId() { return id; }

    public String getRequestId() { return requestId; }
    public void setRequestId(String requestId) { this.requestId = requestId; }

    public double getFloorPrice() { return floorPrice; }
    public void setFloorPrice(double floorPrice) { this.floorPrice = floorPrice; }

    public String getGeo() { return geo; }
    public void setGeo(String geo) { this.geo = geo; }

    public String getDeviceType() { return deviceType; }
    public void setDeviceType(String deviceType) { this.deviceType = deviceType; }

    public String getAudienceSegment() { return audienceSegment; }
    public void setAudienceSegment(String audienceSegment) { this.audienceSegment = audienceSegment; }

    public Double getBidPrice() { return bidPrice; }
    public void setBidPrice(Double bidPrice) { this.bidPrice = bidPrice; }

    public String getCreativeId() { return creativeId; }
    public void setCreativeId(String creativeId) { this.creativeId = creativeId; }

    public Integer getLatencyMs() { return latencyMs; }
    public void setLatencyMs(Integer latencyMs) { this.latencyMs = latencyMs; }

    public String getNoBidReason() { return noBidReason; }
    public void setNoBidReason(String noBidReason) { this.noBidReason = noBidReason; }

    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
}
