package com.teads.summerschool.bidding.dto;

public record BidRequest(
        String requestId,
        double floorPrice,
        Targeting targeting,
        String ipAddress
) {}
