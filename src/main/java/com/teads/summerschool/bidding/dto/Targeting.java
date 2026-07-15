package com.teads.summerschool.bidding.dto;

public record Targeting(
        String geo,
        String deviceType,
        String audienceSegment
) {}
