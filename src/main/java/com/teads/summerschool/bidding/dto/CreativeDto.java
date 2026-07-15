package com.teads.summerschool.bidding.dto;

import java.util.List;

public record CreativeDto(
        String id,
        String name,
        String description,
        String imageUrl,
        String callToAction,
        List<String> allowedGeos,
        List<String> allowedDevices,
        List<String> audienceSegments
) {}
