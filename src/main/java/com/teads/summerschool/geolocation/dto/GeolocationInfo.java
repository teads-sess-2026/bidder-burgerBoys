package com.teads.summerschool.geolocation.dto;

public record GeolocationInfo(
        String ip,
        String countryCode,
        String countryName,
        String city,
        String region,
        boolean cached
) {
    public static GeolocationInfo fromResponse(GeolocationResponse response, boolean cached) {
        return new GeolocationInfo(
                response.ip(),
                response.getCountryCode(),
                response.getCountryName(),
                response.getCity(),
                response.getStateProv(),
                cached
        );
    }

    public static GeolocationInfo unknown(String ip) {
        return new GeolocationInfo(ip, "UNKNOWN", "Unknown", null, null, false);
    }
}
