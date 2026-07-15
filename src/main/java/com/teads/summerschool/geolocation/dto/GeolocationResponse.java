package com.teads.summerschool.geolocation.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record GeolocationResponse(
        @JsonProperty("ip") String ip,
        @JsonProperty("location") Location location,
        @JsonProperty("currency") Currency currency,
        @JsonProperty("time_zone") TimeZone timeZone,
        @JsonProperty("asn") Asn asn
) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Location(
            @JsonProperty("country_code2") String countryCode,
            @JsonProperty("country_name") String countryName,
            @JsonProperty("state_prov") String stateProv,
            @JsonProperty("city") String city,
            @JsonProperty("latitude") String latitude,
            @JsonProperty("longitude") String longitude
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Currency(
            @JsonProperty("code") String code,
            @JsonProperty("name") String name
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record TimeZone(
            @JsonProperty("name") String name,
            @JsonProperty("offset") Integer offset
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Asn(
            @JsonProperty("as_number") String asNumber,
            @JsonProperty("organization") String organization
    ) {}

    public String getCountryCode() {
        return location != null ? location.countryCode() : null;
    }

    public String getCountryName() {
        return location != null ? location.countryName() : null;
    }

    public String getStateProv() {
        return location != null ? location.stateProv() : null;
    }

    public String getCity() {
        return location != null ? location.city() : null;
    }
}
