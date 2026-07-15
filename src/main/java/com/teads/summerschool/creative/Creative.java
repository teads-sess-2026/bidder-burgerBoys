package com.teads.summerschool.creative;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceCreator;
import org.springframework.data.annotation.Transient;
import org.springframework.data.domain.Persistable;
import org.springframework.data.relational.core.mapping.Table;

@Table("creatives")
public class Creative implements Persistable<String> {

    @Id
    private String id;
    private String name;
    private String description;
    private String imageUrl;
    private String callToAction;
    private String bidderId;

    // Per-creative budget limit (competition currency). Initial value seeded from
    // bidder.creative-budget; remaining budget is tracked live in Redis.
    private double budget;

    // Highest price this creative is willing to pay. Requests whose floor price
    // exceeds this are filtered out before bidding, regardless of targeting match.
    // Null means unbounded (no cap) — matches allowedGeos/etc. defaulting to "no restriction".
    private Double maxBidPrice;

    // Comma-separated values; empty string means no restriction
    private String allowedGeos = "";
    private String allowedDevices = "";
    private String audienceSegments = "";

    // id is client-assigned (not DB-generated), so R2DBC can't infer new-vs-existing from a
    // null-id check the way it does for auto-generated PKs. isNew defaults true for rows built
    // by application code (e.g. CreativeSeeder) and false for rows materialized from the DB via
    // the @PersistenceCreator constructor below — see Persistable<String>.
    @Transient
    private boolean isNew = true;

    public Creative() {}

    @PersistenceCreator
    Creative(String id, String name, String description, String imageUrl, String callToAction,
              String bidderId, double budget, Double maxBidPrice,
              String allowedGeos, String allowedDevices, String audienceSegments) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.imageUrl = imageUrl;
        this.callToAction = callToAction;
        this.bidderId = bidderId;
        this.budget = budget;
        this.maxBidPrice = maxBidPrice;
        this.allowedGeos = allowedGeos;
        this.allowedDevices = allowedDevices;
        this.audienceSegments = audienceSegments;
        this.isNew = false;
    }

    @Override
    public boolean isNew() { return isNew; }

    @Override
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }

    public String getImageUrl() { return imageUrl; }
    public void setImageUrl(String imageUrl) { this.imageUrl = imageUrl; }

    public String getCallToAction() { return callToAction; }
    public void setCallToAction(String callToAction) { this.callToAction = callToAction; }

    public String getBidderId() { return bidderId; }
    public void setBidderId(String bidderId) { this.bidderId = bidderId; }

    public double getBudget() { return budget; }
    public void setBudget(double budget) { this.budget = budget; }

    public Double getMaxBidPrice() { return maxBidPrice; }
    public void setMaxBidPrice(Double maxBidPrice) { this.maxBidPrice = maxBidPrice; }

    public String getAllowedGeos() { return allowedGeos; }
    public void setAllowedGeos(String allowedGeos) { this.allowedGeos = allowedGeos; }

    public String getAllowedDevices() { return allowedDevices; }
    public void setAllowedDevices(String allowedDevices) { this.allowedDevices = allowedDevices; }

    public String getAudienceSegments() { return audienceSegments; }
    public void setAudienceSegments(String audienceSegments) { this.audienceSegments = audienceSegments; }

    public boolean isWithinMaxBid(double floorPrice) {
        return maxBidPrice == null || maxBidPrice >= floorPrice;
    }

    public boolean matches(String geo, String deviceType, String audienceSegment) {
        return matchesField(allowedGeos, geo)
                && matchesField(allowedDevices, deviceType)
                && matchesField(audienceSegments, audienceSegment);
    }

    private boolean matchesField(String allowed, String value) {
        if (allowed == null || allowed.isBlank()) return true;
        for (String entry : allowed.split(",")) {
            if (entry.trim().equalsIgnoreCase(value)) return true;
        }
        return false;
    }
}
