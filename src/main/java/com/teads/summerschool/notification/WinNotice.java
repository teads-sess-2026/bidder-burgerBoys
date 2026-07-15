package com.teads.summerschool.notification;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceCreator;
import org.springframework.data.relational.core.mapping.Table;
import java.time.LocalDateTime;

@Table("win_notice")
public class WinNotice {

    @Id
    private Long id;

    private String requestId;
    private String bidderId;
    private double clearingPrice;
    private double bidPrice;
    private LocalDateTime receivedAt = LocalDateTime.now();

    public WinNotice() {}

    public WinNotice(String requestId, String bidderId, double clearingPrice, double bidPrice) {
        this.requestId = requestId;
        this.bidderId = bidderId;
        this.clearingPrice = clearingPrice;
        this.bidPrice = bidPrice;
    }

    // Used only by Spring Data R2DBC to materialize rows read back from the database — the
    // no-setter public constructor above has no way to populate id/receivedAt from a stored row.
    @PersistenceCreator
    WinNotice(Long id, String requestId, String bidderId, double clearingPrice, double bidPrice,
              LocalDateTime receivedAt) {
        this.id = id;
        this.requestId = requestId;
        this.bidderId = bidderId;
        this.clearingPrice = clearingPrice;
        this.bidPrice = bidPrice;
        this.receivedAt = receivedAt;
    }

    public Long getId() { return id; }

    public String getRequestId() { return requestId; }
    public String getBidderId() { return bidderId; }
    public double getClearingPrice() { return clearingPrice; }
    public double getBidPrice() { return bidPrice; }
    public LocalDateTime getReceivedAt() { return receivedAt; }
}
