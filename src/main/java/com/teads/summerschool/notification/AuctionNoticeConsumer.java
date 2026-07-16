package com.teads.summerschool.notification;

import com.teads.summerschool.config.BidderProperties;
import com.teads.summerschool.metrics.BidderMetrics;
import com.teads.summerschool.proto.AuctionNoticeProto;
import com.teads.summerschool.record.BidderStatsCache;
import com.teads.summerschool.record.OwnBidCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class AuctionNoticeConsumer {

    private static final Logger log = LoggerFactory.getLogger(AuctionNoticeConsumer.class);

    private final WinNoticeRepository winNoticeRepository;
    private final BidderProperties properties;
    private final BidderStatsCache statsCache;
    private final BidderMetrics metrics;
    private final OwnBidCache ownBidCache;

    public AuctionNoticeConsumer(WinNoticeRepository winNoticeRepository,
                                 BidderProperties properties,
                                 BidderStatsCache statsCache,
                                 BidderMetrics metrics,
                                 OwnBidCache ownBidCache) {
        this.winNoticeRepository = winNoticeRepository;
        this.properties = properties;
        this.statsCache = statsCache;
        this.metrics = metrics;
        this.ownBidCache = ownBidCache;
    }

    @KafkaListener(topics = "${kafka.topic.auction-notifications}",
            autoStartup = "${spring.kafka.listener.auto-startup:true}")
    public void consume(byte[] message) {
        try {
            AuctionNoticeProto.AuctionNotice notice = AuctionNoticeProto.AuctionNotice.parseFrom(message);

            OwnBidCache.Entry ourBid = ownBidCache.get(notice.getRequestId());
            if (ourBid == null) {
                return;
            }

            boolean won = properties.getId().equals(notice.getWinningBidderId());

            // Build segment key from in-memory OwnBidCache — no Postgres lookup needed
            String segmentKey = BidderStatsCache.buildSegmentKey(
                    ourBid.geo(), ourBid.deviceType(), ourBid.audienceSegment());

            log.debug("KAFKA  id={} winner={} won={}", notice.getRequestId(), notice.getWinningBidderId(), won);

            if (won) {
                // Update in-memory segment stats first (instant, no I/O)
                if (!segmentKey.isEmpty()) {
                    statsCache.recordWinForSegment(
                        segmentKey,
                        notice.getClearingPrice(),
                        ourBid.bidPrice()
                    ).subscribe();
                }

                // Refund overpayment in-memory, then async Postgres sync
                statsCache.recordWin(ourBid.creativeId(), notice.getClearingPrice(), ourBid.bidPrice()).subscribe();

                // Async save win notice to Postgres
                WinNotice winNotice = new WinNotice(
                    notice.getRequestId(),
                    properties.getId(),
                    notice.getClearingPrice(),
                    ourBid.bidPrice()
                );
                winNoticeRepository.save(winNotice).subscribe();

                metrics.recordWin(notice.getClearingPrice());

                log.info("** WIN  id={} creative={} clearing={} segment={}",
                        notice.getRequestId(), ourBid.creativeId(), notice.getClearingPrice(), segmentKey);
            } else {
                // Update in-memory segment stats first
                if (!segmentKey.isEmpty()) {
                    statsCache.recordLossForSegment(segmentKey, ourBid.bidPrice()).subscribe();
                }

                // Refund entire reservation in-memory, then async Postgres sync
                statsCache.recordLoss(ourBid.creativeId(), ourBid.bidPrice()).subscribe();

                metrics.recordLoss();

                log.debug("LOSS id={} bid={} segment={}",
                        notice.getRequestId(), ourBid.bidPrice(), segmentKey);
            }
        } catch (Exception e) {
            log.error("** KAFKA ERROR  failed to process auction notice: {}", e.getMessage());
        }
    }
}
