package com.teads.summerschool.notification;

import com.teads.summerschool.config.BidderProperties;
import com.teads.summerschool.metrics.BidderMetrics;
import com.teads.summerschool.proto.AuctionNoticeProto;
import com.teads.summerschool.record.BidRecord;
import com.teads.summerschool.record.BidRecordRepository;
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
    private final BidRecordRepository bidRecordRepository;

    public AuctionNoticeConsumer(WinNoticeRepository winNoticeRepository,
                                 BidderProperties properties,
                                 BidderStatsCache statsCache,
                                 BidderMetrics metrics,
                                 OwnBidCache ownBidCache,
                                 BidRecordRepository bidRecordRepository) {
        this.winNoticeRepository = winNoticeRepository;
        this.properties = properties;
        this.statsCache = statsCache;
        this.metrics = metrics;
        this.ownBidCache = ownBidCache;
        this.bidRecordRepository = bidRecordRepository;
    }

    @KafkaListener(topics = "${kafka.topic.auction-notifications}",
            autoStartup = "${spring.kafka.listener.auto-startup:true}")
    public void consume(byte[] message) {
        try {
            AuctionNoticeProto.AuctionNotice notice = AuctionNoticeProto.AuctionNotice.parseFrom(message);

            // This topic broadcasts EVERY auction's outcome to EVERY bidder, so most
            // messages a bidder receives are ones it never bid on. Filter on the
            // in-memory OwnBidCache (see BiddingService.bid()) BEFORE touching Redis or
            // Postgres — an O(1) local lookup instead of a DB round trip on every message.
            OwnBidCache.Entry ourBid = ownBidCache.get(notice.getRequestId());
            if (ourBid == null) {
                return;
            }

            boolean won = properties.getId().equals(notice.getWinningBidderId());

            log.debug("KAFKA  id={} winner={} won={}", notice.getRequestId(), notice.getWinningBidderId(), won);

            if (won) {
                String segmentKey = buildSegmentKeyForRequest(notice.getRequestId());

                // Record win in per-segment stats
                if (!segmentKey.isEmpty()) {
                    statsCache.recordWinForSegment(
                        segmentKey,
                        notice.getClearingPrice(),
                        ourBid.bidPrice()
                    ).block();
                }

                // Existing budget tracking
                statsCache.recordWin(ourBid.creativeId(), notice.getClearingPrice()).block();

                // Save win notice to DB
                WinNotice winNotice = new WinNotice(
                    notice.getRequestId(),
                    properties.getId(),
                    notice.getClearingPrice(),
                    ourBid.bidPrice()
                );
                winNoticeRepository.save(winNotice).block();

                // Record metrics
                metrics.recordWin(notice.getClearingPrice());

                log.info("** WIN  id={} creative={} clearing={} segment={}",
                        notice.getRequestId(), ourBid.creativeId(), notice.getClearingPrice(), segmentKey);
            } else {
                String segmentKey = buildSegmentKeyForRequest(notice.getRequestId());

                // Record loss in per-segment stats
                if (!segmentKey.isEmpty()) {
                    statsCache.recordLossForSegment(segmentKey, ourBid.bidPrice()).block();
                }

                // Record metrics
                metrics.recordLoss();

                log.debug("LOSS id={} bid={} segment={}",
                        notice.getRequestId(), ourBid.bidPrice(), segmentKey);
            }
        } catch (Exception e) {
            log.error("** KAFKA ERROR  failed to process auction notice: {}", e.getMessage());
        }
    }

    /**
     * Query BidRecord and build segment key from targeting dimensions.
     * Returns empty string if BidRecord not found or targeting is incomplete.
     */
    private String buildSegmentKeyForRequest(String requestId) {
        BidRecord record = bidRecordRepository.findByRequestId(requestId).block();
        if (record == null) {
            log.warn("BidRecord not found for requestId={}", requestId);
            return "";
        }
        return BidderStatsCache.buildSegmentKey(
            record.getGeo(),
            record.getDeviceType(),
            record.getAudienceSegment()
        );
    }
}
