package com.teads.summerschool.stats;

import com.teads.summerschool.config.BidderProperties;
import com.teads.summerschool.stats.dto.CreativeStatsResponse;
import com.teads.summerschool.stats.dto.StatsResponse;
import com.teads.summerschool.stats.dto.TargetingResponse;
import com.teads.summerschool.stats.dto.TimeseriesResponse;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Service
public class StatsService {

    private final BidderProperties properties;

    public StatsService(BidderProperties properties) {
        this.properties = properties;
    }

    // TODO: implement — query BidRecordRepository and WinNoticeRepository to compute stats
    public StatsResponse getStats() {
        return new StatsResponse(
                properties.getId(),
                LocalDateTime.now(),
                0, 0, 0, 0.0,
                0, 0.0, 0.0,
                0.0, 0.0, 0.0,
                0.0, properties.getBudget(),
                new StatsResponse.LatencyStats(0.0, 0, 0, 0, 0),
                new StatsResponse.NoBidReasons(0, 0, 0),
                new StatsResponse.PacingStats(0.0, 0.0, null, 0.0)
        );
    }

    // TODO: implement — aggregate BidRecord + WinNotice per creative
    // Valid sort values: spend, wins, bids, bid_rate, win_rate
    // Valid order values: asc, desc
    public CreativeStatsResponse getCreativeStats(String creativeId, String sort, String order) {
        return new CreativeStatsResponse(properties.getId(), List.of());
    }

    // TODO: implement — group BidRecord rows by geo / deviceType / audienceSegment
    // Valid dimension values: geo, device, segment, all
    public TargetingResponse getTargetingStats(String dimension) {
        return new TargetingResponse(properties.getId(), List.of(), List.of(), List.of());
    }

    // TODO: implement — bucket BidRecord rows into time windows
    // windowMinutes: clamp to [1, 180]; bucketSeconds: clamp to min 10
    public TimeseriesResponse getTimeseries(int windowMinutes, int bucketSeconds) {
        int clampedWindow = Math.max(1, Math.min(180, windowMinutes));
        int clampedBucket = Math.max(10, bucketSeconds);
        return new TimeseriesResponse(properties.getId(), clampedWindow, clampedBucket, List.of());
    }
}
