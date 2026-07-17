package com.teads.summerschool.bidding;

import com.teads.summerschool.bidding.dto.BidRequest;
import com.teads.summerschool.bidding.dto.BidResponse;
import com.teads.summerschool.bidding.dto.Targeting;
import com.teads.summerschool.config.BidderProperties;
import com.teads.summerschool.creative.Creative;
import com.teads.summerschool.creative.CreativeCache;
import com.teads.summerschool.metrics.BidderMetrics;
import com.teads.summerschool.record.BidRecord;
import com.teads.summerschool.record.BidRecordRepository;
import com.teads.summerschool.record.BidderStatsCache;
import com.teads.summerschool.record.OwnBidCache;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class BiddingServiceTest {

    @Mock
    private BidderProperties properties;

    @Mock
    private CreativeCache creativeCache;

    @Mock
    private BidRecordRepository bidRecordRepository;

    @Mock
    private BidderStatsCache statsCache;

    @Mock
    private BidderMetrics metrics;

    @Mock
    private OwnBidCache ownBidCache;

    private BiddingService service;

    @BeforeEach
    void setUp() {
        service = new BiddingService(
            properties,
            creativeCache,
            bidRecordRepository,
            statsCache,
            metrics,
            ownBidCache
        );

        // Setup default property mocks (lenient since not all tests use all mocks)
        BidderProperties.Strategy strategy = new BidderProperties.Strategy();
        strategy.setWindowSize(50);
        strategy.setColdStartMultiplier(1.15);
        strategy.setMarketMultiplier(1.05);
        lenient().when(properties.getStrategy()).thenReturn(strategy);
        lenient().when(properties.getTimeoutMs()).thenReturn(300L);
        lenient().when(properties.getId()).thenReturn("test-bidder");

        lenient().when(properties.getCreativeBudget()).thenReturn(25.0);

        // Segment stats cached — return cold-start (empty) stats by default
        lenient().when(statsCache.getSegmentStatsCached(anyString()))
            .thenReturn(BidderStatsCache.SegmentStats.empty());

        // Budget ranking: return full budget by default so ranking reflects specificity
        lenient().when(statsCache.getCachedRemainingBudget(anyString())).thenReturn(25.0);

        // Mock bid record repository to return saved record
        when(bidRecordRepository.save(any(BidRecord.class)))
            .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
    }

    @Test
    void testLayer1Fail_AllCreativesExceedMaxBidPrice() {
        // ARRANGE: All creatives have maxBidPrice below floor
        Creative c1 = createCreative("c1", 0.50, "", "", "");
        Creative c2 = createCreative("c2", 0.60, "", "", "");

        BidRequest request = new BidRequest(
            "req-1",
            1.0,  // floorPrice higher than all maxBidPrice
            new Targeting("US", "mobile", "sports"),
            "1.2.3.4"
        );

        when(creativeCache.getAll()).thenReturn(Flux.just(c1, c2));

        // ACT
        Optional<BidResponse> response = service.bid(request).block();

        // ASSERT
        assertThat(response).isEmpty();

        ArgumentCaptor<BidRecord> recordCaptor = ArgumentCaptor.forClass(BidRecord.class);
        verify(bidRecordRepository).save(recordCaptor.capture());
        BidRecord saved = recordCaptor.getValue();

        assertThat(saved.getNoBidReason()).isEqualTo("floor_exceeds_max_bid");
        assertThat(saved.getBidPrice()).isNull();
        assertThat(saved.getCreativeId()).isNull();

        verify(metrics).recordNoBid("floor_exceeds_max_bid");
        verify(statsCache, never()).getRemainingBudget(anyString());
    }

    @Test
    void testLayer2Fail_AllCreativesMissTargeting() {
        // ARRANGE: All creatives pass Layer 1 but fail targeting
        Creative c1 = createCreative("c1", null, "CA", "", "");  // Wrong geo
        Creative c2 = createCreative("c2", null, "GB", "", "");  // Wrong geo

        BidRequest request = new BidRequest(
            "req-2",
            1.0,
            new Targeting("US", "mobile", "sports"),
            "1.2.3.4"
        );

        when(creativeCache.getAll()).thenReturn(Flux.just(c1, c2));

        // ACT
        Optional<BidResponse> response = service.bid(request).block();

        // ASSERT
        assertThat(response).isEmpty();

        ArgumentCaptor<BidRecord> recordCaptor = ArgumentCaptor.forClass(BidRecord.class);
        verify(bidRecordRepository).save(recordCaptor.capture());
        BidRecord saved = recordCaptor.getValue();

        assertThat(saved.getNoBidReason()).isEqualTo("targeting_miss");
        verify(metrics).recordNoBid("targeting_miss");
        verify(statsCache, never()).getRemainingBudget(anyString());
    }

    @Test
    void testLayer3Fail_AllCreativesBudgetExhausted() {
        // ARRANGE: Creatives pass Layer 1 and 2, but have no budget
        Creative c1 = createCreative("c1", null, "US", "mobile", "sports");
        Creative c2 = createCreative("c2", null, "US", "", "");  // Universal targeting

        BidRequest request = new BidRequest(
            "req-3",
            1.0,
            new Targeting("US", "mobile", "sports"),
            "1.2.3.4"
        );

        when(creativeCache.getAll()).thenReturn(Flux.just(c1, c2));
        when(statsCache.getRemainingBudgets(anyList()))
            .thenReturn(Mono.just(Map.of("c1", 0.0, "c2", 0.0)));

        // ACT
        Optional<BidResponse> response = service.bid(request).block();

        // ASSERT
        assertThat(response).isEmpty();

        ArgumentCaptor<BidRecord> recordCaptor = ArgumentCaptor.forClass(BidRecord.class);
        verify(bidRecordRepository).save(recordCaptor.capture());
        BidRecord saved = recordCaptor.getValue();

        assertThat(saved.getNoBidReason()).isEqualTo("budget_exhausted");
        verify(metrics).recordNoBid("budget_exhausted");
    }

    @Test
    void testCreativeSelection_SpecificityWins() {
        // ARRANGE: Multiple survivors, more specific creative should win
        Creative universal = createCreative("universal", null, "", "", "");  // Specificity: 0
        Creative geoTargeted = createCreative("geo", null, "US", "", "");  // Specificity: 1
        Creative fullyTargeted = createCreative("full", null, "US", "mobile", "sports");  // Specificity: 3

        BidRequest request = new BidRequest(
            "req-4",
            1.0,
            new Targeting("US", "mobile", "sports"),
            "1.2.3.4"
        );

        when(creativeCache.getAll()).thenReturn(Flux.just(universal, geoTargeted, fullyTargeted));
        when(statsCache.getRemainingBudgets(anyList()))
            .thenReturn(Mono.just(Map.of("universal", 100.0, "geo", 100.0, "full", 100.0)));
        when(statsCache.reserveBudget(anyString(), anyDouble())).thenReturn(Mono.just(98.0));

        // ACT
        Optional<BidResponse> response = service.bid(request).block();

        // ASSERT
        assertThat(response).isPresent();
        assertThat(response.get().creative().id()).isEqualTo("full");  // Most specific

        ArgumentCaptor<BidRecord> recordCaptor = ArgumentCaptor.forClass(BidRecord.class);
        verify(bidRecordRepository).save(recordCaptor.capture());
        BidRecord saved = recordCaptor.getValue();

        assertThat(saved.getCreativeId()).isEqualTo("full");
        assertThat(saved.getBidPrice()).isNotNull();
        assertThat(saved.getNoBidReason()).isNull();

        verify(metrics).recordBid();
        verify(ownBidCache).record(eq("req-4"), eq("full"), anyDouble(), any(), any(), any());
    }

    @Test
    void testCreativeSelection_TiebreakByMaxBidPrice() {
        // ARRANGE: Two creatives with same specificity, higher maxBidPrice wins
        Creative low = createCreative("low", 2.0, "US", "mobile", "");  // Specificity: 2, maxBid: 2.0
        Creative high = createCreative("high", 5.0, "US", "mobile", "");  // Specificity: 2, maxBid: 5.0

        BidRequest request = new BidRequest(
            "req-5",
            1.0,
            new Targeting("US", "mobile", "tech"),
            "1.2.3.4"
        );

        when(creativeCache.getAll()).thenReturn(Flux.just(low, high));
        when(statsCache.getRemainingBudgets(anyList()))
            .thenReturn(Mono.just(Map.of("low", 100.0, "high", 100.0)));
        when(statsCache.reserveBudget(anyString(), anyDouble())).thenReturn(Mono.just(98.0));

        // ACT
        Optional<BidResponse> response = service.bid(request).block();

        // ASSERT
        assertThat(response).isPresent();
        assertThat(response.get().creative().id()).isEqualTo("high");  // Higher maxBidPrice wins tiebreak

        ArgumentCaptor<BidRecord> recordCaptor = ArgumentCaptor.forClass(BidRecord.class);
        verify(bidRecordRepository).save(recordCaptor.capture());
        BidRecord saved = recordCaptor.getValue();

        assertThat(saved.getCreativeId()).isEqualTo("high");
    }

    @Test
    void testCreativeSelection_NullMaxBidPriceTreatedAsZero() {
        // ARRANGE: One with null maxBidPrice (0), one with 2.0
        Creative nullMax = createCreative("null", null, "US", "", "");  // maxBid: null (treated as 0)
        Creative withMax = createCreative("with", 2.0, "US", "", "");  // maxBid: 2.0

        BidRequest request = new BidRequest(
            "req-6",
            1.0,
            new Targeting("US", "mobile", "tech"),
            "1.2.3.4"
        );

        when(creativeCache.getAll()).thenReturn(Flux.just(nullMax, withMax));
        when(statsCache.getRemainingBudgets(anyList()))
            .thenReturn(Mono.just(Map.of("null", 100.0, "with", 100.0)));
        when(statsCache.reserveBudget(anyString(), anyDouble())).thenReturn(Mono.just(98.0));

        // ACT
        Optional<BidResponse> response = service.bid(request).block();

        // ASSERT
        assertThat(response).isPresent();
        assertThat(response.get().creative().id()).isEqualTo("with");  // Non-null wins tiebreak
    }

    @Test
    void testSuccessfulBid_RecordsAllMetadata() {
        // ARRANGE
        Creative creative = createCreative("winner", null, "US", "mobile", "sports");

        BidRequest request = new BidRequest(
            "req-7",
            2.0,
            new Targeting("US", "mobile", "sports"),
            "1.2.3.4"
        );

        when(creativeCache.getAll()).thenReturn(Flux.just(creative));
        when(statsCache.getRemainingBudgets(anyList()))
            .thenReturn(Mono.just(Map.of("winner", 200.0)));
        when(statsCache.reserveBudget(eq("winner"), anyDouble())).thenReturn(Mono.just(197.0));

        // ACT
        Optional<BidResponse> response = service.bid(request).block();

        // ASSERT
        assertThat(response).isPresent();
        assertThat(response.get().requestId()).isEqualTo("req-7");
        assertThat(response.get().bidPrice()).isGreaterThan(2.0);  // Should be > floor

        ArgumentCaptor<BidRecord> recordCaptor = ArgumentCaptor.forClass(BidRecord.class);
        verify(bidRecordRepository).save(recordCaptor.capture());
        BidRecord saved = recordCaptor.getValue();

        assertThat(saved.getRequestId()).isEqualTo("req-7");
        assertThat(saved.getFloorPrice()).isEqualTo(2.0);
        assertThat(saved.getGeo()).isEqualTo("US");
        assertThat(saved.getDeviceType()).isEqualTo("mobile");
        assertThat(saved.getAudienceSegment()).isEqualTo("sports");
        assertThat(saved.getCreativeId()).isEqualTo("winner");
        assertThat(saved.getBidPrice()).isNotNull();
        assertThat(saved.getNoBidReason()).isNull();
        assertThat(saved.getLatencyMs()).isGreaterThan(0);

        verify(metrics).recordBid();
        verify(metrics).recordLatency(saved.getLatencyMs());
        verify(ownBidCache).record(eq("req-7"), eq("winner"), anyDouble(), any(), any(), any());
    }

    @Test
    void testLayer3OnlyChecksLayer2Survivors() {
        // ARRANGE: 4 creatives total
        // - 2 fail Layer 1 (maxBidPrice too low)
        // - 1 fails Layer 2 (targeting miss)
        // - 1 passes all layers
        Creative failLayer1a = createCreative("fail1a", 0.5, "US", "mobile", "sports");
        Creative failLayer1b = createCreative("fail1b", 0.6, "US", "mobile", "sports");
        Creative failLayer2 = createCreative("fail2", null, "CA", "mobile", "sports");  // Wrong geo
        Creative winner = createCreative("winner", null, "US", "mobile", "sports");

        BidRequest request = new BidRequest(
            "req-8",
            1.0,  // Fails fail1a and fail1b
            new Targeting("US", "mobile", "sports"),
            "1.2.3.4"
        );

        when(creativeCache.getAll()).thenReturn(Flux.just(failLayer1a, failLayer1b, failLayer2, winner));
        when(statsCache.getRemainingBudgets(anyList()))
            .thenReturn(Mono.just(Map.of("winner", 100.0)));
        when(statsCache.reserveBudget(eq("winner"), anyDouble())).thenReturn(Mono.just(98.0));

        // ACT
        Optional<BidResponse> response = service.bid(request).block();

        // ASSERT
        assertThat(response).isPresent();

        // Budget reservation should only happen for "winner" (the selected creative)
        verify(statsCache).reserveBudget(eq("winner"), anyDouble());
        verify(statsCache, never()).reserveBudget(eq("fail1a"), anyDouble());
        verify(statsCache, never()).reserveBudget(eq("fail1b"), anyDouble());
        verify(statsCache, never()).reserveBudget(eq("fail2"), anyDouble());
    }

    // Helper method to create test creatives
    private Creative createCreative(String id, Double maxBidPrice, String geos, String devices, String segments) {
        Creative c = new Creative();
        c.setId(id);
        c.setName("Creative " + id);
        c.setDescription("Test creative");
        c.setImageUrl("https://example.com/" + id + ".jpg");
        c.setCallToAction("Click Here");
        c.setBidderId("test-bidder");
        c.setBudget(25.0);
        c.setMaxBidPrice(maxBidPrice);
        c.setAllowedGeos(geos);
        c.setAllowedDevices(devices);
        c.setAudienceSegments(segments);
        return c;
    }
}
