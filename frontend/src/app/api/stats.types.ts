// TypeScript types for the Bidder Stats API responses.
// Field names match the API exactly (snake_case), so no conversion is needed.
// See the API definition: https://github.com/teads-sess-2026/bidder/blob/main/STATS_API.md

// --- GET /api/stats ---

export interface LatencyMs {
    avg: number;
    p50: number;
    p95: number;
    max: number;
    count: number;
}

export interface NoBidReasons {
    budget_exhausted: number;
    no_eligible_creative: number;
    targeting_miss: number;
}

export interface Pacing {
    spend_per_minute: number;
    elapsed_minutes: number;
    projected_minutes_to_exhaustion: number | null;
    budget_utilization: number;
}

export interface Stats {
    bidder_id: string;
    generated_at: string;
    total_auctions: number;
    bids: number;
    no_bids: number;
    bid_rate: number;
    wins: number;
    win_rate: number;
    win_rate_per_auction: number;
    avg_bid_price: number;
    avg_win_price: number;
    total_spend: number;
    remaining_budget: number;
    budget: number;
    latency_ms: LatencyMs;
    no_bid_reasons: NoBidReasons;
    pacing: Pacing;
}

// --- GET /api/stats/creatives ---

export interface Creative {
    creative_id: string;
    creative_name: string | null;
    bids: number;
    wins: number;
    win_rate: number;
    avg_bid_price: number;
    avg_win_price: number;
    spend: number;
}

export interface CreativesResponse {
    bidder_id: string;
    creatives: Creative[];
}

// --- GET /api/stats/targeting ---

export interface TargetingBucket {
    key: string;
    bids: number;
    wins: number;
    win_rate: number;
    avg_bid_price: number;
}

export interface TargetingResponse {
    bidder_id: string;
    by_geo: TargetingBucket[];
    by_device: TargetingBucket[];
    by_segment: TargetingBucket[];
}

// --- GET /api/stats/timeseries ---

export interface Point {
    time: string;
    auctions: number;
    bids: number;
    wins: number;
    bid_rate: number;
    win_rate: number;
    avg_bid_price: number;
    spend: number;
}

export interface TimeseriesResponse {
    bidder_id: string;
    window_minutes: number;
    bucket_seconds: number;
    points: Point[];
}
