# The Bidder

Quick reference for building your bidder. The **source of truth is the code** —
this page just orients you. Read section 4 (what to implement) first, then dip
into the rest as needed.

## 1. Overview

A **bidder** is an HTTP service that plays in Real-Time Bidding (RTB) auctions
run by the Mock SSP. For every ad impression, the SSP sends every bidder a
**bid request**; each bidder has **100ms** (`bidder.timeout-ms`) to answer with
a price and a creative, or the SSP treats it as a **no-bid**. The highest bid
above the floor price wins and that creative is shown.

After each auction the SSP broadcasts the result over **Kafka**, so your
bidder learns whether it won or lost and can track spend. Budget is tracked
per creative in **Redis**, shared live with the SSP.

A bidder that never crashes and never times out already beats a broken one —
correctness first, cleverness second.

## 2. Bid API

All JSON is `snake_case`. Unknown fields are ignored; missing required fields
are rejected.

**`POST /api/bid`** — SSP → bidder:

```json
{
  "request_id": "req-7f3e2a1b",
  "floor_price": 1.00,
  "targeting": { "geo": "US", "device_type": "mobile", "audience_segment": "sports" }
}
```

To bid, respond **HTTP 200** within the timeout:

```json
{
  "request_id": "req-7f3e2a1b",
  "bid_price": 1.35,
  "creative": {
    "id": "teads-bidder-creative-1",
    "name": "Creative 1",
    "description": "...",
    "image_url": "...",
    "call_to_action": "Learn More",
    "allowed_geos": [], "allowed_devices": [], "audience_segments": []
  }
}
```

`bid_price` must be strictly greater than `floor_price`. To decline, respond
**HTTP 204** with no body. A timeout/error/malformed body is never a 5xx (4xx
for bad input); a non-`POST` to `/api/bid` is 404/405.

Other endpoints:

| Endpoint | Description |
|---|---|
| `GET /health` | `{ "status": "UP" }` |
| `GET /actuator/prometheus` | Prometheus metrics |
| `GET /api/stats*` | Read-only performance dashboard data — see [STATS_API.md](STATS_API.md) |

Exact request/response shapes: `bidding/dto/*.java`. Endpoint wiring:
`bidding/BidController.java`.

## 3. Data model

Each bidder is isolated in its own Postgres schema, `bidder_<sanitized id>`,
created automatically at startup — no manual DDL.

- **`creatives`** (`creative/Creative.java`) — your ad pool, seeded with 200
  randomised creatives on first boot (`CreativeSeeder`). Besides the usual
  display fields (`name`, `description`, `image_url`, `call_to_action`), each
  row carries `budget` (starting budget; live remaining lives in Redis),
  `max_bid_price` (nullable cap on `floor_price`, `null` = unbounded), and
  three CSV targeting columns — `allowed_geos`, `allowed_devices`,
  `audience_segments` (empty = no restriction, matching is case-insensitive).
- **`bid_record`** — one row per incoming request (`record/BidRecord.java`),
  logging what you decided and why: the request's targeting, your `bid_price`
  (`null` on no-bid) and `creative_id`, `latency_ms`, and `no_bid_reason`
  (`no_eligible_creative` / `targeting_miss` / `budget_exhausted` /
  `floor_exceeds_max_bid`).
- **`win_notice`** — one row per confirmed win (`notification/WinNotice.java`):
  `request_id`, `clearing_price`, `bid_price`, `received_at`.

### Redis — live budget

`record/BidderStatsCache.java` keeps each creative's *remaining* budget in
Redis (Postgres only holds the seeded/last-synced value):

| Key | Value | Set by |
|---|---|---|
| `{bidderId}_{creativeId}_budget` | remaining budget, e.g. `18.42` | `initBudget` on startup (`= bidder.creative-budget`, default `25.0`); decremented by `clearing_price` in `recordWin` on every confirmed Kafka win |

A creative may only bid while this value is **strictly greater than `0`**.
If the key is missing (e.g. Redis was flushed), it's treated as full budget
and re-initialized on next read. After each decrement, `recordWin` also
writes the new value back to `creatives.budget` in Postgres, so a restart
doesn't lose it even if Redis is wiped.

### Kafka — auction results

The SSP publishes one protobuf message per auction to `ssp.auction-notifications`
(`src/main/proto/auction_notice.proto`):

```proto
message AuctionNotice {
  string request_id        = 1;  // matches the bid_record you wrote for this auction
  double clearing_price     = 2;  // what the winner actually pays
  string winning_bidder_id  = 3;  // your bidder.id if you won
}
```

`notification/AuctionNoticeConsumer.java` listens on this topic for every
message, win or lose, and figures out which case applies by comparing
`winning_bidder_id` to your own `bidder.id`:

- **You won** — it looks up the `creative_id` from the `bid_record` matching
  `request_id` (the win notice itself doesn't carry a creative id), decrements
  that creative's Redis budget by `clearing_price`, and saves a `win_notice`.
  If no matching `bid_record` exists, it logs a warning and skips the budget
  update (nothing to decrement).
- **You lost** — if you had bid on that request, it just logs the gap between
  your bid and the clearing price; no state changes.

## 4. Feature List

Features are ordered from easiest to most complex — implement them in order.
They're all part of the same bidder: F0 alone gets you a bidder that never
does anything smart, F1–F3 make it correct, PERF keeps it correct under load.
The bidding logic to change lives in `bidding/BiddingService.java`; everything
else (HTTP wiring, DTOs, repositories, Redis/Kafka plumbing, metrics) is
provided.

| # | Difficulty | Feature | Description |
|---|---|---|---|
| **F0** | 🟢 Easy | **Basic bidding** | Bid only when at least one of your creatives is eligible for the request. Respond `200` with a valid body (`request_id` echoed, `bid_price` strictly above `floor_price`, `creative` one of your own) whenever one is. Respond `204` when none is. |
| **F1** | 🟢 Easy | **Audience targeting** | Only bid with creatives whose `allowed_geos`, `allowed_devices`, and `audience_segments` match the request's `geo`, `device_type`, and `audience_segment`. An empty restriction on a dimension accepts any value; matching is case-insensitive; **all three** dimensions must pass or the creative is out. `204` only when none match. ⭐ **Going further** — when several creatives match, is picking one at random the best choice? Consider favouring the most specific match or the highest-value creative. |
| **F2** | 🟢 Easy | **Budget control** | A creative may only bid while its remaining budget (in Redis) is strictly above `0`. Skip exhausted creatives, fall back to one that still has budget, and return `204` when every matching creative is exhausted. ⭐ **Going further** — rather than spending flat-out until a creative runs dry, think about **pacing**: spread each creative's budget across the competition window (`bidder.competition.*`, `strategy.pacing-boost`/`pacing-cut`) so it doesn't exhaust early. |
| **F3** | 🟢 Easy | **Max bid price cap** | Each creative has a `max_bid_price` — the highest floor price it's willing to pay. A creative must never be selected for a request whose `floor_price` exceeds its cap, checked *before* targeting and budget. `null` means unbounded. Return `204` when the only eligible creatives are priced out (`no_bid_reason = floor_exceeds_max_bid`). |
| **PERF** | 🟠 Medium | **Performance under load** | Stay correct and fast under concurrency: every response is a `200` or `204` within the 100ms SLA, even under bursts of 5–100 simultaneous requests. A bidder that passes F0–F3 functionally but blocks on DB queries or holds a global lock will start missing the SLA as load climbs. Watch for connection-pool saturation and unbounded synchronous work in the hot path. |

⭐ **Going further (open-ended)** — beyond the features above, the reference
bidder ships with hooks you can build on:

- **Smart creative selection** — favour higher-value or better-targeted creatives instead of picking one at random.
- **Dynamic bid pricing** — the bidder tracks win rate and recent clearing prices (`BidderStatsCache`, with `strategy.market-multiplier`/`cold-start-multiplier`/`window-size`). Learn from past auctions: bid lower when you win too easily, more aggressively when you keep losing — while never dropping at or below the floor.

## 5. Architecture

Infrastructure (Postgres, Kafka, Redis, Prometheus, Grafana) now lives in the
companion **`ssp`** repo — start it there first with `make run`. This repo
only builds and runs the bidder container, which joins that stack's
`ss2026-net` Docker network so `postgres`/`kafka`/`redis` resolve by name.

## 6. Running it

Java 21, Spring Boot, Maven. `ddl-auto=update` creates your schema/tables on
boot; `CreativeSeeder` seeds 200 creatives the first time.

```
make install           Download Maven dependencies
make build             Compile and package (skip tests)
make run               Build & run the bidder container (requires ssp's `make run` already up)
make run-team          Custom port/id: make run-team PORT=8081 BIDDER_ID=team-alpha
make restart-bidder    Reset this bidder's budget in Postgres+Redis and restart
make down              Stop the bidder container
make test              Run unit tests
make clean             Remove build artifacts
make run-prod           Prod mode against AWS backing services (needs config.prod.env)
make down-prod / logs-prod
```

Key config lives in `src/main/resources/application.properties`
(`bidder.id`, budgets, `timeout-ms`, `strategy.*`, `competition.*`) and
`config.env` (id, ngrok URL, Kafka/Tailscale endpoint).