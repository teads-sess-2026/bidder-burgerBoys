# Bidder Template

Spring Boot bidder skeleton for Summer School 2026.

## What to implement

Three areas are stubbed — look for `// TODO` comments:

| File | What to implement |
|---|---|
| `BiddingService.java` | `bid()` — decide whether/how much to bid; `computeBidPrice()` — your pricing strategy |
| `AuctionNoticeConsumer.java` | `consume()` — handle win and loss Kafka notices, track budget |
| `StatsService.java` | `getStats()`, `getCreativeStats()`, `getTargetingStats()`, `getTimeseries()` — the dashboard API |

---

## Local development (no AWS/ssp repo needed)

Want to run and test your bidder standalone, without the AWS-hosted infra or the ssp
repo? Bring up your own local Postgres/Kafka/Redis/Prometheus/Grafana, then run the
bidder against it:

```bash
make run       # starts local infra (if not already up) + the bidder container
```

`make run` = `make run-infra` + `make run-docker` — see `make help` to run either step
on its own (`make run-infra`, `make down-infra`, `make infra-logs`).

This creates the same `ss2026-net` Docker network the ssp repo's `docker-compose.all.yml`
creates, so only run one or the other — not both at once.

|                | URL |
|---|---|
| Kafka UI  | http://localhost:8090 |
| Redis UI  | http://localhost:8091 |
| Prometheus | http://localhost:9090 |
| Grafana   | http://localhost:3000 (admin / admin) |

Once your bidder is running, see [Testing your bidder](#testing-your-bidder) below.

---

## Workshop setup

Infrastructure (Postgres/Kafka/Redis/Prometheus/Grafana) runs on an always-on AWS VM
(see `../infra/terraform`). SSP and every bidder run locally on the same local network,
connecting out to that AWS-hosted infra — no public tunnels (ngrok/Tailscale) needed.

---

### Step 1 — Configure config.prod.env

```bash
cp config.prod.env.example config.prod.env
```

Fill in `VM_HOST`/`DB_PASSWORD` (ask the organiser, or read them from Terraform outputs if
you're the organiser):

```bash
cd ../infra/terraform
VM_HOST=$(tofu output -raw vm_public_ip)
DB_PASSWORD=$(tofu output -raw db_password)
```

> Your machine must be in the Terraform `data_ingress_cidrs` allowlist to reach these ports.

---

### Step 2 — Register with the SSP

Edit `config.env`:

```env
BIDDER_ID=your-team-name
BIDDER_ENDPOINT_URL=http://<your-local-ip>:8080     # SSP reaches you directly over the local network
```

Tell the workshop organiser your `BIDDER_ID` and `BIDDER_ENDPOINT_URL`. They will add you to
`ssp/bidders.json`.

---

### Step 3 — Run

```bash
make run-prod
```

---

## All commands

```bash
make run-prod                                       Start the bidder container against the AWS-hosted infra (config.prod.env)
make run-team-prod PORT=9001 BIDDER_ID=team-alpha   Same, with a custom port/id
make down-prod                                       Stop the prod-mode bidder container
make logs-prod                                       Tail the prod-mode bidder container logs
make build                                           Compile and package (skip tests)
make test                                            Run unit tests
make clean                                           Remove build artifacts
```

> Local-only mode (`make run`, joining a local `ss2026-net` Docker network instead of the
> AWS-hosted infra) is also available — see `make help`.

---

## Key endpoints

| Endpoint | Description |
|---|---|
| `POST /api/bid` | Receive a bid request from the SSP |
| `GET /api/budget` | Current remaining budget (polled by SSP every second) |
| `GET /health` | Health check |
| `GET /api/stats` | Overall performance snapshot |
| `GET /api/stats/creatives` | Per-creative breakdown |
| `GET /api/stats/targeting` | Breakdown by geo / device / segment |
| `GET /api/stats/timeseries` | Time-bucketed trend data |
| `GET /actuator/prometheus` | Prometheus metrics |

---

## Testing your bidder

Once it's running (`make run`, default port 8080), print ready-to-copy examples with:

```bash
make curl-examples                 # or: make curl-examples PORT=8081
```

Or run them directly:

```bash
# Health check
curl http://localhost:8080/health

# Send a bid request (mirrors what the SSP sends)
curl -X POST http://localhost:8080/api/bid \
  -H 'Content-Type: application/json' \
  -d '{
    "request_id": "test-1",
    "floor_price": 0.10,
    "targeting": {
      "geo": "US",
      "device_type": "mobile",
      "audience_segment": "tech"
    }
  }'
# → 200 with a bid body if you bid, or 204 No Content if you pass.

# Check remaining budget
curl http://localhost:8080/api/budget

# Dashboard/API stats
curl http://localhost:8080/api/stats
curl http://localhost:8080/api/stats/creatives
curl http://localhost:8080/api/stats/targeting
curl http://localhost:8080/api/stats/timeseries

# Prometheus metrics
curl http://localhost:8080/actuator/prometheus
```

---

## Configuration reference

| Variable | File | Description |
|---|---|---|
| `BIDDER_ID` | `config.env` | Your team's unique ID — must match `id` in `ssp/bidders.json` |
| `BIDDER_ENDPOINT_URL` | `config.env` | Your machine's local HTTP address — SSP and every bidder run on the same local network |
| `VM_HOST`, `DB_PASSWORD`, etc. | `config.prod.env` | AWS-hosted Postgres/Kafka/Redis connection details — see `config.prod.env.example` for the full list |

Bidder strategy, budget, and competition timing are in `src/main/resources/application.properties`.
