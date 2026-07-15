.DEFAULT_GOAL := help
-include config.env
export
MVN          := ./mvnw
PORT         ?= 8080
BIDDER_ID    ?= teads-bidder
DB_HOST      ?= localhost
REDIS_HOST   ?= $(DB_HOST)
CREATIVE_BUDGET ?= 25.0
SCHEMA       := bidder_$(shell echo $(BIDDER_ID) | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9]+/_/g')

.PHONY: help install build run run-infra down-infra infra-logs run-team run-docker restart-bidder down test clean run-prod run-team-prod down-prod logs-prod curl-examples

help:
	@echo ""
	@echo "  Bidder — Summer School 2026"
	@echo ""
	@echo "  Infrastructure — run your own local Postgres/Kafka/Redis/Prometheus/Grafana"
	@echo "  (docker-compose.infra.yml), or point at the ssp repo's shared stack instead"
	@echo "  (both create the same 'ss2026-net' Docker network — only start one)."
	@echo "    make run-infra          Start local Postgres/Kafka/Redis(+UI)/Prometheus/Grafana"
	@echo "    make down-infra         Stop the local infra stack"
	@echo "    make infra-logs         Tail local infra logs"
	@echo ""
	@echo "  Bidder"
	@echo "    make install           Download all Maven dependencies"
	@echo "    make build             Compile and package (skip tests)"
	@echo "    make run               Start local infra (if not already up), then build & run the"
	@echo "                           bidder container on port 8080 (schema bidder_teads_bidder)"
	@echo "    make run-team          Custom port/id:  make run-team PORT=8081 BIDDER_ID=team-alpha"
	@echo "    make run-docker        Alias for 'make run' minus the infra dependency"
	@echo "    make restart-bidder    Reset this bidder's budget in Postgres+Redis and restart the bidder container"
	@echo "    make down              Stop the bidder container"
	@echo "    make curl-examples     Print example curl requests to test your running bidder"
	@echo "    make test              Run unit tests"
	@echo "    make clean             Remove build artifacts"
	@echo ""
	@echo "  Bidder (prod mode — connects to the AWS backing services, see ../infra/terraform)"
	@echo "    make run-prod          Build & run the bidder container using config.prod.env"
	@echo "    make run-team-prod     Custom port/id in prod mode:  make run-team-prod PORT=8081 BIDDER_ID=team-alpha"
	@echo "    make down-prod         Stop the prod-mode bidder container"
	@echo "    make logs-prod         Tail the prod-mode bidder container logs"
	@echo ""

# ── Install / Build ──────────────────────────────────────────────────────────

install:
	$(MVN) dependency:resolve -q
	@echo "✓ Dependencies resolved"

build:
	$(MVN) clean package -DskipTests -q
	@echo "✓ Build complete"

# ── Infrastructure ────────────────────────────────────────────────────────────
# Local Postgres/Kafka/Redis(+UI)/Prometheus/Grafana, standalone — no ssp repo needed.
# Creates "ss2026-net", the same network name the ssp repo's docker-compose.all.yml
# creates, so only run one or the other at a time.

run-infra:
	docker compose -f docker-compose.infra.yml up -d
	@docker compose -f docker-compose.infra.yml ps
	@echo ""
	@echo "✓ Infra up — Kafka UI http://localhost:8090  Redis UI http://localhost:8091  Prometheus http://localhost:9090  Grafana http://localhost:3000"

down-infra:
	docker compose -f docker-compose.infra.yml down -v --remove-orphans

infra-logs:
	docker compose -f docker-compose.infra.yml logs -f

# ── Run ───────────────────────────────────────────────────────────────────────

# Runs the "bidder" service from docker-compose.yml, joining the "ss2026-net"
# network — either this repo's own run-infra, or the ssp repo's `make run`
# (docker-compose.all.yml), whichever you started. PORT/BIDDER_ID are exported
# by the `export` directive above, so docker compose picks them up automatically.
run: run-infra run-docker

run-team: run-infra
	docker compose up -d --build bidder
	@echo "✓ Bidder running in Docker on port $(PORT) (schema $(SCHEMA))"
	@docker compose logs -f bidder

run-docker:
	docker compose up -d --build bidder
	@echo "✓ Bidder running in Docker on port $${PORT:-8080}"
	@docker compose logs -f bidder

down:
	docker compose down

# Resets this bidder's remaining budget back to CREATIVE_BUDGET in Postgres, then restarts
# the bidder container. On boot, CreativeSeeder re-syncs Redis from the (now-reset) Postgres
# value, so both stores come back full. Requires the bidder to be running via `make run-docker`.
# Talks to the ss2026-postgres container directly (it's owned by the ssp repo's compose file,
# not this one).
restart-bidder:
	@echo "Resetting budget for bidder '$(BIDDER_ID)' (schema $(SCHEMA)) to $(CREATIVE_BUDGET)"
	docker exec -i ss2026-postgres psql -U bidder -d summerschool -c \
	  "UPDATE $(SCHEMA).creatives SET budget = $(CREATIVE_BUDGET) WHERE bidder_id = '$(BIDDER_ID)';"
	docker compose restart bidder
	@echo "✓ Bidder restarted — budget reset to $(CREATIVE_BUDGET) in Postgres and Redis"

# ── Prod mode (connects to AWS backing services instead of local containers) ──

run-prod:
	@test -f config.prod.env || (echo "Missing config.prod.env — copy config.prod.env.example and fill in VM_HOST/DB_PASSWORD" && exit 1)
	docker compose -f docker-compose.prod.yml up -d --build
	@echo "✓ Bidder running in prod mode on port $${PORT:-8080}"
	@docker compose -f docker-compose.prod.yml logs -f

# PORT/BIDDER_ID are exported by the `export` directive above, so docker compose
# picks them up automatically — same override pattern as run-team, against AWS.
run-team-prod:
	@test -f config.prod.env || (echo "Missing config.prod.env — copy config.prod.env.example and fill in VM_HOST/DB_PASSWORD" && exit 1)
	docker compose -f docker-compose.prod.yml up -d --build
	@echo "✓ Bidder running in prod mode on port $(PORT) (schema $(SCHEMA))"
	@docker compose -f docker-compose.prod.yml logs -f

down-prod:
	docker compose -f docker-compose.prod.yml down

logs-prod:
	docker compose -f docker-compose.prod.yml logs -f

# ── Try it ────────────────────────────────────────────────────────────────────
# Example curl requests against a running bidder (make run / make run-team first).
# PORT defaults to 8080 — override the same way you did for run-team, e.g.
# `make curl-examples PORT=8081`.

curl-examples:
	@echo ""
	@echo "  Health check"
	@echo "  ─────────────"
	@echo "  curl http://localhost:$(PORT)/health"
	@echo ""
	@echo "  Send a bid request (mirrors what the SSP sends)"
	@echo "  ───────────────────────────────────────────────"
	@echo "  curl -X POST http://localhost:$(PORT)/api/bid \\"
	@echo "    -H 'Content-Type: application/json' \\"
	@echo "    -d '{"
	@echo "      \"request_id\": \"test-1\","
	@echo "      \"floor_price\": 0.10,"
	@echo "      \"targeting\": {"
	@echo "        \"geo\": \"US\","
	@echo "        \"device_type\": \"mobile\","
	@echo "        \"audience_segment\": \"tech\""
	@echo "      }"
	@echo "    }'"
	@echo ""
	@echo "  → 200 with a bid body if you bid, or 204 No Content if you pass."
	@echo ""
	@echo "  Check remaining budget"
	@echo "  ───────────────────────"
	@echo "  curl http://localhost:$(PORT)/api/budget"
	@echo ""
	@echo "  Dashboard/API stats"
	@echo "  ────────────────────"
	@echo "  curl http://localhost:$(PORT)/api/stats"
	@echo "  curl http://localhost:$(PORT)/api/stats/creatives"
	@echo "  curl http://localhost:$(PORT)/api/stats/targeting"
	@echo "  curl http://localhost:$(PORT)/api/stats/timeseries"
	@echo ""
	@echo "  Prometheus metrics"
	@echo "  ───────────────────"
	@echo "  curl http://localhost:$(PORT)/actuator/prometheus"
	@echo ""

# ── Test ──────────────────────────────────────────────────────────────────────

test:
	$(MVN) test

# ── Clean ─────────────────────────────────────────────────────────────────────

clean:
	$(MVN) clean -q
