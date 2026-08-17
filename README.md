# Anvil.Forge

Anvil's data plane. Today it runs the OSRS hiscores sweep; plugin ingest, the Discord fan-out queue
and the gateway follow.

It exists because those four workloads scale with the *playerbase* rather than with how many clans
signed up, and they want a long-lived process with a worker pool — which is exactly what Next.js
route handlers are not. The domain logic (tiles, balance, reveal, draft, recaps, embeds) stays in
Anvil.Site, in TypeScript, where it already works.

**Forge ingests. The Site evaluates.** See [docs/BOUNDARY.md](docs/BOUNDARY.md).

## Status

| Step | State |
|---|---|
| 2. Hiscores sweep | **working** — scheduler, ladder, client, persistence, outbox |
| 3. Plugin read path (`ETag`/304 board + config) | not started |
| 4. Plugin ingest writes | not started |
| 5. Discord fan-out queue | not started |
| 6. Discord gateway | not started |

## Running it locally

```bash
docker compose up -d                                    # Postgres on :55432
export DATABASE_URL="postgres://forge:forge@localhost:55432/forge"
psql "$DATABASE_URL" -f migrations/0001_init.sql
psql "$DATABASE_URL" -f scripts/seed-dev.sql            # real accounts, incl. a deliberate 404
go run ./cmd/forge
```

`FORGE_DRY_RUN=true` polls for real and writes nothing but the run log — the way to observe
behaviour and request rates against production data before letting it touch a row.

```bash
go test ./...                       # includes the differential test against the TS parser
go test ./internal/sweep -v         # prints the budget arithmetic
```

## Configuration

| Variable | Default | Notes |
|---|---|---|
| `DATABASE_URL` | — | required |
| `FORGE_HISCORES_RPS` | `5` | **global** rate to Jagex. Read [docs/RATE_BUDGET.md](docs/RATE_BUDGET.md) before raising. |
| `FORGE_HISCORES_BURST` | `10` | |
| `FORGE_WORKERS` | `8` | in-flight fetches; the limiter governs regardless |
| `FORGE_CLAIM_BATCH` | `200` | players leased per claim |
| `FORGE_CLAIM_LEASE` | `5m` | a crashed worker costs its players this much delay |
| `FORGE_TICK_INTERVAL` | `10s` | how often to look for due players — *not* the polling cadence |
| `FORGE_DRY_RUN` | `false` | |
| `FORGE_HTTP_ADDR` | `:8080` | `/health` (liveness), `/ready` (DB + backlog) |
| `FORGE_LOG_LEVEL` | `info` | |

## The thing to understand before changing the scheduler

Wise Old Man tracks a large fraction of the entire OSRS playerbase at **four hiscores requests per
second**, globally, with no periodic all-player sweep at all. At 20k clans our enrolled population
is ~1.2M accounts, which at 5 req/s is **one poll per account every two days**.

So the sweep is not a way to keep everyone fresh — it cannot be, at any polite request rate. It is
a way to **allocate a scarce budget by priority**. What actually carries the load:

1. **The plugin** — push costs zero hiscores requests. Every point of adoption buys budget for
   players who do not have it. This is the core scaling lever, not a product nice-to-have.
2. **Boundaries** — a mandatory poll at event start (the frozen baseline) and at event end (closing
   the books exactly). Twice per event, so affordable.
3. **On demand** — a refresh button with a 60s per-player floor.
4. **The ladder** — spends whatever remains on whoever is most likely to have moved.

Backlog in `sweep_runs` is the expected steady state, not a fault. Because baselines are frozen at
enrolment and gains are `current − baseline`, slow polling costs *detection latency*, never
*correctness* — which is the property that makes allocating under scarcity acceptable at all.

## Layout

```
cmd/forge/            entrypoint, health server
internal/hiscores/    client, parser, RSN rules, generated name tables
internal/sweep/       the ladder (pure, tested) and the runner
internal/store/       pgx queries; boundary.go is the Site-facing surface
internal/config/      environment
migrations/           Forge-owned tables only
scripts/              name-table generator, dev seed
```

## Licence

Same as the rest of Anvil: PolyForm Noncommercial + Attribution. Source-available, not open source.
