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
| 3. Plugin read path (`ETag`/304 config + board) | **working** — content-addressed payloads, pre-gzipped, 304 on match |
| 4. Plugin ingest writes | **working** — token auth, batched pushes, idempotent, heartbeats |
| 5. Discord fan-out queue | **working** — durable, per-webhook rate limits, 429-aware, dead-webhook detection |
| 6. Discord gateway | **deliberately not built** — see below |

### Why step 6 is not here

The gateway needs a bot token to be worth anything, and a hand-rolled WebSocket client covering
sharding, resume, session invalidation and identify rate limits is a large amount of code that
cannot be tested without one. Shipping that untested would be worse than not shipping it.

When the shared bot is real and past Discord's verification gate (required above 100 guilds), the
work is: add `bwmarrin/discordgo`, connect with the right shard count, and write received events
into an inbox table the Site consumes — the same pattern as `player_events` and
`plugin_ingest_events`. The *outbound* direction, which is what most features actually need, is
already covered by the delivery queue in step 5.

### How this fits the Site's schema

The multi-clan rework (`feat/drop-federation`) landed global identity, so Forge no longer has to
invent any:

- **`players`** is a *person*. **`accounts`** is a global OSRS account — `rsn_normalized` and
  `account_hash` both UNIQUE, hiscores state on the row, and `accounts_due_idx` already built.
- **`clan_memberships`** is the seat: `(clan_id, account_id)`. One account in three clans is three
  seats and **one** poll, which was the whole point.

So Forge's earlier `forge_players` + `Reconcile` — which existed only to derive global identity from
per-clan membership — are deleted. Keeping a shadow identity table alongside `accounts` would have
been two sources of truth for "who is this account".

Forge writes `accounts.stats_*` exactly as `/api/cron/stats` does, so every existing read path keeps
working and rollback is turning Forge off.

## Running it locally

```bash
docker compose up -d                                    # Postgres on :55432
export DATABASE_URL="postgres://forge:forge@localhost:55432/forge"
for f in migrations/*.sql; do psql "$DATABASE_URL" -f "$f"; done
psql "$DATABASE_URL" -f scripts/seed-dev.sql            # real accounts, incl. a deliberate 404
go run ./cmd/forge
```

Each subsystem can be run alone — useful because they scale differently. The edge is stateless and
scales horizontally; the sweep must stay a single instance, because its rate budget is global and
cannot be divided across replicas without coordination.

```bash
FORGE_ENABLE_SWEEP=false FORGE_ENABLE_DISCORD=false go run ./cmd/forge   # edge only
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
| `FORGE_HTTP_ADDR` | `:8080` | `/health` (liveness), `/ready` (DB + backlog), plugin edge |
| `FORGE_LOG_LEVEL` | `info` | |
| `FORGE_ENABLE_SWEEP` / `_EDGE` / `_DISCORD` | `true` | run subsystems independently |
| `FORGE_DISCORD_WORKERS` | `16` | |
| `FORGE_DISCORD_CLAIM_BATCH` | `100` | |
| `FORGE_DISCORD_KEEP_DELIVERED` | `72h` | retention for delivered rows |

## HTTP surface

| Route | Notes |
|---|---|
| `GET /health` | liveness; never touches the database |
| `GET /ready` | readiness; pings Postgres and reports sweep backlog |
| `GET /api/plugin/config` | bearer auth, `ETag`/304, pre-gzipped |
| `GET /api/plugin/board` | same, plus anonymous `?eventId=` preview |
| `POST /api/plugin/ingest` | single or batched pushes, idempotent via `dedupeKey`, returns **202** |
| `POST /api/plugin/heartbeat` | stamps `live_seen_at`; the strongest signal the sweep gets |

`202` on ingest rather than `200` is deliberate: the events are durably stored but nothing has been
*scored* yet. The Site decides what any of it means, so promising more would be a lie the plugin
might act on.

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

## Deploying

CI mirrors Anvil.Admin's shape, not the Site's: Forge is a **singleton**, so there is no per-clan
rollout. Build → GHCR → SSH → `docker compose up -d anvil-forge` → health gate.

The health gate checks that `/health` reports the SHA CI just built, not merely that something
answered — the question after a deploy is "is the new one running", and a green check on the old
binary is worse than a red one.

```
deploy/compose-snippet.yml   # add to /opt/anvil/docker-compose.yml
deploy/forge.env.example     # copy to /opt/anvil/forge.env
```

Repo secrets needed: `SSH_HOST`, `SSH_USER`, `SSH_KEY`, optionally `SSH_PORT`. Until `SSH_HOST` is
set the deploy step is skipped and the build still publishes the image.

The image is distroless (12 MB, no shell), so the container healthcheck runs the binary's own
`-healthcheck` probe rather than shelling out to curl.
