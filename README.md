# Anvil.Forge

Anvil's data plane. Today it fetches the OSRS hiscores for the Site's sweep; plugin ingest, the
Discord fan-out queue and the gateway follow.

It exists because those four workloads scale with the *playerbase* rather than with how many clans
signed up, and they want a long-lived process with a worker pool — which is exactly what Next.js
route handlers are not. The domain logic (tiles, balance, reveal, draft, recaps, embeds) stays in
Anvil.Site, in TypeScript, where it already works.

**Forge ingests. The Site evaluates.** See [docs/BOUNDARY.md](docs/BOUNDARY.md).

## Status

| Step | State |
|---|---|
| 2. Hiscores fetching | **working** — drains the Site's `hiscores_fetches` queue under one global rate limit |
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

### How the sweep is split (since 2026-10)

Anvil.Site runs the sweep: who is due, what a read means for tiles, weekly rows, history and
baselines. With `STATS_SWEEP_OWNER=forge` it stops calling Jagex itself and queues a row per due
player in `hiscores_fetches` (Site migration `0125`). Forge claims those rows, fetches them under
**one** global rate limit, and writes back the outcome and the endpoint's **raw JSON body**. The
Site's next drain parses that body with its own parser and applies it with the same code its
in-app fetch uses.

So Forge holds no copy of the sweep's rules and no parser to keep in step. An earlier design had
Forge run its own scheduler and write `accounts.stats_*` directly; it drifted from the Site within
weeks and was retired. Forge only fetches what it was asked for, so it cannot add hiscores traffic
the Site did not choose.

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
| `FORGE_HISCORES_RPS` | `1` | **global** rate to Jagex. Read [docs/RATE_BUDGET.md](docs/RATE_BUDGET.md) before raising. |
| `FORGE_HISCORES_BURST` | `2` | |
| `FORGE_WORKERS` | `4` | in-flight fetches; the limiter governs regardless |
| `FORGE_CLAIM_BATCH` | `20` | queue rows leased per claim |
| `FORGE_CLAIM_LEASE` | `5m` | a crashed worker's rows return to the queue after this |
| `FORGE_TICK_INTERVAL` | `5s` | how often an idle runner re-checks the queue |
| `FORGE_DRY_RUN` | `false` | fetches for real but writes no result back — not free |
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
| `GET /ready` | readiness; pings Postgres and reports the fetch-queue backlog |
| `GET /api/plugin/config` | bearer auth, `ETag`/304, pre-gzipped |
| `GET /api/plugin/board` | same, plus anonymous `?eventId=` preview |
| `POST /api/plugin/ingest` | single or batched pushes, idempotent via `dedupeKey`, returns **202** |
| `POST /api/plugin/heartbeat` | stamps `live_seen_at`; the strongest signal the sweep gets |

`202` on ingest rather than `200` is deliberate: the events are durably stored but nothing has been
*scored* yet. The Site decides what any of it means, so promising more would be a lie the plugin
might act on.

## The thing to understand before changing the rate

A large community hiscores tracker covers a big fraction of the entire OSRS playerbase at **four
requests per second**, globally, with no periodic all-player sweep at all. Polling is not a way to
keep everyone fresh; it is a scarce budget allocated by priority, and the Site does the allocating
(plugin-first scheduling, the backoff ladder, competition work ahead of roster filler — queue
`priority` 0/1/2). The plugin carries the load: a push costs zero hiscores requests.

A growing `backlog` on `/ready` means the Site is asking for more than the rate can fetch. Widen the
Site's ladder before raising `FORGE_HISCORES_RPS`.

## Layout

```
cmd/forge/            entrypoint, health server
internal/hiscores/    client (raw body per hiscores table), RSN rules
internal/sweep/       the queue runner
internal/store/       pgx queries: claim, complete, run log
internal/config/      environment
migrations/           Forge-owned tables only
scripts/              dev seed
```

## Licence

Same as the rest of Anvil: PolyForm Noncommercial + Attribution. Source-available, not open source.

## Deploying

**Beta only.** A push to the `beta` branch builds the image and deploys it into the beta stack
(`/opt/anvil/beta`, service `anvil-forge-beta`, image pinned via `FORGE_IMAGE` in that directory's
`.env`). A push to `main` or a `v*` tag builds and publishes the image and stops: production runs no
Forge until a deliberate, manual rollout.

The health gate checks that `/health` reports the SHA CI just built, not merely that something
answered — the question after a deploy is "is the new one running", and a green check on the old
binary is worse than a red one.

Repo secrets needed: `SSH_HOST`, `SSH_USER`, `SSH_KEY`, optionally `SSH_PORT`. Until `SSH_HOST` is
set the deploy step is skipped and the build still publishes the image.

The image is distroless (12 MB, no shell), so the container healthcheck runs the binary's own
`-healthcheck` probe rather than shelling out to curl.
