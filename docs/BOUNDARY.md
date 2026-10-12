# Where Forge stops and Anvil.Site starts

Forge **ingests**. The Site **evaluates**. Nothing crosses that line.

## The rule

Forge may:

- fetch the hiscores the Site queued, under one global rate limit, and store the raw result
- record a plugin heartbeat

Forge may **not**:

- decide *who* to poll or *when* — the Site's sweep queues that
- parse or interpret a hiscores body
- decide whether a change completes a tile
- decide whether a milestone was crossed
- decide what a weekly standing is
- post to Discord
- know what a tile, team, event, board, draft or clan even is

## Why the line is here

Two reasons, and the second is the one that actually matters.

**Performance.** Ingest has to be fast and must never block on domain logic. A plugin heartbeat or a
snapshot write should be a single indexed statement, not a transaction that also has to evaluate a
tile-kind matrix.

**Language cost.** The tile system spans kill, timed, pvp, lap, gain, deathless, diary, ca, clog,
value, lms and mission kinds, each with its own credit rules, plus the balance engine, the reveal
engine, frozen contribution splits and the Discord embed grammar. That is the accumulated result of
months of debugging against real events. Re-implementing it in Go would buy nothing a user could
see and would re-open every bug already closed. So it stays in TypeScript, in one place, and Forge
stays deliberately ignorant of it.

The corollary is that Forge is small — a few thousand lines — and that is the point. If it starts
growing a concept of "tiles", the boundary has moved and something has gone wrong.

## How they talk

### Site ⇄ Forge: the `hiscores_fetches` queue

The Site's sweep (`lib/statsSweep`, source `forge`) builds its due list and inserts one row per due
player; a partial unique index on `(rsn_key, mode) WHERE fetched_at IS NULL` makes re-queueing free.
Forge claims pending rows (`priority`, then `requested_at`; `FOR UPDATE SKIP LOCKED`, leased via
`claimed_at`), fetches, and writes `fetched_at`, `outcome` (`ok`/`unranked`/`transient`) and the raw
`body`. The Site's next drain marks rows consumed, parses each body with `lib/hiscores`, and applies
it through the exact code its in-app fetch uses. A read older than 30 minutes is dropped rather than
applied.

`mode` picks the table — `main`, `ironman`, `hardcore`, `ultimate` — so the account-type check is the
same queue with a different mode.

The earlier `forge_player_events` outbox (Forge scheduling and scoring-adjacent writes) is retired.

### Site → Forge: the plugin read path

Forge serves `/api/plugin/config` and `/api/plugin/board` as **opaque bytes**. The Site renders
them — all 1100 lines of `config/route.ts` worth of schedules, reveal state, notification config and
tracked-tile projections — and stores the result:

```sql
-- 1. Store the rendered payload, content-addressed by its ETag.
INSERT INTO forge_plugin_payloads (etag, body, encoding) VALUES ($1, $2, 'gzip')
ON CONFLICT (etag) DO NOTHING;

-- 2. Point the caller at it.
INSERT INTO forge_plugin_bindings (token_hash, kind, etag) VALUES ($1, 'config', $2)
ON CONFLICT (token_hash, kind) DO UPDATE SET etag = EXCLUDED.etag, updated_at = now();
```

Content-addressing is what makes this affordable: every member of the same team on the same event
gets a byte-identical config, so one payload row serves hundreds of bindings and a board edit
rewrites one row rather than four hundred.

Two rules the Site must hold to:

- **The payload must be deterministic for the same underlying data.** No per-request timestamps, no
  random key ordering. A payload that varies churns the ETag, and a churning ETag means every poll
  returns a full body — which is the entire cost the read path exists to avoid.
- **Store it gzipped.** `edge.Gzip` is the helper. Compression on a realistic config payload is
  ~1% of original, and pre-compressing means the hot path never spends CPU producing identical
  bytes.

### Forge → Site: the plugin ingest outbox

`forge_plugin_ingest_events` works exactly like `forge_player_events`: Forge appends, the Site consumes and
marks consumed. Forge validates the bearer token and stores the push; it does not decide whether a
kill completes a tile or a drop is worth announcing.

Note what Forge deliberately does **not** do on this path: no auto-linking, no auto-claiming, no
rename reconciliation. Those live in the Site's `resolvePluginMember` and are genuine domain
decisions — putting them at the edge would mean running them on every poll. The Site publishes the
answer into `forge_plugin_credentials`; Forge only looks it up.

### Site → Forge: the Discord queue

The Site enqueues instead of posting inline:

```sql
INSERT INTO forge_discord_deliveries (webhook_url, bucket, payload, dedupe_key, priority)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (dedupe_key) DO NOTHING;
```

`dedupe_key` should be something stable and meaningful — `completion:8821`, `event-start:412`. That
is what makes the Site's outbox consumer safe to replay: re-processing an event cannot double-post.

The `payload` is passed to Discord byte-identically. Forge does not know what an embed is and must
never reserialise one.

## One database

There is no second datastore. Forge is a second **process** against the same Postgres, and
"ownership" means only "this is what writes here".

For the sweep Forge writes exactly one Site table, and only the result columns of rows it claimed:

| | |
|---|---|
| **Forge writes (Site-owned, migration 0125)** | `hiscores_fetches.claimed_at`, `fetched_at`, `outcome`, `body`, `error`; `forge_sweep_runs` (its tick log) |
| **Forge-only tables** | `forge_discord_deliveries`, `forge_plugin_payloads` / `_bindings` / `_public_payloads` / `_credentials` / `_ingest_events` |
| **Retired** | `forge_player_events` and the `accounts.sweep_*` columns, from the design where Forge ran its own scheduler |
| **Site owns outright** | `players`, `accounts` (all stats columns), `clan_memberships`, `clans`, events, tiles, teams, completions, everything else |

Migrations for Forge-owned tables ship in this repo. Migrations for everything else ship in
Anvil.Site. Neither repo's migration tool should ever see the other's tables.

## No shared parser

Forge used to parse hiscores into snapshots with a Go port of `osrs-json-hiscores`, and a boss
missing from its generated name table silently scored zero. It now stores the endpoint's body
verbatim and the Site parses it, so a new boss is one line in the Site's `EXTRA_HISCORE_BOSSES` and
nothing here.
