# Where Forge stops and Anvil.Site starts

Forge **ingests**. The Site **evaluates**. Nothing crosses that line.

## The rule

Forge may:

- decide *when* to poll a player
- fetch the hiscores and store the result
- detect that something *changed*
- record a plugin heartbeat

Forge may **not**:

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

### Forge → Site: the `forge_player_events` outbox

Forge appends; the Site consumes and marks consumed.

```sql
SELECT id, account_id, kind, payload
FROM forge_player_events
WHERE consumed_at IS NULL
ORDER BY id
LIMIT 500;
-- ... evaluate ...
UPDATE forge_player_events SET consumed_at = now() WHERE id = ANY($1);
```

| kind | payload | what the Site does with it |
|---|---|---|
| `snapshot.changed` | `{capturedAt, overallXp, deltas, snapshot}` | score stat tiles, roll up daily history, detect milestones, apply weekly values |
| `account.unranked` | `{rsn}` | flag for re-probe / rename review |
| `rsn.changed` | `{from, to}` | update display names, append rename history |

A durable outbox rather than `LISTEN/NOTIFY` on purpose: scoring bugs are replayable. Fix the rule,
reset `consumed_at` over a range, re-consume. With a fire-and-forget channel the evidence is gone.

### Site → Forge: writes to `accounts`

The Site never calls a Forge API — a scheduler that needs an HTTP round trip to learn someone
enrolled is a scheduler with an outage mode. Everything goes through shared columns.

| When | What | Store method |
|---|---|---|
| event start / end | promote every participant | `Promote` |
| event ending soon | promote only participants who have moved | `PromoteMoved` |
| plugin push | stamp `sweep_live_seen_at` | `RecordHeartbeat` |

Enrolment is the exception: Forge refreshes `sweep_enrolled` itself on a timer from live events and
competitions, so the Site does not have to remember to. It is denormalised because the claim query
runs constantly and must stay a single-table index scan — walking
`event_participants → clan_memberships → accounts` on every claim would put the domain schema on the
hot path.

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

That is why Forge adds so little schema. It is replacing the **cron**, not the **schema**: XP and
kill counts are Anvil data that Forge happens to fetch, so they go in the columns the Site already
reads.

| | |
|---|---|
| **Forge writes, Site owns** | `accounts.stats_next_due_at`, `stats_miss_streak`, `stats_overall_xp`, `stats_last_snapshot`, `status`, `live_stats` |
| **Forge adds to `accounts`** | `sweep_tier`, `sweep_error_streak`, `sweep_claimed_at`, `sweep_live_seen_at`, `sweep_enrolled` — the little the TS cron never had to persist because it recomputed each tick |
| **Forge-only tables** | `forge_player_events` (outbox), `forge_sweep_runs`, `forge_discord_deliveries`, `forge_plugin_payloads` / `_bindings` / `_public_payloads` / `_credentials` / `_ingest_events` |
| **Site owns outright** | `players` (people), `accounts`, `clan_memberships`, `clans`, events, tiles, teams, completions, `player_snapshots`, everything else |

Scheduling state lives as columns on `accounts` rather than in a parallel table. A `forge_sweep_state`
table was the obvious move and the wrong one: two rows per account, a join on the hottest query in
the service, and two places to look when someone is not being polled. These columns belong next to
`stats_next_due_at`, which is the column they modify.

The Site reads Forge's tables freely. It just does not **write** them, with the exceptions above.

**One known schema bug, and it is the Site's to fix.** `accounts.stats_overall_xp` is `integer`
(max 2,147,483,647), but a maxed OSRS account carries ~4.6 **billion** total XP. The write is
rejected client-side, and with a claim lease that account comes back minutes later to fail
identically — a poison pill burning requests forever, on precisely the accounts belonging to a
clan's best players. Forge degrades gracefully (keeps the snapshot, which holds the true figure,
and logs loudly), but the fix belongs in `schema.ts`:

```ts
statsOverallXp: bigint('stats_overall_xp', { mode: 'number' }),
```

Forge deliberately does not `ALTER` it: the column is drizzle-owned, and widening it here would
drift from `schema.ts` and be reverted by the next `drizzle-kit generate`.

Migrations for Forge-owned tables ship in this repo. Migrations for everything else ship in
Anvil.Site. Neither repo's migration tool should ever see the other's tables.

## The one shared contract that can silently break

`internal/hiscores/names_gen.go` maps hiscores display names to canonical keys, and it is generated
from the same `osrs-json-hiscores` release the Site pins. A tile tracking `zulrah` scores off a key
this table produces.

If the two drift, **nothing errors** — the tile just quietly scores zero forever. So:

- after bumping `osrs-json-hiscores` in Anvil.Site, run `node scripts/gen-hiscores-names.mjs` here
- `extraBosses` in `internal/hiscores/client.go` must mirror `EXTRA_HISCORE_BOSSES` in the Site's
  `src/lib/hiscores.ts`
- `TestParseMatchesSiteParser` diffs a real hiscores response against the TS parser's output; it is
  the thing that catches this, and it is why the golden file is committed
