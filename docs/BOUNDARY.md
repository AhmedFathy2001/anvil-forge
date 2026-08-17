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

### Forge → Site: the `player_events` outbox

Forge appends; the Site consumes and marks consumed.

```sql
SELECT id, player_id, kind, payload
FROM player_events
WHERE consumed_at IS NULL
ORDER BY id
LIMIT 500;
-- ... evaluate ...
UPDATE player_events SET consumed_at = now() WHERE id = ANY($1);
```

| kind | payload | what the Site does with it |
|---|---|---|
| `snapshot.changed` | `{capturedAt, overallXp, deltas}` | score stat tiles, roll up daily history, detect milestones, apply weekly values |
| `player.unranked` | `{rsn}` | flag for re-probe / rename review |
| `rsn.changed` | `{from, to}` | update display names, append rename history |

A durable outbox rather than `LISTEN/NOTIFY` on purpose: scoring bugs are replayable. Fix the rule,
reset `consumed_at` over a range, re-consume. With a fire-and-forget channel the evidence is gone.

### Site → Forge: three writes to `sweep_state`

The Site never calls a Forge API. It writes three things to shared tables, because a scheduler that
needs an HTTP round trip to learn someone enrolled is a scheduler with an outage mode.

| When | What | Store method |
|---|---|---|
| enrolment changes | `enrolled` flag (+ due immediately when turning on) | `SetEnrolled` |
| event start / end | promote every participant | `Promote` |
| event ending soon | promote only participants who have moved | `PromoteMoved` |
| plugin push | stamp `live_seen_at` | `RecordHeartbeat` |

`enrolled` is denormalised onto `sweep_state` deliberately. The claim query runs constantly and must
stay a single-table index scan; joining into the Site's domain tables to ask "is this player in a
live event" would put the Site's schema on Forge's hot path and couple the two exactly where they
should be independent.

## Table ownership

| Owner | Tables |
|---|---|
| **Forge** | `players`, `player_current`, `player_snapshots`, `sweep_state`, `player_events`, `sweep_runs` |
| **Site** | clans, memberships, events, tiles, teams, completions, competitions, baselines, everything else |

The Site reads Forge's tables freely — leaderboards join `player_current`, competition standings
join baselines against it. It just does not **write** them, with the four exceptions above.

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
