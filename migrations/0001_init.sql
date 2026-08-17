-- Forge owns player identity and hiscores tracking. Anvil.Site owns clans, events, tiles, teams,
-- scoring — and references players(id).
--
-- The split exists because these tables have a completely different access pattern from the domain
-- ones: a few hundred writes a second from one process, no human ever editing them, and a row count
-- that scales with the OSRS playerbase rather than with how many clans signed up.
--
-- THE INVERSION THIS ENCODES. Snapshots used to be competition-scoped: two rows per (member,
-- competition), which meant a player guesting in three clans' events generated three polls and six
-- rows of the same numbers. Here the snapshot stream is GLOBAL and per-player, and a competition
-- stores only a baseline pointer. Three events, one poll, one snapshot, three cheap baseline rows.
--
-- Run with: psql "$DATABASE_URL" -f migrations/0001_init.sql

BEGIN;

-- ─────────────────────────────────────────────────────────────────────────────────────────────────
-- Identity
-- ─────────────────────────────────────────────────────────────────────────────────────────────────

-- One row per OSRS account, for the whole platform. NOT per clan: a player who guests in four
-- clans is one row here and four membership rows over in the Site's schema.
CREATE TABLE players (
  id              bigserial PRIMARY KEY,

  -- The strong identity key, captured during the plugin handshake. Survives renames, which RSN
  -- does not. Null until we have seen the player through the plugin at least once — which for
  -- roughly half the population is never, hence the nullable column and the RSN fallback.
  account_hash    text UNIQUE,

  rsn             text NOT NULL,          -- current display casing
  -- Lowercased, with space/underscore collapsed. OSRS treats "GIM_nisbro" and "GIM Nisbro" as the
  -- same account; without this they become two players and one person's history splits in half.
  rsn_normalized  text NOT NULL UNIQUE,

  -- Rename history, appended whenever a change is detected. Also how a lookup recovers a player
  -- who 404s under their new name but is already known under the old one.
  previous_rsns   jsonb NOT NULL DEFAULT '[]'::jsonb,

  first_seen_at   timestamptz NOT NULL DEFAULT now(),
  last_seen_at    timestamptz,

  -- 'active'   — polled normally
  -- 'unranked' — hiscores 404s (renamed, banned, or genuinely not on the hiscores yet). Excluded
  --              from the sweep until a re-probe lifts them, because otherwise one renamed account
  --              burns a request every tick forever.
  -- 'invalid'  — the name can never be looked up (RuneLite placeholders like "#Player1404").
  status          text NOT NULL DEFAULT 'active'
                  CHECK (status IN ('active', 'unranked', 'invalid')),
  status_checked_at timestamptz
);

COMMENT ON TABLE players IS
  'Global OSRS account identity. One row per account across every clan on the platform.';

-- ─────────────────────────────────────────────────────────────────────────────────────────────────
-- Tracking state
-- ─────────────────────────────────────────────────────────────────────────────────────────────────

-- Current stats, updated IN PLACE. One row per player, forever — this table never grows with time,
-- only with the playerbase. Every leaderboard read hits this and never touches the history table.
CREATE TABLE player_current (
  player_id     bigint PRIMARY KEY REFERENCES players(id) ON DELETE CASCADE,
  snapshot      jsonb  NOT NULL,          -- the full hiscores read, same shape as Anvil.Site's
  overall_xp    bigint NOT NULL,          -- promoted out of the blob: it is the change detector
  captured_at   timestamptz NOT NULL
);

-- Append-only history, written ONLY when something actually changed.
--
-- The write-on-change rule is what makes this affordable. Most polls return numbers identical to
-- the last one — an unconditional insert would mean ~2M rows per sweep of pure duplication, which
-- is exactly the bloat that put 1.2 GB of redundant snapshots in the old competition-scoped table.
-- Writing only on change reduces daily volume from "every tracked player" to "every player who
-- actually played".
--
-- Partitioned by month so retention is a DROP TABLE rather than a DELETE that has to be vacuumed.
CREATE TABLE player_snapshots (
  id            bigserial,
  player_id     bigint NOT NULL REFERENCES players(id) ON DELETE CASCADE,
  captured_at   timestamptz NOT NULL,
  overall_xp    bigint NOT NULL,
  snapshot      jsonb  NOT NULL,
  PRIMARY KEY (id, captured_at)
) PARTITION BY RANGE (captured_at);

CREATE INDEX player_snapshots_player_time_idx ON player_snapshots (player_id, captured_at DESC);

-- Creates the partition covering a given month, if it does not exist. Call from a monthly job and
-- once at boot — an insert into a range with no partition is a hard error, so this must run AHEAD
-- of the month it covers, not during it.
CREATE OR REPLACE FUNCTION ensure_snapshot_partition(for_month date)
RETURNS void AS $$
DECLARE
  start_at date := date_trunc('month', for_month)::date;
  end_at   date := (date_trunc('month', for_month) + interval '1 month')::date;
  part     text := 'player_snapshots_' || to_char(start_at, 'YYYY_MM');
BEGIN
  IF to_regclass(part) IS NULL THEN
    EXECUTE format(
      'CREATE TABLE %I PARTITION OF player_snapshots FOR VALUES FROM (%L) TO (%L)',
      part, start_at, end_at
    );
  END IF;
END;
$$ LANGUAGE plpgsql;

-- This month and the next two, so a boot never lands in an uncovered range.
SELECT ensure_snapshot_partition(CURRENT_DATE);
SELECT ensure_snapshot_partition((CURRENT_DATE + interval '1 month')::date);
SELECT ensure_snapshot_partition((CURRENT_DATE + interval '2 months')::date);

-- ─────────────────────────────────────────────────────────────────────────────────────────────────
-- The scheduler
-- ─────────────────────────────────────────────────────────────────────────────────────────────────

-- One row per player, holding everything the sweep needs to decide WHEN to poll them next.
--
-- Deliberately denormalised: the claim query runs many times a second and must be a single-table
-- index scan with no joins into the Site's domain tables. `enrolled` and `live_seen_at` are pushed
-- here by the Site rather than derived at claim time.
CREATE TABLE sweep_state (
  player_id       bigint PRIMARY KEY REFERENCES players(id) ON DELETE CASCADE,

  -- The queue position. Everything in this file exists to compute this one column well.
  next_poll_at    timestamptz NOT NULL DEFAULT now(),

  -- 0 hot / 1 warm / 2 cool / 3 idle / 4 dormant. Derived from miss_streak; stored so the
  -- distribution is queryable ("how many players are dormant?") without recomputing the ladder.
  tier            smallint NOT NULL DEFAULT 0,

  -- Consecutive polls that found nothing new. Any change resets it to 0.
  miss_streak     integer NOT NULL DEFAULT 0,

  last_polled_at  timestamptz,
  last_change_at  timestamptz,
  last_outcome    text,                  -- 'ok' | 'unranked' | 'transient'

  -- Transient failures back off separately from the miss ladder: a player whose fetch keeps timing
  -- out is not idle, they are unreachable, and conflating the two would park an active player on
  -- the dormant tier because of our own network.
  error_streak    integer NOT NULL DEFAULT 0,

  -- IS THIS PLAYER WORTH POLLING AT ALL. False for anyone not enrolled in a live competition —
  -- they are still tracked, just not swept. Maintained by the Site on enrolment change.
  enrolled        boolean NOT NULL DEFAULT false,

  -- Last plugin heartbeat. The strongest signal we ever get: it means they are logged in RIGHT
  -- NOW, so it overrides any backoff the ladder had built up while they were away.
  live_seen_at    timestamptz,

  -- Set while a worker holds the row, so a crashed worker's players return to the queue when the
  -- lease expires rather than being stuck forever.
  claimed_at      timestamptz
);

-- THE index. Partial on `enrolled` so it covers only pollable rows: at 2M players with 1.2M
-- enrolled, that is 40% less index to walk on every claim, and the non-enrolled majority costs
-- nothing to carry.
CREATE INDEX sweep_state_due_idx ON sweep_state (next_poll_at) WHERE enrolled;

-- For the dormancy reporting the admin UI shows clan staff ("your roster is 40% ghosts").
CREATE INDEX sweep_state_tier_idx ON sweep_state (tier) WHERE enrolled;

-- ─────────────────────────────────────────────────────────────────────────────────────────────────
-- The outbox
-- ─────────────────────────────────────────────────────────────────────────────────────────────────

-- Forge INGESTS; it does not EVALUATE. When a snapshot changes, that fact lands here and the Site
-- decides what it means — whether a tile completed, a milestone was crossed, a weekly value moved.
--
-- Keeping the boundary here is what stops the tile-kind matrix (kill / timed / pvp / lap / gain /
-- deathless / diary / ca / clog / mission…) from having to exist in two languages. It also means a
-- scoring bug is replayable: the events are durable, so the Site can re-consume a range after a fix
-- instead of the evidence being gone.
CREATE TABLE player_events (
  id           bigserial PRIMARY KEY,
  player_id    bigint NOT NULL REFERENCES players(id) ON DELETE CASCADE,

  -- 'snapshot.changed' — new stats, payload carries the deltas
  -- 'player.unranked'  — hiscores 404, payload carries the RSN tried
  -- 'rsn.changed'      — a rename was detected, payload carries from/to
  kind         text NOT NULL,

  payload      jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at   timestamptz NOT NULL DEFAULT now(),
  consumed_at  timestamptz
);

-- The consumer's cursor. Partial so the index shrinks as events are consumed rather than growing
-- forever alongside the table.
CREATE INDEX player_events_unconsumed_idx ON player_events (id) WHERE consumed_at IS NULL;

-- ─────────────────────────────────────────────────────────────────────────────────────────────────
-- Observability
-- ─────────────────────────────────────────────────────────────────────────────────────────────────

-- One row per sweep tick. Small, bounded by a retention job, and the first thing to look at when
-- the question is "why is the leaderboard stale" — it distinguishes "we are rate limited", "we are
-- behind", and "nobody is playing", which otherwise look identical from the outside.
CREATE TABLE sweep_runs (
  id              bigserial PRIMARY KEY,
  started_at      timestamptz NOT NULL DEFAULT now(),
  finished_at     timestamptz,
  claimed         integer NOT NULL DEFAULT 0,
  fetched         integer NOT NULL DEFAULT 0,
  changed         integer NOT NULL DEFAULT 0,
  unranked        integer NOT NULL DEFAULT 0,
  errors          integer NOT NULL DEFAULT 0,
  -- How many were due but not claimed this tick. Sustained non-zero means the polling budget is
  -- smaller than the enrolled population, which is the signal to widen the ladder rather than to
  -- raise the request rate.
  backlog         integer NOT NULL DEFAULT 0,
  shadow          boolean NOT NULL DEFAULT false
);

CREATE INDEX sweep_runs_started_idx ON sweep_runs (started_at DESC);

COMMIT;
