-- Forge's schema additions.
--
-- There is ONE database. Forge is a second process against the same Postgres Anvil.Site uses, and
-- "ownership" here means only "the Go service is what writes this" — there is no wall.
--
-- Which is why this file is small. Forge is replacing the CRON, not the schema: XP, kill counts and
-- snapshots are Anvil data that Forge happens to fetch, so they stay in the columns the Site already
-- reads. `accounts` is the global OSRS account and already carries `stats_next_due_at` and
-- `stats_miss_streak`; all that was missing is the little the TS cron never had to persist because
-- it recomputed everything each tick.
--
-- Run with: psql "$DATABASE_URL" -f migrations/0001_init.sql

BEGIN;

-- ─────────────────────────────────────────────────────────────────────────────────────────────────
-- Scheduling state, as columns on the row it describes
-- ─────────────────────────────────────────────────────────────────────────────────────────────────
--
-- A parallel forge_sweep_state table was the obvious move and the wrong one: two rows per account,
-- a join on the hottest query in the service, and two places to look when a player is not being
-- polled. These belong next to stats_next_due_at, which is the column they modify.

ALTER TABLE accounts
  -- 0 hot / 1 warm / 2 cool / 3 idle / 4 dormant. Derived from stats_miss_streak, but stored so the
  -- distribution is queryable — "what fraction of enrolled accounts are dormant?" is the number that
  -- tells a clan its roster is mostly ghosts, and the number that predicts our polling bill.
  ADD COLUMN IF NOT EXISTS sweep_tier smallint NOT NULL DEFAULT 0,

  -- Transient failures back off separately from the miss ladder. An account whose fetch keeps
  -- timing out is not idle, it is unreachable; conflating the two parks an actively-playing person
  -- on the dormant tier because of OUR network.
  ADD COLUMN IF NOT EXISTS sweep_error_streak integer NOT NULL DEFAULT 0,

  -- Set while a worker holds the row. A crashed worker's accounts return to the queue when the
  -- lease elapses instead of being stuck forever.
  ADD COLUMN IF NOT EXISTS sweep_claimed_at timestamptz,

  -- Last plugin heartbeat. The strongest signal we ever get — it means they are logged in RIGHT
  -- NOW — so it overrides whatever backoff the ladder built up while they were away.
  ADD COLUMN IF NOT EXISTS sweep_live_seen_at timestamptz,

  -- IS THIS ACCOUNT WORTH POLLING AT ALL. Denormalised from event/weekly enrolment so the claim
  -- query stays a single-table index scan: joining eventParticipants -> clanMemberships -> accounts
  -- on every claim would put the domain schema on the hot path. Refreshed periodically by Forge.
  ADD COLUMN IF NOT EXISTS sweep_enrolled boolean NOT NULL DEFAULT false;

-- THE index. Partial on the two conditions every claim applies, so it covers only pollable rows:
-- at 1.2M accounts with a fraction enrolled, the rest cost nothing to carry.
CREATE INDEX IF NOT EXISTS accounts_sweep_due_idx
  ON accounts (stats_next_due_at)
  WHERE sweep_enrolled AND status = 'active';

-- For the dormancy reporting an admin surface shows clan staff.
CREATE INDEX IF NOT EXISTS accounts_sweep_tier_idx
  ON accounts (sweep_tier)
  WHERE sweep_enrolled;

-- ─────────────────────────────────────────────────────────────────────────────────────────────────
-- The outbox
-- ─────────────────────────────────────────────────────────────────────────────────────────────────

-- Forge INGESTS; it does not EVALUATE. When a snapshot changes, that fact lands here and Anvil.Site
-- decides what it means — whether a tile completed, a milestone was crossed, a weekly value moved,
-- what the EHP gain was.
--
-- Keeping the line here is what stops the tile-kind matrix (kill / timed / pvp / lap / gain /
-- deathless / diary / ca / clog / mission…) from having to exist in two languages. It also makes a
-- scoring bug replayable: the events are durable, so a fixed rule can re-consume a range instead of
-- the evidence being gone.
CREATE TABLE forge_player_events (
  id           bigserial PRIMARY KEY,
  account_id   integer NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,

  -- 'snapshot.changed' — new stats, payload carries the deltas
  -- 'account.unranked' — hiscores 404, payload carries the RSN tried
  -- 'rsn.changed'      — a rename was detected, payload carries from/to
  kind         text NOT NULL,

  payload      jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at   timestamptz NOT NULL DEFAULT now(),
  consumed_at  timestamptz
);

-- The consumer's cursor. Partial so the index shrinks as events are consumed rather than growing
-- forever alongside the table.
CREATE INDEX forge_player_events_unconsumed_idx ON forge_player_events (id) WHERE consumed_at IS NULL;

-- ─────────────────────────────────────────────────────────────────────────────────────────────────
-- Observability
-- ─────────────────────────────────────────────────────────────────────────────────────────────────

-- One row per sweep tick. The first thing to look at when the question is "why is the leaderboard
-- stale" — it distinguishes "we are rate limited", "we are behind", and "nobody is playing", which
-- otherwise look identical from outside.
CREATE TABLE forge_sweep_runs (
  id              bigserial PRIMARY KEY,
  started_at      timestamptz NOT NULL DEFAULT now(),
  finished_at     timestamptz,
  claimed         integer NOT NULL DEFAULT 0,
  fetched         integer NOT NULL DEFAULT 0,
  changed         integer NOT NULL DEFAULT 0,
  unranked        integer NOT NULL DEFAULT 0,
  errors          integer NOT NULL DEFAULT 0,
  -- Due but not claimed this tick. Sustained non-zero means the enrolled population has outgrown
  -- the request budget — the signal to widen the ladder, NOT to raise the rate.
  backlog         integer NOT NULL DEFAULT 0,
  shadow          boolean NOT NULL DEFAULT false
);

CREATE INDEX forge_sweep_runs_started_idx ON forge_sweep_runs (started_at DESC);

COMMIT;
