-- The plugin edge: the read path Forge serves, and the write path it accepts.
--
-- This is where the raw request volume is. At 20k clans with ~10 concurrent players each, config
-- and board polls are thousands of requests a second — and the overwhelming majority are "nothing
-- changed", which should cost a single indexed lookup and a 304, not a Next.js route handler
-- rebuilding a 30-field payload from fifteen queries.
--
-- FORGE DOES NOT BUILD PAYLOADS. /api/plugin/config is 1100 lines of domain logic in Anvil.Site —
-- schedules, reveal state, notification config, tracked-tile projections. Forge serves opaque
-- bytes that the Site rendered and stored. If Forge ever needs to know what a tile is to answer a
-- request, the boundary has moved and something has gone wrong.

BEGIN;

-- ─────────────────────────────────────────────────────────────────────────────────────────────────
-- Read path
-- ─────────────────────────────────────────────────────────────────────────────────────────────────

-- Rendered payloads, CONTENT-ADDRESSED by their ETag.
--
-- Deduplication is the point. Every member of the same team on the same event gets a byte-identical
-- config payload, so keying by content collapses what would be one row per member into one row per
-- distinct payload: order 100k rows instead of order 2M, and a board edit rewrites one row rather
-- than four hundred.
CREATE TABLE plugin_payloads (
  etag        text PRIMARY KEY,        -- the weak ETag, e.g. W/"base64sha1"
  body        bytea NOT NULL,
  -- Stored PRE-COMPRESSED. Compression is pure CPU we would otherwise pay on every 200, on the
  -- hottest path in the system, to produce identical bytes each time. Clients that do not accept
  -- gzip get it decompressed, which is the rare case.
  encoding    text NOT NULL DEFAULT 'gzip' CHECK (encoding IN ('gzip', 'identity')),
  built_at    timestamptz NOT NULL DEFAULT now()
);

-- Which payload a given caller should get. The Site writes these; Forge only reads them.
--
-- Keyed on a HASH of the bearer token, never the token itself: this table is the hottest read in
-- the service and will appear in slow-query logs, EXPLAIN output and backups. A plugin token is a
-- credential and none of those places should be able to hand one out.
CREATE TABLE plugin_bindings (
  token_hash  bytea NOT NULL,
  kind        text  NOT NULL CHECK (kind IN ('config', 'board')),
  etag        text  NOT NULL REFERENCES plugin_payloads(etag) ON DELETE CASCADE,
  updated_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (token_hash, kind)
);

-- Anonymous board previews (`/board?eventId=`) are not bound to a caller, so they get their own
-- tiny keyspace rather than a fabricated token.
CREATE TABLE plugin_public_payloads (
  scope_key   text PRIMARY KEY,        -- e.g. 'board:event:412'
  etag        text NOT NULL REFERENCES plugin_payloads(etag) ON DELETE CASCADE,
  updated_at  timestamptz NOT NULL DEFAULT now()
);

-- ─────────────────────────────────────────────────────────────────────────────────────────────────
-- Write path
-- ─────────────────────────────────────────────────────────────────────────────────────────────────

-- Who may push. The Site owns account linking, verification and auto-claim — all of which are
-- domain decisions with real subtlety (see resolvePluginMember in Anvil.Site) — and publishes the
-- resulting answer here as a flat, fast lookup.
CREATE TABLE plugin_credentials (
  token_hash    bytea PRIMARY KEY,
  player_id     bigint REFERENCES players(id) ON DELETE CASCADE,
  -- Opaque Site-side identity (its users.id). Forge never interprets it; it is carried through to
  -- the ingest event so the Site can attribute the push without a second lookup.
  subject       text,
  revoked_at    timestamptz,
  -- Cheap liveness, updated at most once a minute per token so a 30s poll does not turn into a
  -- write on every request.
  last_seen_at  timestamptz
);

CREATE INDEX plugin_credentials_player_idx ON plugin_credentials (player_id)
  WHERE revoked_at IS NULL;

-- Raw pushes from the game client. APPEND ONLY.
--
-- Forge accepts, validates the caller, and stores. It does NOT decide whether a kill completes a
-- tile, whether a drop is rare enough to announce, or which team gets the credit — that is the
-- tile-kind matrix, and it stays in one language in Anvil.Site. The Site consumes this table the
-- same way it consumes player_events.
--
-- Durable rather than fire-and-forget because scoring bugs are then replayable: fix the rule, reset
-- consumed_at over a range, re-consume. A push that was evaluated in-request and discarded is gone.
CREATE TABLE plugin_ingest_events (
  id           bigserial PRIMARY KEY,
  player_id    bigint REFERENCES players(id) ON DELETE SET NULL,
  subject      text,
  -- 'stats' | 'kill' | 'drop' | 'clog' | 'pb' | 'moment' | 'clip' | 'counter' | 'death' | ...
  -- Deliberately not a CHECK constraint: the plugin ships new event kinds ahead of the server that
  -- understands them, and rejecting an unknown kind at the edge would drop data we could otherwise
  -- have replayed once the Site learned to read it.
  kind         text NOT NULL,
  payload      jsonb NOT NULL,
  received_at  timestamptz NOT NULL DEFAULT now(),
  consumed_at  timestamptz,
  -- Client-supplied idempotency key. The plugin retries on a flaky connection, and a retried kill
  -- must not credit twice.
  dedupe_key   text
);

CREATE UNIQUE INDEX plugin_ingest_dedupe_idx ON plugin_ingest_events (dedupe_key)
  WHERE dedupe_key IS NOT NULL;

CREATE INDEX plugin_ingest_unconsumed_idx ON plugin_ingest_events (id)
  WHERE consumed_at IS NULL;

COMMIT;
