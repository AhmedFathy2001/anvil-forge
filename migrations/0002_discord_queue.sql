-- The Discord delivery queue.
--
-- Today the Site posts to Discord fire-and-forget from inside a request. That is fine for one clan
-- and wrong for twenty thousand: a webhook that 429s is simply lost, a webhook that was deleted is
-- retried forever inside request handlers, and a burst of tile completions at an event boundary
-- competes with the page render for the same event loop.
--
-- Moving it here makes delivery durable (a crash retries rather than drops), rate-limited per
-- webhook (Discord's limit is per webhook, not global, so one busy clan must not starve another),
-- and observable (a clan can be told their webhook is dead instead of silently getting nothing).

BEGIN;

CREATE TABLE discord_deliveries (
  id            bigserial PRIMARY KEY,

  -- Which webhook to post to. Stored rather than referenced because the Site owns webhook config
  -- and Forge must be able to deliver a queued message even if the row it came from is edited.
  webhook_url   text NOT NULL,

  -- Groups deliveries that share a rate limit. Discord limits per webhook, so this is normally a
  -- hash of the URL — kept separate so that if we later move to a bot token (where the limit is
  -- per channel) the bucket can change without rewriting the queue.
  bucket        text NOT NULL,

  payload       jsonb NOT NULL,

  -- Idempotency. The Site sets this to something stable and meaningful — "completion:8821",
  -- "event-start:412" — so a retry, a double-fire from two workers, or a replayed outbox event
  -- collapses into one Discord message instead of three.
  dedupe_key    text UNIQUE,

  -- 'pending' | 'delivering' | 'delivered' | 'failed' | 'dead'
  --
  -- 'dead' is distinct from 'failed' on purpose: 'failed' means we gave up after retries and it
  -- might work later, 'dead' means Discord told us this webhook no longer exists. Only 'dead'
  -- should surface to a clan as "your webhook is broken, here is how to fix it".
  status        text NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'delivering', 'delivered', 'failed', 'dead')),

  attempts      integer NOT NULL DEFAULT 0,
  next_attempt_at timestamptz NOT NULL DEFAULT now(),
  last_error    text,
  last_status   integer,               -- the HTTP status of the last attempt, for triage

  created_at    timestamptz NOT NULL DEFAULT now(),
  delivered_at  timestamptz,

  -- Higher goes first within the same due time. Event start/end announcements should not queue
  -- behind a backlog of routine drop notifications.
  priority      smallint NOT NULL DEFAULT 0
);

-- The claim index: partial on the only status a worker ever looks for, so it stays small even as
-- delivered rows accumulate ahead of the retention sweep.
CREATE INDEX discord_deliveries_claim_idx
  ON discord_deliveries (priority DESC, next_attempt_at)
  WHERE status IN ('pending', 'failed');

-- For the admin surface that tells a clan their webhook is dead.
CREATE INDEX discord_deliveries_dead_idx ON discord_deliveries (bucket, created_at DESC)
  WHERE status = 'dead';

-- Retention: delivered rows are only interesting for a short while.
CREATE INDEX discord_deliveries_delivered_idx ON discord_deliveries (delivered_at)
  WHERE status = 'delivered';

COMMIT;
