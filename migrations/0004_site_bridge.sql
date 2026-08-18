-- The bridge between Forge's global player identity and Anvil.Site's per-clan membership.
--
-- WHY THIS EXISTS. The multi-clan schema makes CLANS rows, but membership is still per clan:
-- clan_members is unique on (clan_id, rsn_normalized), so one OSRS account in three clans is three
-- rows, each carrying its own stats_next_due_at / stats_last_snapshot / live_stats. Polled naively
-- that is three hiscores requests for one account — and cross-clan deduplication is the single
-- biggest win available on the polling budget.
--
-- Rather than wait for the Site to adopt global players, Forge derives the global identity ITSELF
-- and keeps this mapping. One poll per account, fanned out to every clan that cares. When the Site
-- eventually does unify identity, this table is what the migration reads.

BEGIN;

CREATE TABLE forge_player_clans (
  player_id       bigint  NOT NULL REFERENCES forge_players(id) ON DELETE CASCADE,

  -- Anvil.Site's clan_members.id and clans.id.
  --
  -- DELIBERATELY NO FOREIGN KEY. Forge's migrations must run against a database where the Site's
  -- tables do not exist yet (a fresh dev box, a Forge-only deployment), and a cross-service FK turns
  -- deploy ordering into a dependency graph. The reconciler prunes rows whose member has gone, which
  -- is the same guarantee at a cost we control.
  clan_member_id  integer NOT NULL,
  clan_id         integer NOT NULL,

  linked_at       timestamptz NOT NULL DEFAULT now(),
  -- Last time the reconciler saw this membership in clan_members. Rows that stop being seen are
  -- pruned, which is how a member leaving a clan stops costing us fan-out.
  seen_at         timestamptz NOT NULL DEFAULT now(),

  PRIMARY KEY (player_id, clan_member_id)
);

-- One membership belongs to exactly one global player. This is the constraint that catches an
-- identity fork: if a reconciler bug ever tried to attach the same clan_members row to two
-- forge_players, it fails loudly here rather than silently splitting someone's history.
CREATE UNIQUE INDEX forge_player_clans_member_idx ON forge_player_clans (clan_member_id);
CREATE INDEX forge_player_clans_clan_idx ON forge_player_clans (clan_id);

-- How many clans each player is in — the dedup factor, and the number that says how much this
-- table is saving. A view rather than a column so it cannot drift.
CREATE VIEW forge_player_fanout AS
  SELECT p.id AS player_id,
         p.rsn,
         count(pc.clan_member_id) AS clan_count
  FROM forge_players p
  LEFT JOIN forge_player_clans pc ON pc.player_id = p.id
  GROUP BY p.id, p.rsn;

COMMIT;
