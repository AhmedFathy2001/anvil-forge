package store

import (
	"context"
	"fmt"
)

// Reconciliation: deriving Forge's global player identity from Anvil.Site's per-clan membership.
//
// The Site's schema makes clans rows but keeps membership per clan — clan_members is unique on
// (clan_id, rsn_normalized), so one OSRS account in three clans is three rows. Polling those
// naively is three hiscores requests for one account, and at a 5 req/s budget that is the most
// expensive mistake available.
//
// So Forge collapses them. Every distinct account across every clan becomes one forge_players row,
// mapped to each membership it came from. One poll, fanned out.
//
// This runs on a schedule rather than on demand because it is cheap (a few statements over an
// indexed table) and because the alternative — the Site calling Forge whenever a roster changes —
// couples the two services on a path where a Forge outage would break roster sync.

// ReconcileResult reports what one pass did.
type ReconcileResult struct {
	PlayersInserted int64
	LinksInserted   int64
	LinksPruned     int64
	Enrolled        int64
	Unenrolled      int64
}

// Reconcile derives global players and their clan links from the Site's clan_members.
//
// Safe to run repeatedly; every statement is an upsert or a scoped delete.
func (s *Store) Reconcile(ctx context.Context) (ReconcileResult, error) {
	var r ReconcileResult
	if s.DryRun {
		return r, nil
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return r, fmt.Errorf("begin reconcile: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// 1. Every distinct account across every clan becomes one global player.
	//
	// DISTINCT ON picks one row per normalised name; ordering by account_hash NULLS LAST means a
	// membership that carries the strong Jagex identity wins over one that only has an RSN. That
	// matters because the hash is what survives a rename and the RSN is not.
	//
	// Placeholder RSNs are excluded here rather than filtered at poll time: RuneLite hands us
	// "#Player1404" for members whose name it cannot resolve, and those can never be looked up, so
	// admitting them would burn a request per tick to learn nothing.
	tag, err := tx.Exec(ctx, `
		INSERT INTO forge_players (rsn, rsn_normalized, account_hash)
		SELECT DISTINCT ON (cm.rsn_normalized)
		       cm.rsn, cm.rsn_normalized, cm.account_hash
		FROM clan_members cm
		WHERE cm.left_at IS NULL
		  AND cm.status = 'active'
		  AND cm.rsn ~ '^[A-Za-z0-9 _-]{1,12}$'
		ORDER BY cm.rsn_normalized, cm.account_hash NULLS LAST, cm.id
		ON CONFLICT (rsn_normalized) DO UPDATE
		SET rsn = EXCLUDED.rsn,
		    -- Never overwrite a hash we already hold with NULL: one clan knowing the account and
		    -- another not must not lose the stronger key.
		    account_hash = COALESCE(forge_players.account_hash, EXCLUDED.account_hash)`)
	if err != nil {
		return r, fmt.Errorf("upserting players: %w", err)
	}
	r.PlayersInserted = tag.RowsAffected()

	// 2. Map every live membership to its global player.
	tag, err = tx.Exec(ctx, `
		INSERT INTO forge_player_clans (player_id, clan_member_id, clan_id)
		SELECT p.id, cm.id, cm.clan_id
		FROM clan_members cm
		JOIN forge_players p ON p.rsn_normalized = cm.rsn_normalized
		WHERE cm.left_at IS NULL AND cm.status = 'active'
		ON CONFLICT (player_id, clan_member_id) DO UPDATE SET seen_at = now()`)
	if err != nil {
		return r, fmt.Errorf("linking memberships: %w", err)
	}
	r.LinksInserted = tag.RowsAffected()

	// 3. Drop links whose membership has left or been archived, so a leaver stops costing fan-out.
	//    The forge_players row survives: their history belongs to the account, not the membership.
	tag, err = tx.Exec(ctx, `
		DELETE FROM forge_player_clans pc
		WHERE NOT EXISTS (
		  SELECT 1 FROM clan_members cm
		  WHERE cm.id = pc.clan_member_id AND cm.left_at IS NULL AND cm.status = 'active'
		)`)
	if err != nil {
		return r, fmt.Errorf("pruning stale links: %w", err)
	}
	r.LinksPruned = tag.RowsAffected()

	// 4. Enrolment: is this player in anything live right now?
	//
	// Denormalised onto forge_sweep_state so the claim query stays a single-table index scan. A
	// player counts as enrolled if ANY of their clans has them in a running bingo or an active
	// weekly — the whole point of the global identity is that one poll serves all of them.
	tag, err = tx.Exec(ctx, `
		INSERT INTO forge_sweep_state (player_id, enrolled, next_poll_at)
		SELECT DISTINCT pc.player_id, true, now()
		FROM forge_player_clans pc
		WHERE EXISTS (
		        SELECT 1 FROM players pl
		        JOIN events e ON e.id = pl.event_id
		        WHERE pl.clan_member_id = pc.clan_member_id
		          AND e.force_ended_at IS NULL
		          AND (e.start_date IS NULL OR e.start_date <= to_char(now() AT TIME ZONE 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'))
		          AND (e.end_date   IS NULL OR e.end_date   >= to_char(now() AT TIME ZONE 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'))
		      )
		   OR EXISTS (
		        SELECT 1 FROM weekly_participants wp
		        JOIN weekly_competitions wc ON wc.id = wp.competition_id
		        WHERE wp.clan_member_id = pc.clan_member_id AND wc.status = 'active'
		      )
		ON CONFLICT (player_id) DO UPDATE
		SET enrolled = true,
		    -- Becoming enrolled makes them due immediately: they need a baseline before anything
		    -- they do can be scored, and that is the one poll that cannot be deferred.
		    next_poll_at = CASE WHEN forge_sweep_state.enrolled THEN forge_sweep_state.next_poll_at
		                        ELSE now() END`)
	if err != nil {
		return r, fmt.Errorf("marking enrolled: %w", err)
	}
	r.Enrolled = tag.RowsAffected()

	// 5. And un-enrol anyone whose events have all finished. They stay tracked; they just stop
	//    consuming budget, which is the difference between a platform that scales and one that does
	//    not.
	tag, err = tx.Exec(ctx, `
		UPDATE forge_sweep_state s
		SET enrolled = false
		WHERE s.enrolled
		  AND NOT EXISTS (
		    SELECT 1 FROM forge_player_clans pc
		    WHERE pc.player_id = s.player_id
		      AND (EXISTS (
		            SELECT 1 FROM players pl
		            JOIN events e ON e.id = pl.event_id
		            WHERE pl.clan_member_id = pc.clan_member_id
		              AND e.force_ended_at IS NULL
		              AND (e.start_date IS NULL OR e.start_date <= to_char(now() AT TIME ZONE 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'))
		              AND (e.end_date   IS NULL OR e.end_date   >= to_char(now() AT TIME ZONE 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'))
		          )
		       OR EXISTS (
		            SELECT 1 FROM weekly_participants wp
		            JOIN weekly_competitions wc ON wc.id = wp.competition_id
		            WHERE wp.clan_member_id = pc.clan_member_id AND wc.status = 'active'
		          ))
		  )`)
	if err != nil {
		return r, fmt.Errorf("un-enrolling finished players: %w", err)
	}
	r.Unenrolled = tag.RowsAffected()

	if err := tx.Commit(ctx); err != nil {
		return r, fmt.Errorf("commit reconcile: %w", err)
	}
	return r, nil
}

// Fanout reports how much cross-clan deduplication is actually buying: how many memberships exist
// versus how many distinct accounts they collapse to.
//
// Worth logging every pass. It is the number that says whether the global identity is earning its
// complexity, and it is the honest denominator for any claim about the polling budget.
func (s *Store) Fanout(ctx context.Context) (players int64, memberships int64, err error) {
	err = s.pool.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM forge_players),
		       (SELECT count(*) FROM forge_player_clans)`).Scan(&players, &memberships)
	if err != nil {
		return 0, 0, fmt.Errorf("reading fanout: %w", err)
	}
	return players, memberships, nil
}
