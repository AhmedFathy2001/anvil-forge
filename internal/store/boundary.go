package store

import (
	"context"
	"fmt"
	"time"
)

// Boundary sweeps: the polls that are never negotiable.
//
// Continuous polling cannot cover the enrolled population at any polite request rate (see
// docs/RATE_BUDGET.md), so correctness is anchored at the two moments where an exact number is
// actually required, and the ladder does best-effort in between:
//
//	EVENT START — every participant polled once. This is the frozen baseline that every gain in the
//	              event is measured from, so it is the one poll that must not be skipped or deferred.
//
//	EVENT END   — every participant polled once, closing the books exactly. Without it the final
//	              standings are "whatever the ladder happened to have collected", which is precisely
//	              the moment nobody will accept an approximation.
//
// Both are twice per event rather than continuous, which is why they are affordable at all.

// PromoteReason names why a player jumped the queue, for the run log.
type PromoteReason string

const (
	PromoteEnrolled   PromoteReason = "enrolled"    // needs a baseline immediately
	PromoteEventStart PromoteReason = "event-start" // mandatory baseline sweep
	PromoteEventEnd   PromoteReason = "event-end"   // mandatory closing sweep
	PromoteHeartbeat  PromoteReason = "heartbeat"   // plugin says they are online
	PromoteRequested  PromoteReason = "requested"   // a human pressed refresh
	PromoteActivity   PromoteReason = "activity"    // submission, proof upload, site visit
)

// Promote moves players to the front of the queue, respecting the per-player floor so a burst of
// signals for one player cannot spend a disproportionate share of the budget on them.
//
// Bulk by design: an event-start sweep promotes every participant in one statement rather than one
// round trip per player, which for a 400-member clan is the difference between a query and a stall.
func (s *Store) Promote(ctx context.Context, playerIDs []int64, reason PromoteReason) (int64, error) {
	if len(playerIDs) == 0 {
		return 0, nil
	}
	if s.DryRun {
		return 0, nil
	}

	// GREATEST(now(), last_polled_at + floor) is the whole guard: due immediately unless this
	// player was polled within the last minute, in which case just after that.
	tag, err := s.pool.Exec(ctx, `
		UPDATE forge_sweep_state
		SET next_poll_at = GREATEST(now(), COALESCE(last_polled_at, 'epoch'::timestamptz) + $2::interval),
		    tier         = 0,
		    miss_streak  = 0
		WHERE player_id = ANY($1)
		  AND enrolled`,
		playerIDs, minPollInterval.String())
	if err != nil {
		return 0, fmt.Errorf("promoting %d players (%s): %w", len(playerIDs), reason, err)
	}
	return tag.RowsAffected(), nil
}

// minPollInterval mirrors sweep.MinPollInterval. Duplicated rather than imported to keep the store
// package free of a dependency on the scheduler; the two are asserted equal in the sweep tests.
const minPollInterval = 60 * time.Second

// PromoteMoved promotes only players who have actually gained something since a given time.
//
// This is lifted from WOM's `competition-ending-2h` handler, and it is the single best filter in
// their design. An account that has not moved for the whole event will not have moved in the last
// two hours either, so re-polling it at the exact moment every competition on the platform is
// closing spends the scarcest budget of the week on the least likely rows. Filtering to movers
// typically cuts an end-of-event sweep by more than half, and cuts it hardest on the bulk-enrolled
// dead rosters that need it least.
//
// Use it for the EARLY closing sweep (a couple of hours out). The final sweep at the actual end
// must still take everyone, because "they gained nothing" is itself a result that has to be exact.
func (s *Store) PromoteMoved(ctx context.Context, playerIDs []int64, since time.Time) (int64, error) {
	if len(playerIDs) == 0 {
		return 0, nil
	}
	if s.DryRun {
		return 0, nil
	}

	tag, err := s.pool.Exec(ctx, `
		UPDATE forge_sweep_state s
		SET next_poll_at = GREATEST(now(), COALESCE(s.last_polled_at, 'epoch'::timestamptz) + $3::interval),
		    tier         = 0,
		    miss_streak  = 0
		WHERE s.player_id = ANY($1)
		  AND s.enrolled
		  AND s.last_change_at IS NOT NULL
		  AND s.last_change_at >= $2`,
		playerIDs, since, minPollInterval.String())
	if err != nil {
		return 0, fmt.Errorf("promoting movers among %d players: %w", len(playerIDs), err)
	}
	return tag.RowsAffected(), nil
}

// SetEnrolled is how the Site tells the scheduler which players are worth polling at all.
//
// Denormalised onto sweep_state deliberately: the claim query runs constantly and must stay a
// single-table index scan. Joining into the domain tables to ask "is this player in a live event"
// would put the Site's schema on Forge's hot path and couple the two services at exactly the point
// where they should be independent.
func (s *Store) SetEnrolled(ctx context.Context, playerIDs []int64, enrolled bool) (int64, error) {
	if len(playerIDs) == 0 {
		return 0, nil
	}
	if s.DryRun {
		return 0, nil
	}

	// A player who becomes enrolled is due immediately — they need a baseline before anything they
	// do can be scored, and that baseline is the one poll that cannot be deferred.
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO forge_sweep_state (player_id, enrolled, next_poll_at)
		SELECT id, $2, CASE WHEN $2 THEN now() ELSE 'infinity'::timestamptz END
		FROM forge_players WHERE id = ANY($1)
		ON CONFLICT (player_id) DO UPDATE
		SET enrolled = EXCLUDED.enrolled,
		    next_poll_at = CASE
		      WHEN EXCLUDED.enrolled AND NOT sweep_state.enrolled THEN now()
		      ELSE sweep_state.next_poll_at
		    END`,
		playerIDs, enrolled)
	if err != nil {
		return 0, fmt.Errorf("setting enrolled=%v for %d players: %w", enrolled, len(playerIDs), err)
	}
	return tag.RowsAffected(), nil
}

// RecordHeartbeat stamps a plugin push. Cheap and very frequent, so it is a single narrow update
// with no read first — and it does NOT promote on its own. The ladder reads live_seen_at at poll
// time and keeps the player hot from there, which means a session's worth of heartbeats costs one
// write each rather than one queue reshuffle each.
func (s *Store) RecordHeartbeat(ctx context.Context, playerID int64) error {
	if s.DryRun {
		return nil
	}
	_, err := s.pool.Exec(ctx,
		`UPDATE forge_sweep_state SET live_seen_at = now() WHERE player_id = $1`, playerID)
	if err != nil {
		return fmt.Errorf("recording heartbeat for %d: %w", playerID, err)
	}
	return nil
}
