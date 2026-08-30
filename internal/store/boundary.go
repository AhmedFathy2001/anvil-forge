package store

import (
	"context"
	"fmt"
	"time"
)

// Boundary polls: the ones that are never negotiable.
//
// Continuous polling cannot cover the enrolled population at any polite request rate (see
// docs/RATE_BUDGET.md), so correctness is anchored at the two moments an exact number is actually
// required, and the ladder does best-effort in between:
//
//	EVENT START — every participant polled once. The frozen baseline every gain is measured from,
//	              so it is the one poll that must not be skipped or deferred.
//	EVENT END   — every participant polled once, closing the books exactly. Without it the final
//	              standings are "whatever the ladder happened to collect", at precisely the moment
//	              nobody will accept an approximation.
//
// Twice per event rather than continuous, which is why they are affordable at all.

// PromoteReason names why an account jumped the queue, for the run log.
type PromoteReason string

const (
	PromoteEnrolled   PromoteReason = "enrolled"
	PromoteEventStart PromoteReason = "event-start"
	PromoteEventEnd   PromoteReason = "event-end"
	PromoteHeartbeat  PromoteReason = "heartbeat"
	PromoteRequested  PromoteReason = "requested"
	PromoteActivity   PromoteReason = "activity"
)

// minPollInterval mirrors sweep.MinPollInterval. Duplicated rather than imported to keep the store
// free of a dependency on the scheduler; the two are asserted equal in the sweep tests.
const minPollInterval = 60 * time.Second

// Promote moves accounts to the front of the queue, respecting the per-account floor so a burst of
// signals for one person cannot spend a disproportionate share of a scarce budget on them.
//
// Bulk by design: an event-start sweep promotes every participant in one statement rather than one
// round trip each, which for a 400-member clan is the difference between a query and a stall.
func (s *Store) Promote(ctx context.Context, accountIDs []int64, reason PromoteReason) (int64, error) {
	if len(accountIDs) == 0 || s.DryRun {
		return 0, nil
	}

	// Due now (NULL, the Site's convention) unless this account was polled within the last minute,
	// in which case just after that. sweep_claimed_at is the last time a worker took it, which is
	// the closest thing to "last polled" that the schedule columns carry.
	tag, err := s.pool.Exec(ctx, `
		UPDATE accounts
		SET stats_next_due_at = CASE
		      WHEN sweep_claimed_at IS NOT NULL AND sweep_claimed_at > now() - $2::interval
		      THEN to_char(sweep_claimed_at at time zone 'utc' + $2::interval, 'YYYY-MM-DD HH24:MI:SS')
		      ELSE NULL
		    END,
		    sweep_tier = 0,
		    stats_miss_streak = 0
		WHERE id = ANY($1) AND sweep_enrolled`,
		accountIDs, minPollInterval.String())
	if err != nil {
		return 0, fmt.Errorf("promoting %d accounts (%s): %w", len(accountIDs), reason, err)
	}
	return tag.RowsAffected(), nil
}

// PromoteMoved promotes only accounts that have actually gained something recently.
//
// The single best filter for a sweep fired as events end: an
// account that has not moved all event will not have moved in the last two hours either, so
// re-polling it at the exact moment every competition on the platform is closing spends the
// scarcest budget of the week on the least likely rows.
//
// For the EARLY closing sweep only. The final sweep at the actual end must take everyone, because
// "they gained nothing" is itself a result that has to be exact.
func (s *Store) PromoteMoved(ctx context.Context, accountIDs []int64, since time.Time) (int64, error) {
	if len(accountIDs) == 0 || s.DryRun {
		return 0, nil
	}

	// "Moved recently" is read off the outbox rather than a column: forge_player_events only ever
	// gets a snapshot.changed row when something actually rose, so it is already the record of who
	// is playing.
	tag, err := s.pool.Exec(ctx, `
		UPDATE accounts a
		SET stats_next_due_at = NULL, sweep_tier = 0, stats_miss_streak = 0
		WHERE a.id = ANY($1)
		  AND a.sweep_enrolled
		  AND EXISTS (
		    SELECT 1 FROM forge_player_events e
		    WHERE e.account_id = a.id
		      AND e.kind = 'snapshot.changed'
		      AND e.created_at >= $2
		  )`, accountIDs, since)
	if err != nil {
		return 0, fmt.Errorf("promoting movers among %d accounts: %w", len(accountIDs), err)
	}
	return tag.RowsAffected(), nil
}

// RecordHeartbeat stamps a plugin push. Cheap and very frequent, so it is one narrow update with no
// read first — and it does NOT promote on its own. The ladder reads sweep_live_seen_at at poll time
// and keeps the account hot from there, so a session's worth of heartbeats costs one write each
// rather than one queue reshuffle each.
func (s *Store) RecordHeartbeat(ctx context.Context, accountID int64) error {
	if s.DryRun {
		return nil
	}
	_, err := s.pool.Exec(ctx,
		`UPDATE accounts SET sweep_live_seen_at = now() WHERE id = $1`, accountID)
	if err != nil {
		return fmt.Errorf("recording heartbeat for %d: %w", accountID, err)
	}
	return nil
}
