package store

import (
	"context"
	"fmt"
	"time"
)

// Enrolment: deciding which accounts are worth polling at all.
//
// `accounts.sweep_enrolled` is denormalised from event and weekly participation because the claim
// query runs constantly and must stay a single-table index scan. Walking
// event_participants -> clan_memberships -> accounts on every claim would put the domain schema on
// the hot path and couple the two services exactly where they should be independent.
//
// This is deliberately NOT the old identity reconciler. The Site now owns global identity in
// `accounts`, so there is nothing to derive — only a flag to refresh.

// liveEventPredicate matches an account enrolled in a running bingo or an active weekly.
//
// The date comparisons are string comparisons against the Site's TEXT timestamps, which works only
// because the format is zero-padded and lexicographically ordered. That is load-bearing: compare a
// space-separated timestamp against an ISO one and the 'T' sorts after ' ', silently excluding
// rows. Both sides are generated the same way here for that reason.
const liveEventPredicate = `
	EXISTS (
	  SELECT 1
	  FROM clan_memberships cm
	  JOIN event_participants ep ON ep.clan_member_id = cm.id
	  JOIN events e ON e.id = ep.event_id
	  WHERE cm.account_id = a.id
	    AND cm.left_at IS NULL
	    AND e.force_ended_at IS NULL
	    AND (e.start_date IS NULL OR e.start_date <= $now)
	    AND (e.end_date   IS NULL OR e.end_date   >= $now)
	)
	OR EXISTS (
	  SELECT 1
	  FROM clan_memberships cm
	  JOIN weekly_participants wp ON wp.clan_member_id = cm.id
	  JOIN weekly_competitions wc ON wc.id = wp.competition_id
	  WHERE cm.account_id = a.id
	    AND cm.left_at IS NULL
	    AND wc.status = 'active'
	)`

// RefreshResult reports what one enrolment pass changed.
type RefreshResult struct {
	Enrolled   int64
	Unenrolled int64
}

// RefreshEnrolment brings sweep_enrolled in line with who is actually in something live.
//
// Newly enrolled accounts are made due immediately: they need a baseline before anything they do
// can be scored, and that baseline is the one poll that must never be deferred.
func (s *Store) RefreshEnrolment(ctx context.Context) (RefreshResult, error) {
	var r RefreshResult
	if s.DryRun {
		return r, nil
	}

	now := time.Now().UTC().Format(siteTime)

	enrol := `
		UPDATE accounts a
		SET sweep_enrolled = true,
		    -- NULL means "due now" in the Site's convention, which is what a new baseline needs.
		    stats_next_due_at = CASE WHEN a.sweep_enrolled THEN a.stats_next_due_at ELSE NULL END
		WHERE NOT a.sweep_enrolled
		  AND a.status = 'active'
		  AND (` + replaceNow(liveEventPredicate) + `)`

	tag, err := s.pool.Exec(ctx, enrol, now)
	if err != nil {
		return r, fmt.Errorf("marking enrolled: %w", err)
	}
	r.Enrolled = tag.RowsAffected()

	// And drop anyone whose events have all finished. They stay tracked; they just stop consuming
	// budget, which is the difference between a platform that scales and one that does not.
	unenrol := `
		UPDATE accounts a
		SET sweep_enrolled = false
		WHERE a.sweep_enrolled
		  AND NOT (` + replaceNow(liveEventPredicate) + `)`

	tag, err = s.pool.Exec(ctx, unenrol, now)
	if err != nil {
		return r, fmt.Errorf("un-enrolling finished accounts: %w", err)
	}
	r.Unenrolled = tag.RowsAffected()
	return r, nil
}

// replaceNow swaps the readable $now marker for the positional parameter. Keeping the predicate
// readable matters more than avoiding one string replace — it appears twice and a divergence
// between the enrol and un-enrol halves would make accounts flap in and out of the queue forever.
func replaceNow(pred string) string {
	out := ""
	for {
		i := indexOf(pred, "$now")
		if i < 0 {
			return out + pred
		}
		out += pred[:i] + "$1"
		pred = pred[i+len("$now"):]
	}
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// Fanout reports how much the global-account model is saving: how many clan seats exist versus how
// many distinct accounts they resolve to.
//
// Worth logging. It is the honest denominator for any claim about the polling budget, and it is the
// number that says whether one account in three clans really is costing one poll and not three.
func (s *Store) Fanout(ctx context.Context) (accounts int64, seats int64, err error) {
	err = s.pool.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM accounts WHERE sweep_enrolled),
		       (SELECT count(*) FROM clan_memberships cm
		          JOIN accounts a ON a.id = cm.account_id
		         WHERE cm.left_at IS NULL AND a.sweep_enrolled)`).Scan(&accounts, &seats)
	if err != nil {
		return 0, 0, fmt.Errorf("reading fanout: %w", err)
	}
	return accounts, seats, nil
}
