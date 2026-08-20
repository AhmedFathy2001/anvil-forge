package sweep

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"github.com/anvilosrs/forge/internal/hiscores"
	"github.com/anvilosrs/forge/internal/store"
)

// Runner is the sweep loop: claim due players, fetch them under a global rate limit, apply the
// results.
//
// The shape is a long-lived worker pool rather than a cron tick, because the work is a continuous
// priority queue over millions of rows, not a batch. A cron-shaped sweep has to guess a batch size
// that fits its window; this one just runs, and the rate limiter — not the schedule — decides how
// fast.
type Runner struct {
	Store    *store.Store
	Hiscores *hiscores.Client
	Log      *slog.Logger

	// Limiter is the GLOBAL budget against Jagex, shared by every worker. One limiter for the
	// process is the point: per-worker limits compose into a total nobody chose, which is exactly
	// how the old per-clan-container design turned a safe 2.5 req/s into 125 req/s at fifty clans.
	Limiter *rate.Limiter

	Workers      int
	ClaimBatch   int
	ClaimLease   time.Duration
	TickInterval time.Duration
}

// Run works until the context is cancelled.
func (r *Runner) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.TickInterval)
	defer ticker.Stop()

	for {
		if err := r.tick(ctx); err != nil && ctx.Err() == nil {
			// A failed tick is survivable — the next one re-claims whatever was missed, because
			// nothing was marked done. Log and carry on rather than taking the service down.
			r.Log.Error("sweep tick failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (r *Runner) tick(ctx context.Context) error {
	started := time.Now()

	claims, err := r.Store.ClaimDue(ctx, r.ClaimBatch, r.ClaimLease)
	if err != nil {
		return err
	}
	if len(claims) == 0 {
		return nil // nothing due; the ladder is keeping up
	}

	var stats struct {
		fetched  atomic.Int64
		changed  atomic.Int64
		unranked atomic.Int64
		errors   atomic.Int64
	}

	queue := make(chan store.Claim)
	var wg sync.WaitGroup
	for i := 0; i < r.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for claim := range queue {
				// Block until the global budget allows this request. Every worker waiting here is
				// the intended state — the pool exists to keep the budget saturated despite slow
				// responses, not to exceed it.
				if err := r.Limiter.Wait(ctx); err != nil {
					return // context cancelled
				}
				outcome := r.poll(ctx, claim)
				switch outcome.Kind {
				case "ok":
					stats.fetched.Add(1)
					if outcome.Changed {
						stats.changed.Add(1)
					}
				case "unranked":
					stats.unranked.Add(1)
				default:
					stats.errors.Add(1)
				}
				overflowed, err := r.Store.Apply(ctx, outcome)
				if err != nil && ctx.Err() == nil {
					r.Log.Error("applying poll result",
						"accountId", claim.AccountID, "rsn", claim.Rsn, "error", err)
				}
				if overflowed {
					// Loud on purpose, and actionable: this is a schema bug in Anvil.Site that
					// silently mis-tracks its highest-XP players until someone widens the column.
					r.Log.Error("overall XP does not fit accounts.stats_overall_xp",
						"accountId", claim.AccountID, "rsn", claim.Rsn,
						"overallXp", outcome.OverallXp, "columnMax", 2147483647,
						"fix", "ALTER TABLE accounts ALTER COLUMN stats_overall_xp TYPE bigint")
				}
			}
		}()
	}

	for _, c := range claims {
		select {
		case <-ctx.Done():
			close(queue)
			wg.Wait()
			return ctx.Err()
		case queue <- c:
		}
	}
	close(queue)
	wg.Wait()

	backlog, err := r.Store.Backlog(ctx)
	if err != nil {
		r.Log.Warn("reading backlog", "error", err)
	}

	rs := store.RunStats{
		Claimed:  len(claims),
		Fetched:  int(stats.fetched.Load()),
		Changed:  int(stats.changed.Load()),
		Unranked: int(stats.unranked.Load()),
		Errors:   int(stats.errors.Load()),
		Backlog:  backlog,
	}
	if err := r.Store.RecordRun(ctx, started, rs); err != nil {
		r.Log.Warn("recording run", "error", err)
	}

	r.Log.Info("sweep.tick",
		"claimed", rs.Claimed,
		"fetched", rs.Fetched,
		"changed", rs.Changed,
		"unranked", rs.Unranked,
		"errors", rs.Errors,
		// The number to watch. Sustained non-zero means the enrolled population has outgrown the
		// request budget — widen the ladder before raising the rate.
		"backlog", rs.Backlog,
		"durationMs", time.Since(started).Milliseconds(),
	)
	return nil
}

// poll fetches one player and works out where they land on the ladder.
func (r *Runner) poll(ctx context.Context, c store.Claim) store.Outcome {
	now := time.Now()
	res := r.Hiscores.Fetch(ctx, c.Rsn)

	in := Input{
		MissStreak:  c.MissStreak,
		ErrorStreak: c.ErrorStreak,
		Outcome:     res.Outcome.String(),
		LiveSeenAt:  c.LiveSeenAt,
		Now:         now,
	}

	out := store.Outcome{
		AccountID:  c.AccountID,
		Rsn:        c.Rsn,
		Kind:       res.Outcome.String(),
		CapturedAt: now,
	}

	if res.Outcome == hiscores.OutcomeOK {
		out.Snapshot = res.Snapshot
		out.OverallXp = res.Snapshot.OverallXp()

		// Prune the plugin overlay against the fresh read: keys the hiscores caught up to, and keys
		// stuck above them past the logout window. Done on every successful poll, changed or not —
		// healing a bogus push is exactly the case where nothing else moved.
		out.LiveStats, out.LiveStatsChanged = ReconcileOverlay(
			ParseOverlay(c.LiveStats), ParseKeyTimes(c.LiveStatKeyTimes), res.Snapshot, now)
		// The change detector: total XP, compared against the value read at claim time. A player
		// we have never polled counts as changed so their first snapshot always lands — that first
		// write is the baseline everything else is measured from.
		in.Changed = !c.HasPrevious || out.OverallXp != c.PrevOverallXp
		out.Changed = in.Changed

		if out.Changed && c.HasPrevious {
			// Only now is it worth loading the previous blob. Doing this at claim time instead
			// would move gigabytes a day to discover that most players did not play.
			previous, err := r.Store.PreviousSnapshot(ctx, c.AccountID)
			if err != nil {
				r.Log.Warn("loading previous snapshot", "accountId", c.AccountID, "error", err)
			} else {
				out.Deltas = hiscores.ComputeDeltas(previous, res.Snapshot)
			}
		}
	} else if res.Err != nil {
		r.Log.Debug("hiscores fetch failed",
			"accountId", c.AccountID, "rsn", c.Rsn, "outcome", res.Outcome, "error", res.Err)
	}

	d := Next(in)
	out.Tier = int16(d.Tier)
	out.MissStreak = d.MissStreak
	out.ErrorStreak = d.ErrorStreak
	out.NextPollAt = d.NextPollAt
	out.Reason = d.Reason
	return out
}
