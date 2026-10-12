// Package sweep drains Anvil.Site's hiscores fetch queue under one global rate limit.
//
// The Site decides who is due and what a result means; Forge only does the part that has to run
// continuously and be rate limited as a whole — talking to Jagex. See docs/BOUNDARY.md.
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

// Fetcher reads one player's stats body. *hiscores.Client in production; a fake in tests.
type Fetcher interface {
	Fetch(ctx context.Context, rsn string, mode hiscores.Mode) hiscores.Result
}

// Queue is the slice of the store the runner needs.
type Queue interface {
	Claim(ctx context.Context, limit int, lease time.Duration) ([]store.Request, error)
	Complete(ctx context.Context, id int64, outcome string, body []byte, errText string) error
	Backlog(ctx context.Context) (int, error)
	RecordRun(ctx context.Context, started time.Time, st store.RunStats) error
}

// Runner claims queued requests and fetches them.
type Runner struct {
	Queue    Queue
	Hiscores Fetcher
	Log      *slog.Logger

	// Limiter is the GLOBAL budget against Jagex, shared by every worker. One limiter for the
	// process is the point: per-worker limits compose into a total nobody chose.
	Limiter *rate.Limiter

	Workers      int
	ClaimBatch   int
	ClaimLease   time.Duration
	TickInterval time.Duration
}

// Run works until the context is cancelled. An idle queue is checked every TickInterval; a busy
// one is drained back to back.
func (r *Runner) Run(ctx context.Context) error {
	for {
		n, err := r.Tick(ctx)
		if err != nil && ctx.Err() == nil {
			// Survivable: nothing was marked done, so the next tick re-claims it after the lease.
			r.Log.Error("sweep tick failed", "error", err)
		}
		if n > 0 && err == nil {
			continue // more may be waiting; the limiter, not the ticker, sets the pace
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(r.TickInterval):
		}
	}
}

// Tick claims one batch and fetches it. Returns how many requests it claimed.
func (r *Runner) Tick(ctx context.Context) (int, error) {
	started := time.Now()
	claims, err := r.Queue.Claim(ctx, r.ClaimBatch, r.ClaimLease)
	if err != nil {
		return 0, err
	}
	if len(claims) == 0 {
		return 0, nil
	}

	var fetched, unranked, errs atomic.Int64
	queue := make(chan store.Request)
	var wg sync.WaitGroup
	for i := 0; i < r.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for req := range queue {
				if err := r.Limiter.Wait(ctx); err != nil {
					return // cancelled; unclaimed rows return after their lease
				}
				res := r.Hiscores.Fetch(ctx, req.Rsn, hiscores.Mode(req.Mode))
				if ctx.Err() != nil {
					return // a cancelled fetch is not a result; let the lease hand it back
				}
				errText := ""
				switch res.Outcome {
				case hiscores.OutcomeOK:
					fetched.Add(1)
				case hiscores.OutcomeUnranked:
					unranked.Add(1)
				default:
					errs.Add(1)
				}
				if res.Err != nil {
					errText = res.Err.Error()
				}
				if err := r.Queue.Complete(ctx, req.ID, res.Outcome.String(), res.Body, errText); err != nil && ctx.Err() == nil {
					r.Log.Error("completing fetch", "id", req.ID, "rsn", req.Rsn, "error", err)
				}
			}
		}()
	}
	for _, c := range claims {
		select {
		case <-ctx.Done():
			close(queue)
			wg.Wait()
			return len(claims), ctx.Err()
		case queue <- c:
		}
	}
	close(queue)
	wg.Wait()

	backlog, err := r.Queue.Backlog(ctx)
	if err != nil {
		r.Log.Warn("reading backlog", "error", err)
	}
	rs := store.RunStats{
		Claimed:  len(claims),
		Fetched:  int(fetched.Load()),
		Unranked: int(unranked.Load()),
		Errors:   int(errs.Load()),
		Backlog:  backlog,
	}
	if err := r.Queue.RecordRun(ctx, started, rs); err != nil {
		r.Log.Warn("recording run", "error", err)
	}
	r.Log.Info("sweep.tick",
		"claimed", rs.Claimed,
		"fetched", rs.Fetched,
		"unranked", rs.Unranked,
		"errors", rs.Errors,
		"backlog", rs.Backlog,
		"durationMs", time.Since(started).Milliseconds(),
	)
	return len(claims), nil
}
