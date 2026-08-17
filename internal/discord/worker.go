package discord

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anvilosrs/forge/internal/ratelimit"
)

// Worker drains the delivery queue.
type Worker struct {
	Queue  *Queue
	Sender *Sender
	Log    *slog.Logger

	// Limiter is keyed by webhook bucket, because Discord's limit is per webhook. A global limiter
	// would throttle a quiet clan on account of a busy one and still exceed the limit on the busy
	// one — wrong in both directions at once.
	Limiter *ratelimit.Keyed

	Workers    int
	ClaimBatch int
	ClaimLease time.Duration
	TickEvery  time.Duration

	// KeepDelivered is how long delivered rows survive before the prune sweep.
	KeepDelivered time.Duration
}

// Run drains the queue until the context ends.
func (w *Worker) Run(ctx context.Context) error {
	ticker := time.NewTicker(w.TickEvery)
	defer ticker.Stop()

	// Housekeeping runs far less often than delivery and must not share its cadence.
	go w.housekeep(ctx)

	for {
		// Keep draining while there is work, so a burst at an event boundary is not metered out one
		// batch per tick. The tick only decides how long to idle when the queue is empty.
		for {
			n, err := w.drain(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				w.Log.Error("discord drain failed", "error", err)
				break
			}
			if n < w.ClaimBatch {
				break // queue is drained for now
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (w *Worker) drain(ctx context.Context) (int, error) {
	claims, err := w.Queue.Claim(ctx, w.ClaimBatch, w.ClaimLease)
	if err != nil {
		return 0, err
	}
	if len(claims) == 0 {
		return 0, nil
	}

	var delivered, retried, dead, dropped atomic.Int64

	queue := make(chan Delivery)
	var wg sync.WaitGroup
	for i := 0; i < w.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for d := range queue {
				// Per-webhook budget. Blocking here is the intended state: it is how a busy clan
				// paces itself without touching anyone else's throughput.
				if err := w.Limiter.Wait(ctx, d.Bucket); err != nil {
					return
				}

				res := w.Sender.Send(ctx, d.WebhookURL, d.Payload)

				// Honour Discord's own backoff for the whole bucket, not just this message.
				// Without this, a 429 is followed immediately by the next message to the same
				// webhook, which 429s in turn — the queue converges on being rate limited rather
				// than on being delivered.
				if res.RetryAfter > 0 {
					w.Limiter.ReserveFor(d.Bucket, res.RetryAfter)
				}

				switch res.Verdict {
				case VerdictDelivered:
					delivered.Add(1)
				case VerdictDead:
					dead.Add(1)
					w.Log.Warn("discord webhook is dead",
						"deliveryId", d.ID, "bucket", d.Bucket, "status", res.Status, "error", res.Err)
				case VerdictDrop:
					dropped.Add(1)
					w.Log.Warn("discord rejected payload",
						"deliveryId", d.ID, "bucket", d.Bucket, "status", res.Status, "error", res.Err)
				default:
					retried.Add(1)
				}

				if err := w.Queue.Complete(ctx, d, res); err != nil && ctx.Err() == nil {
					w.Log.Error("recording delivery outcome", "deliveryId", d.ID, "error", err)
				}
			}
		}()
	}

	for _, d := range claims {
		select {
		case <-ctx.Done():
			close(queue)
			wg.Wait()
			return 0, ctx.Err()
		case queue <- d:
		}
	}
	close(queue)
	wg.Wait()

	w.Log.Info("discord.drain",
		"claimed", len(claims),
		"delivered", delivered.Load(),
		"retried", retried.Load(),
		"dead", dead.Load(),
		"dropped", dropped.Load(),
		"buckets", w.Limiter.Len(),
	)
	return len(claims), nil
}

// housekeep prunes delivered rows, evicts idle rate-limit buckets, and reports queue health.
func (w *Worker) housekeep(ctx context.Context) {
	t := time.NewTicker(5 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if evicted := w.Limiter.Sweep(); evicted > 0 {
				w.Log.Debug("evicted idle rate-limit buckets", "count", evicted)
			}
			if n, err := w.Queue.Prune(ctx, w.KeepDelivered); err != nil {
				w.Log.Warn("pruning deliveries", "error", err)
			} else if n > 0 {
				w.Log.Info("pruned delivered messages", "count", n)
			}
			stats, err := w.Queue.Stats(ctx)
			if err != nil {
				w.Log.Warn("reading queue stats", "error", err)
				continue
			}
			// Oldest-pending age is the health signal, not depth: a deep queue that is moving is
			// fine, and a shallow one that is not moving is an outage.
			w.Log.Info("discord.queue",
				"pending", stats.Pending,
				"failed", stats.Failed,
				"dead", stats.Dead,
				"oldestPendingSeconds", int(stats.Oldest.Seconds()),
			)
		}
	}
}
