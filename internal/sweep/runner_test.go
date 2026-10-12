package sweep

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"golang.org/x/time/rate"

	"github.com/anvilosrs/forge/internal/hiscores"
	"github.com/anvilosrs/forge/internal/store"
)

type fakeQueue struct {
	mu        sync.Mutex
	pending   []store.Request
	completed map[int64]string
	bodies    map[int64]string
	runs      int
}

func (q *fakeQueue) Claim(_ context.Context, limit int, _ time.Duration) ([]store.Request, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	n := limit
	if n > len(q.pending) {
		n = len(q.pending)
	}
	out := q.pending[:n]
	q.pending = q.pending[n:]
	return out, nil
}

func (q *fakeQueue) Complete(_ context.Context, id int64, outcome string, body []byte, _ string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.completed[id] = outcome
	q.bodies[id] = string(body)
	return nil
}

func (q *fakeQueue) Backlog(context.Context) (int, error) { return len(q.pending), nil }

func (q *fakeQueue) RecordRun(context.Context, time.Time, store.RunStats) error {
	q.runs++
	return nil
}

type fakeFetcher struct{ modes sync.Map }

func (f *fakeFetcher) Fetch(_ context.Context, rsn string, mode hiscores.Mode) hiscores.Result {
	f.modes.Store(rsn, mode)
	if rsn == "Gone" {
		return hiscores.Result{Outcome: hiscores.OutcomeUnranked}
	}
	return hiscores.Result{Outcome: hiscores.OutcomeOK, Body: []byte(`{"skills":[{}]}`)}
}

func TestTickFetchesEveryClaimAndWritesTheResultBack(t *testing.T) {
	q := &fakeQueue{
		pending: []store.Request{
			{ID: 1, Rsn: "Zezima", Mode: "main"},
			{ID: 2, Rsn: "Gone", Mode: "main"},
			{ID: 3, Rsn: "Iron Guy", Mode: "ironman"},
		},
		completed: map[int64]string{},
		bodies:    map[int64]string{},
	}
	f := &fakeFetcher{}
	r := &Runner{
		Queue: q, Hiscores: f, Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Limiter: rate.NewLimiter(rate.Inf, 1), Workers: 2, ClaimBatch: 10, ClaimLease: time.Minute,
	}

	n, err := r.Tick(context.Background())
	if err != nil || n != 3 {
		t.Fatalf("tick = %d, %v", n, err)
	}
	if q.completed[1] != "ok" || q.completed[2] != "unranked" || q.completed[3] != "ok" {
		t.Fatalf("outcomes = %v", q.completed)
	}
	if q.bodies[1] != `{"skills":[{}]}` || q.bodies[2] != "" {
		t.Fatalf("bodies = %v", q.bodies)
	}
	if m, _ := f.modes.Load("Iron Guy"); m != hiscores.ModeIronman {
		t.Fatalf("the ironman request read table %v", m)
	}
	if q.runs != 1 {
		t.Fatalf("runs recorded = %d", q.runs)
	}

	// An empty queue is a no-op: no run row, nothing claimed.
	if n, _ := r.Tick(context.Background()); n != 0 || q.runs != 1 {
		t.Fatalf("idle tick claimed %d, runs %d", n, q.runs)
	}
}
