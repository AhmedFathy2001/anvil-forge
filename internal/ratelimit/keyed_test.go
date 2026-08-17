package ratelimit

import (
	"context"
	"sync"
	"testing"
	"time"
)

// The property the whole design rests on: one busy key must not slow a quiet one. Discord limits
// per webhook, so a global limiter would throttle a quiet clan on account of a busy one AND still
// exceed the limit on the busy one — wrong in both directions at once.
func TestKeysDoNotInterfere(t *testing.T) {
	k := NewKeyed(2, 1, time.Minute) // 2/s, burst 1 — deliberately tight
	ctx := context.Background()

	// Drain the busy key's bucket.
	if err := k.Wait(ctx, "busy"); err != nil {
		t.Fatal(err)
	}

	// A different key must still go straight through despite "busy" being exhausted.
	start := time.Now()
	if err := k.Wait(ctx, "quiet"); err != nil {
		t.Fatal(err)
	}
	if waited := time.Since(start); waited > 50*time.Millisecond {
		t.Errorf("quiet key waited %v behind a busy one; buckets are not isolated", waited)
	}
}

func TestSameKeyIsThrottled(t *testing.T) {
	k := NewKeyed(20, 1, time.Minute) // 20/s so the test stays fast
	ctx := context.Background()

	if err := k.Wait(ctx, "same"); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := k.Wait(ctx, "same"); err != nil {
		t.Fatal(err)
	}
	// Second call on an exhausted bucket must actually wait for a refill.
	if waited := time.Since(start); waited < 20*time.Millisecond {
		t.Errorf("second call on the same key waited only %v; not throttled", waited)
	}
}

// A 429 tells us exactly how long to hold off, which is better information than our own guess —
// but only if the NEXT message to that webhook waits too. Otherwise one 429 is followed straight
// away by another and the queue converges on being rate limited rather than on being delivered.
func TestReserveForHoldsTheWholeBucket(t *testing.T) {
	k := NewKeyed(100, 10, time.Minute) // generous, so only ReserveFor can cause a wait
	ctx := context.Background()

	k.ReserveFor("throttled", 120*time.Millisecond)

	start := time.Now()
	if err := k.Wait(ctx, "throttled"); err != nil {
		t.Fatal(err)
	}
	waited := time.Since(start)
	if waited < 80*time.Millisecond {
		t.Errorf("waited %v after a 120ms reservation; the hold was not honoured", waited)
	}

	// And it must not spill onto other webhooks.
	start = time.Now()
	if err := k.Wait(ctx, "unrelated"); err != nil {
		t.Fatal(err)
	}
	if spill := time.Since(start); spill > 50*time.Millisecond {
		t.Errorf("unrelated key waited %v; a 429 on one webhook stalled another", spill)
	}
}

func TestSweepEvictsIdleBuckets(t *testing.T) {
	// Twenty thousand live buckets is fine; twenty thousand plus every webhook ever deleted is a
	// leak that grows for the life of the process.
	k := NewKeyed(10, 1, 20*time.Millisecond)
	ctx := context.Background()

	for _, key := range []string{"a", "b", "c"} {
		if err := k.Wait(ctx, key); err != nil {
			t.Fatal(err)
		}
	}
	if k.Len() != 3 {
		t.Fatalf("live buckets = %d, want 3", k.Len())
	}

	time.Sleep(40 * time.Millisecond)
	if evicted := k.Sweep(); evicted != 3 {
		t.Errorf("evicted %d, want 3", evicted)
	}
	if k.Len() != 0 {
		t.Errorf("live buckets = %d after sweep, want 0", k.Len())
	}

	// A key that becomes busy again simply gets a fresh bucket.
	if err := k.Wait(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if k.Len() != 1 {
		t.Errorf("live buckets = %d after revival, want 1", k.Len())
	}
}

func TestConcurrentUseIsSafe(t *testing.T) {
	// Workers hit this from every goroutine in the pool; a data race here would be a rare,
	// production-only crash.
	k := NewKeyed(1000, 100, time.Minute)
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			key := string(rune('a' + n%8))
			for j := 0; j < 10; j++ {
				_ = k.Wait(ctx, key)
				if j%3 == 0 {
					k.ReserveFor(key, time.Millisecond)
				}
			}
		}(i)
	}
	wg.Wait()
	k.Sweep()
}
