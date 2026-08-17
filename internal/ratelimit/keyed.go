// Package ratelimit provides a per-key rate limiter.
package ratelimit

import (
	"context"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// Keyed hands out one rate limiter per key, creating them on demand and evicting idle ones.
//
// Discord's webhook limit is PER WEBHOOK, not global, which means a single global limiter is the
// wrong shape twice over: it throttles a quiet clan because a busy one is sending, and it still
// lets us exceed the limit on the busy one. One bucket per webhook is the only arrangement where
// twenty thousand clans do not interfere with each other.
//
// The eviction matters at that scale — twenty thousand live limiters is fine, but twenty thousand
// that never go away plus every webhook ever deleted is a leak. Idle buckets are dropped on a
// sweep, and a key that becomes busy again simply gets a fresh one.
type Keyed struct {
	limit rate.Limit
	burst int
	ttl   time.Duration

	mu      sync.Mutex
	buckets map[string]*bucket
}

type bucket struct {
	limiter  *rate.Limiter
	lastUsed time.Time
	// notBefore is a hard pause on top of the token bucket, used to honour a server's explicit
	// "retry after" instruction. Kept separate from the limiter because a token bucket can only
	// express a rate, and what a 429 gives us is a deadline.
	notBefore time.Time
}

// NewKeyed returns a limiter allowing `r` events per second per key, with the given burst.
// Buckets unused for `ttl` are evicted.
func NewKeyed(r rate.Limit, burst int, ttl time.Duration) *Keyed {
	if burst < 1 {
		burst = 1
	}
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}
	return &Keyed{limit: r, burst: burst, ttl: ttl, buckets: map[string]*bucket{}}
}

// Wait blocks until the key's bucket allows an event, or the context ends.
func (k *Keyed) Wait(ctx context.Context, key string) error {
	limiter, pause := k.get(key)

	// Serve any explicit hold first, then the ordinary rate. Both, in that order: a 429 tells us
	// when we may resume, not that we may then send without limit.
	if pause > 0 {
		timer := time.NewTimer(pause)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	return limiter.Wait(ctx)
}

// ReserveFor holds the key off for at least d, on top of its ordinary rate.
//
// This is how a 429 is honoured. Discord tells us exactly how long to wait in `retry_after`, which
// is far better information than our own guess — but it only helps if the NEXT message to the same
// webhook also waits. Without it, one 429 is followed immediately by another and the queue
// converges on being rate limited rather than on being delivered.
//
// Extending an existing hold never shortens it: two 429s in flight must not race such that the
// shorter one wins.
func (k *Keyed) ReserveFor(key string, d time.Duration) {
	if d <= 0 {
		return
	}
	until := time.Now().Add(d)

	k.mu.Lock()
	defer k.mu.Unlock()
	b := k.ensureLocked(key)
	if until.After(b.notBefore) {
		b.notBefore = until
	}
}

// get returns the key's limiter and how long it is currently held off for.
func (k *Keyed) get(key string) (*rate.Limiter, time.Duration) {
	k.mu.Lock()
	defer k.mu.Unlock()
	b := k.ensureLocked(key)
	return b.limiter, time.Until(b.notBefore)
}

// ensureLocked returns the key's bucket, creating it if needed. Caller holds k.mu.
func (k *Keyed) ensureLocked(key string) *bucket {
	b, ok := k.buckets[key]
	if !ok {
		b = &bucket{limiter: rate.NewLimiter(k.limit, k.burst)}
		k.buckets[key] = b
	}
	b.lastUsed = time.Now()
	return b
}

// Sweep evicts buckets idle for longer than the TTL, returning how many went. Call periodically.
func (k *Keyed) Sweep() int {
	cutoff := time.Now().Add(-k.ttl)
	k.mu.Lock()
	defer k.mu.Unlock()
	evicted := 0
	for key, b := range k.buckets {
		if b.lastUsed.Before(cutoff) {
			delete(k.buckets, key)
			evicted++
		}
	}
	return evicted
}

// Len reports how many buckets are live, for the run log.
func (k *Keyed) Len() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return len(k.buckets)
}
