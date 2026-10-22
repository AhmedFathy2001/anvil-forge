// Package store is Forge's Postgres access layer.
//
// Same database as Anvil.Site. For the sweep, Forge touches exactly one Site table —
// hiscores_fetches, the queue the Site fills (Anvil.Site drizzle/0125) — plus its own run log. It
// claims pending rows, fetches them, and writes the raw result back. Everything a result MEANS is
// decided by the Site when it drains the queue.
package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Store owns the pool. Safe for concurrent use.
type Store struct {
	pool *pgxpool.Pool
	// DryRun claims and fetches but writes no result back: the rows return to the queue when their
	// lease lapses. NOT free — every claim is a real request to Jagex.
	DryRun bool
}

// Open connects and verifies the connection.
func Open(ctx context.Context, databaseURL string, dryRun bool) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parsing DATABASE_URL: %w", err)
	}
	// Forge shares this database with the site, so a stall here must not exhaust its connections.
	if cfg.MaxConns > 8 {
		cfg.MaxConns = 8
	}
	cfg.MaxConnLifetime = time.Hour
	cfg.MaxConnIdleTime = 10 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connecting: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pinging: %w", err)
	}
	return &Store{pool: pool, DryRun: dryRun}, nil
}

// Close releases the pool.
func (s *Store) Close() { s.pool.Close() }

// Pool exposes the underlying pool for health checks, the edge and the Discord queue.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Request is one queued fetch.
type Request struct {
	ID   int64
	Rsn  string
	Mode string
}

// Queue lanes (hiscores_fetches.priority; the Site decides which lane a request goes in):
//
//	-1  late joiner — one player just added to a running competition, waiting on their start line
//	 0  a competition is waiting
//	 1  roster filler
//	 2  background account-type probes
//
// Lower goes first. The late-joiner lane (any priority below zero) jumps everything, but only up to
// JumpShare(limit) rows per claim: a burst of late adds can slow the competition lane, never stop it.
// An older Forge that knows nothing of the lane orders it first anyway (ORDER BY priority), just
// without the bound — the Site keeps that burst small on its side too.

// JumpShare is how many late-joiner rows one claim of `limit` may take: a fifth, at least one.
func JumpShare(limit int) int {
	if limit <= 0 {
		return 0
	}
	n := limit / 5
	if n < 1 {
		n = 1
	}
	return n
}

// Claim leases up to `limit` pending requests, highest priority (lowest number) and oldest first,
// skipping rows another worker holds — late joiners first, capped at JumpShare(limit), then the rest.
// A row whose lease lapsed — its worker crashed mid-fetch — is pending again, so nothing is lost and
// nothing needs a reaper.
func (s *Store) Claim(ctx context.Context, limit int, lease time.Duration) ([]Request, error) {
	rows, err := s.pool.Query(ctx, `
		WITH jump AS (
		  SELECT id, priority, requested_at FROM hiscores_fetches
		  WHERE fetched_at IS NULL
		    AND (claimed_at IS NULL OR claimed_at < now() - $2::interval)
		    AND priority < 0
		  ORDER BY priority, requested_at
		  LIMIT $3
		  FOR UPDATE SKIP LOCKED
		), rest AS (
		  SELECT id, priority, requested_at FROM hiscores_fetches
		  WHERE fetched_at IS NULL
		    AND (claimed_at IS NULL OR claimed_at < now() - $2::interval)
		    AND priority >= 0
		  ORDER BY priority, requested_at
		  LIMIT $1
		  FOR UPDATE SKIP LOCKED
		), due AS (
		  SELECT id FROM (SELECT * FROM jump UNION ALL SELECT * FROM rest) picked
		  ORDER BY priority, requested_at
		  LIMIT $1
		), claimed AS (
		  UPDATE hiscores_fetches f
		  SET claimed_at = now()
		  FROM due
		  WHERE f.id = due.id
		  RETURNING f.id, f.rsn, f.mode, f.priority, f.requested_at
		)
		-- Handed to the workers in lane order, so a late joiner is fetched first within the batch too.
		SELECT id, rsn, mode FROM claimed ORDER BY priority, requested_at`, limit, lease.String(), JumpShare(limit))
	if err != nil {
		return nil, fmt.Errorf("claiming fetches: %w", err)
	}
	defer rows.Close()

	var out []Request
	for rows.Next() {
		var r Request
		if err := rows.Scan(&r.ID, &r.Rsn, &r.Mode); err != nil {
			return nil, fmt.Errorf("scanning claim: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// Complete writes one fetch's result. `body` is the raw endpoint JSON and only set for "ok".
func (s *Store) Complete(ctx context.Context, id int64, outcome string, body []byte, errText string) error {
	if s.DryRun {
		return nil
	}
	var bodyArg, errArg any
	if len(body) > 0 {
		bodyArg = string(body)
	}
	if errText != "" {
		errArg = errText
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE hiscores_fetches
		SET fetched_at = now(), outcome = $2, body = $3, error = $4
		WHERE id = $1 AND fetched_at IS NULL`, id, outcome, bodyArg, errArg)
	if err != nil {
		return fmt.Errorf("completing fetch %d: %w", id, err)
	}
	return nil
}

// Backlog counts pending requests. Sustained growth means the Site is asking for more than the
// rate budget can fetch — widen the Site's polling ladder before raising the rate.
func (s *Store) Backlog(ctx context.Context) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx, `SELECT count(*) FROM hiscores_fetches WHERE fetched_at IS NULL`).Scan(&n)
	return n, err
}

// RunStats is one tick's tally.
type RunStats struct {
	Claimed  int
	Fetched  int
	Unranked int
	Errors   int
	Backlog  int
}

// RecordRun writes the tick's row to forge_sweep_runs, for observability. Nothing reads it to
// decide anything; a failure here is logged and ignored.
func (s *Store) RecordRun(ctx context.Context, started time.Time, st RunStats) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO forge_sweep_runs
		  (started_at, finished_at, claimed, fetched, changed, unranked, errors, backlog, shadow)
		VALUES ($1, now(), $2, $3, 0, $4, $5, $6, $7)`,
		started, st.Claimed, st.Fetched, st.Unranked, st.Errors, st.Backlog, s.DryRun)
	if err != nil {
		return fmt.Errorf("recording run: %w", err)
	}
	return nil
}
