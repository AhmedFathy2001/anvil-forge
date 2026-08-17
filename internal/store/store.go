// Package store is Forge's Postgres access layer.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/anvilosrs/forge/internal/hiscores"
)

// Store owns the pool. Safe for concurrent use.
type Store struct {
	pool *pgxpool.Pool
	// DryRun suppresses every mutation except the run log, so a sweep can be observed against
	// production data without touching a row.
	DryRun bool
}

// Open connects and verifies the connection.
func Open(ctx context.Context, databaseURL string, dryRun bool) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parsing DATABASE_URL: %w", err)
	}
	// Sized for the sweep's shape: short transactions, lots of them. The pool is the backstop
	// against a stall turning into connection exhaustion for everything else on the database.
	if cfg.MaxConns < 8 {
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

// Pool exposes the underlying pool for health checks.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Claim is one player leased for polling.
type Claim struct {
	PlayerID    int64
	Rsn         string
	MissStreak  int
	ErrorStreak int
	LiveSeenAt  time.Time
	// PrevOverallXp is the change detector, read at claim time so the common case — a poll that
	// finds nothing new — never has to load the previous snapshot blob at all. At 2M players and a
	// ~6 KB snapshot each, loading blobs speculatively would move gigabytes a day to learn nothing.
	PrevOverallXp int64
	// HasPrevious is false for a player we have never successfully polled.
	HasPrevious bool
}

// ClaimDue leases up to `limit` players whose next poll is due, skipping rows another worker holds.
//
// The lease is written into next_poll_at rather than tracked separately, so there is exactly one
// column deciding queue position and a crashed worker self-heals: its players simply become due
// again when the lease elapses. The real next_poll_at is written afterwards by ApplyResult.
func (s *Store) ClaimDue(ctx context.Context, limit int, lease time.Duration) ([]Claim, error) {
	const q = `
		UPDATE sweep_state s
		SET next_poll_at = now() + $2::interval,
		    claimed_at   = now()
		FROM (
		  SELECT player_id
		  FROM sweep_state
		  WHERE enrolled AND next_poll_at <= now()
		  ORDER BY next_poll_at
		  LIMIT $1
		  FOR UPDATE SKIP LOCKED
		) due
		JOIN players p ON p.id = due.player_id
		LEFT JOIN player_current pc ON pc.player_id = due.player_id
		WHERE s.player_id = due.player_id
		  AND p.status = 'active'
		RETURNING s.player_id, p.rsn, s.miss_streak, s.error_streak,
		          s.live_seen_at, pc.overall_xp`

	rows, err := s.pool.Query(ctx, q, limit, lease.String())
	if err != nil {
		return nil, fmt.Errorf("claiming due players: %w", err)
	}
	defer rows.Close()

	var out []Claim
	for rows.Next() {
		var c Claim
		var liveSeen *time.Time
		var prevXp *int64
		if err := rows.Scan(&c.PlayerID, &c.Rsn, &c.MissStreak, &c.ErrorStreak, &liveSeen, &prevXp); err != nil {
			return nil, fmt.Errorf("scanning claim: %w", err)
		}
		if liveSeen != nil {
			c.LiveSeenAt = *liveSeen
		}
		if prevXp != nil {
			c.PrevOverallXp = *prevXp
			c.HasPrevious = true
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// Backlog counts players who are due but were not claimed. Sustained non-zero means the polling
// budget is smaller than the enrolled population — the signal to widen the ladder rather than to
// raise the request rate.
func (s *Store) Backlog(ctx context.Context) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM sweep_state WHERE enrolled AND next_poll_at <= now()`).Scan(&n)
	return n, err
}

// PreviousSnapshot loads a player's last stored snapshot, for the delta computation. Only called
// when a change was detected, which is the minority of polls.
func (s *Store) PreviousSnapshot(ctx context.Context, playerID int64) (*hiscores.Snapshot, error) {
	var raw []byte
	err := s.pool.QueryRow(ctx,
		`SELECT snapshot FROM player_current WHERE player_id = $1`, playerID).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("loading previous snapshot for %d: %w", playerID, err)
	}
	var snap hiscores.Snapshot
	if err := json.Unmarshal(raw, &snap); err != nil {
		// A corrupt blob must not fail the tick — treat it as a first sighting. The player loses
		// one tick's deltas, which is strictly better than the sweep stalling on one bad row.
		return nil, nil
	}
	return &snap, nil
}

// Outcome records everything one poll produced.
type Outcome struct {
	PlayerID int64
	Rsn      string

	// Fetch classification: "ok", "unranked", or "transient".
	Kind string

	// Ladder decision.
	Tier        int16
	MissStreak  int
	ErrorStreak int
	NextPollAt  time.Time
	Reason      string

	// Set only when Kind is "ok" and the snapshot differed from the stored one.
	Changed    bool
	Snapshot   *hiscores.Snapshot
	OverallXp  int64
	Deltas     hiscores.Deltas
	CapturedAt time.Time
}

// Apply writes one poll's result: the ladder decision always, and — when something actually
// changed — the new snapshot, a history row, and an outbox event for the Site to score.
//
// All of it in one transaction, because a snapshot written without its event would be silently
// unscored, and an event without its snapshot would point at stats that are not there.
func (s *Store) Apply(ctx context.Context, o Outcome) error {
	if s.DryRun {
		return nil
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	_, err = tx.Exec(ctx, `
		UPDATE sweep_state
		SET next_poll_at   = $2,
		    tier           = $3,
		    miss_streak    = $4,
		    error_streak   = $5,
		    last_polled_at = now(),
		    last_outcome   = $6,
		    last_change_at = CASE WHEN $7 THEN now() ELSE last_change_at END,
		    claimed_at     = NULL
		WHERE player_id = $1`,
		o.PlayerID, o.NextPollAt, o.Tier, o.MissStreak, o.ErrorStreak, o.Kind, o.Changed)
	if err != nil {
		return fmt.Errorf("updating sweep_state for %d: %w", o.PlayerID, err)
	}

	switch o.Kind {
	case "unranked":
		// Take them out of the sweep entirely. Without this one renamed account 404s every tick
		// forever and steals a slot from a healthy row.
		if _, err := tx.Exec(ctx, `
			UPDATE players SET status = 'unranked', status_checked_at = now()
			WHERE id = $1 AND status = 'active'`, o.PlayerID); err != nil {
			return fmt.Errorf("flagging %d unranked: %w", o.PlayerID, err)
		}
		if err := insertEvent(ctx, tx, o.PlayerID, "player.unranked",
			map[string]any{"rsn": o.Rsn}); err != nil {
			return err
		}

	case "ok":
		if _, err := tx.Exec(ctx,
			`UPDATE players SET last_seen_at = now() WHERE id = $1`, o.PlayerID); err != nil {
			return fmt.Errorf("touching player %d: %w", o.PlayerID, err)
		}
		if !o.Changed {
			break // nothing moved: no snapshot, no history row, no event. The common case.
		}

		blob, err := json.Marshal(o.Snapshot)
		if err != nil {
			return fmt.Errorf("marshalling snapshot for %d: %w", o.PlayerID, err)
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO player_current (player_id, snapshot, overall_xp, captured_at)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (player_id) DO UPDATE
			SET snapshot = EXCLUDED.snapshot,
			    overall_xp = EXCLUDED.overall_xp,
			    captured_at = EXCLUDED.captured_at`,
			o.PlayerID, blob, o.OverallXp, o.CapturedAt); err != nil {
			return fmt.Errorf("upserting player_current for %d: %w", o.PlayerID, err)
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO player_snapshots (player_id, captured_at, overall_xp, snapshot)
			VALUES ($1, $2, $3, $4)`,
			o.PlayerID, o.CapturedAt, o.OverallXp, blob); err != nil {
			return fmt.Errorf("inserting snapshot for %d: %w", o.PlayerID, err)
		}

		if err := insertEvent(ctx, tx, o.PlayerID, "snapshot.changed", map[string]any{
			"capturedAt": o.CapturedAt,
			"overallXp":  o.OverallXp,
			"deltas":     o.Deltas,
		}); err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit for %d: %w", o.PlayerID, err)
	}
	return nil
}

func insertEvent(ctx context.Context, tx pgx.Tx, playerID int64, kind string, payload map[string]any) error {
	blob, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshalling %s payload: %w", kind, err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO player_events (player_id, kind, payload) VALUES ($1, $2, $3)`,
		playerID, kind, blob); err != nil {
		return fmt.Errorf("inserting %s event: %w", kind, err)
	}
	return nil
}

// RunStats is one tick's tally.
type RunStats struct {
	Claimed  int
	Fetched  int
	Changed  int
	Unranked int
	Errors   int
	Backlog  int
}

// RecordRun writes the tick's row. Written even in dry-run mode — observing behaviour is the whole
// point of dry-run, so the observation itself must land.
func (s *Store) RecordRun(ctx context.Context, started time.Time, st RunStats) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO sweep_runs
		  (started_at, finished_at, claimed, fetched, changed, unranked, errors, backlog, shadow)
		VALUES ($1, now(), $2, $3, $4, $5, $6, $7, $8)`,
		started, st.Claimed, st.Fetched, st.Changed, st.Unranked, st.Errors, st.Backlog, s.DryRun)
	if err != nil {
		return fmt.Errorf("recording run: %w", err)
	}
	return nil
}

// EnsurePartitions creates the snapshot partitions covering the next few months. Called at boot
// and daily: an insert into a range with no partition is a hard error, so these must exist BEFORE
// the month they cover, not during it.
func (s *Store) EnsurePartitions(ctx context.Context) error {
	for months := 0; months <= 2; months++ {
		when := time.Now().AddDate(0, months, 0)
		if _, err := s.pool.Exec(ctx,
			`SELECT ensure_snapshot_partition($1::date)`, when.Format("2006-01-02")); err != nil {
			return fmt.Errorf("ensuring partition for %s: %w", when.Format("2006-01"), err)
		}
	}
	return nil
}
