// Package store is Forge's Postgres access layer.
//
// Same database as Anvil.Site. Forge is a second process, not a second datastore — so the sweep's
// output goes into the columns the Site already reads (`accounts.stats_*`), and the only tables
// Forge adds are for things that have no home in the Site's schema at all.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/anvilosrs/forge/internal/hiscores"
)

// siteTime is the format Anvil.Site stores its text timestamps in.
//
// The Site keeps most timestamps as TEXT rather than timestamptz, in two different shapes depending
// on who wrote them (a SQLite-era space-separated form and a JS ISO one). Forge writes the form the
// Site's own SQL default produces, so a column stays internally consistent whichever process filled
// it — a mixed column is what makes date comparisons silently wrong rather than loudly broken.
const siteTime = "2006-01-02 15:04:05"

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
	// Sized for the sweep's shape: short transactions, lots of them. Also a backstop — Forge shares
	// this database with every clan container, so a stall here must not exhaust connections for the
	// app people are actually looking at.
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

// Pool exposes the underlying pool for health checks and the edge.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Claim is one account leased for polling.
type Claim struct {
	AccountID   int64
	Rsn         string
	MissStreak  int
	ErrorStreak int
	LiveSeenAt  time.Time
	// PrevOverallXp is the change detector, read at claim time so the common case — a poll that
	// finds nothing new — never loads the previous snapshot blob at all. A snapshot is ~6 KB; at
	// scale, fetching them speculatively would move gigabytes a day to learn nobody played.
	PrevOverallXp int64
	HasPrevious   bool
	// LiveStats is the plugin's absolute-value overlay, reconciled against fresh hiscores below.
	LiveStats []byte
	// LiveStatKeyTimes is the per-key last-rose map, which is what makes the stale-overlay prune
	// possible: without it a bogus push stuck ABOVE the hiscores can never be retired.
	LiveStatKeyTimes []byte
}

// ClaimDue leases up to `limit` accounts whose next poll is due, skipping rows another worker holds.
//
// The lease is written into stats_next_due_at rather than tracked separately, so exactly one column
// decides queue position and a crashed worker self-heals: its accounts simply become due again when
// the lease elapses. The real next-due is written afterwards by Apply.
//
// NULL stats_next_due_at means "due now" — that is the Site's existing convention and the reason a
// freshly enrolled account is picked up on the very next tick.
func (s *Store) ClaimDue(ctx context.Context, limit int, lease time.Duration) ([]Claim, error) {
	const q = `
		UPDATE accounts a
		SET stats_next_due_at = to_char(now() at time zone 'utc' + $2::interval, 'YYYY-MM-DD HH24:MI:SS'),
		    sweep_claimed_at  = now()
		FROM (
		  SELECT id FROM accounts
		  WHERE sweep_enrolled
		    AND status = 'active'
		    AND (stats_next_due_at IS NULL
		         OR stats_next_due_at <= to_char(now() at time zone 'utc', 'YYYY-MM-DD HH24:MI:SS'))
		  ORDER BY stats_next_due_at NULLS FIRST
		  LIMIT $1
		  FOR UPDATE SKIP LOCKED
		) due
		WHERE a.id = due.id
		RETURNING a.id, a.rsn, a.stats_miss_streak, a.sweep_error_streak,
		          a.sweep_live_seen_at, a.stats_overall_xp, a.live_stats, a.live_stat_key_times`

	rows, err := s.pool.Query(ctx, q, limit, lease.String())
	if err != nil {
		return nil, fmt.Errorf("claiming due accounts: %w", err)
	}
	defer rows.Close()

	var out []Claim
	for rows.Next() {
		var c Claim
		var liveSeen *time.Time
		var prevXp *int64
		var liveStats, keyTimes *string
		if err := rows.Scan(&c.AccountID, &c.Rsn, &c.MissStreak, &c.ErrorStreak,
			&liveSeen, &prevXp, &liveStats, &keyTimes); err != nil {
			return nil, fmt.Errorf("scanning claim: %w", err)
		}
		if liveSeen != nil {
			c.LiveSeenAt = *liveSeen
		}
		if prevXp != nil {
			c.PrevOverallXp = *prevXp
			c.HasPrevious = true
		}
		if liveStats != nil {
			c.LiveStats = []byte(*liveStats)
		}
		if keyTimes != nil {
			c.LiveStatKeyTimes = []byte(*keyTimes)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// Backlog counts accounts due but not claimed. Sustained non-zero means the enrolled population has
// outgrown the request budget — the signal to widen the ladder rather than to raise the rate.
func (s *Store) Backlog(ctx context.Context) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx, `
		SELECT count(*) FROM accounts
		WHERE sweep_enrolled AND status = 'active'
		  AND (stats_next_due_at IS NULL
		       OR stats_next_due_at <= to_char(now() at time zone 'utc', 'YYYY-MM-DD HH24:MI:SS'))`).Scan(&n)
	return n, err
}

// PreviousSnapshot loads an account's last stored snapshot, for the delta computation. Only called
// when a change was detected, which is the minority of polls.
func (s *Store) PreviousSnapshot(ctx context.Context, accountID int64) (*hiscores.Snapshot, error) {
	var raw *string
	err := s.pool.QueryRow(ctx,
		`SELECT stats_last_snapshot FROM accounts WHERE id = $1`, accountID).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) || raw == nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("loading previous snapshot for %d: %w", accountID, err)
	}
	var snap hiscores.Snapshot
	if err := json.Unmarshal([]byte(*raw), &snap); err != nil {
		// A corrupt blob must not fail the tick — treat it as a first sighting. The account loses
		// one tick's deltas, which is strictly better than the sweep stalling on one bad row.
		return nil, nil
	}
	return &snap, nil
}

// Outcome records everything one poll produced.
type Outcome struct {
	AccountID int64
	Rsn       string

	// Fetch classification: "ok", "unranked", or "transient".
	Kind string

	// Ladder decision.
	Tier        int16
	MissStreak  int
	ErrorStreak int
	NextPollAt  time.Time
	Reason      string

	// Set only when Kind is "ok" and the snapshot differed from the stored one.
	Changed   bool
	Snapshot  *hiscores.Snapshot
	OverallXp int64
	Deltas    hiscores.Deltas

	// LiveStats is the reconciled overlay to persist, and LiveStatsChanged whether it differs from
	// what was stored. Nil map with Changed=true clears the column.
	LiveStats        map[string]int64
	LiveStatsChanged bool

	CapturedAt time.Time
}

// Apply writes one poll's result.
//
// The scheduling columns always; the stats columns when something moved; an outbox event so the
// Site can score it. One transaction, because a snapshot written without its event would be
// silently unscored, and an event without its snapshot would point at stats that are not there.
func (s *Store) Apply(ctx context.Context, o Outcome) (overflowed bool, err error) {
	if s.DryRun {
		return false, nil
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	nextDue := o.NextPollAt.UTC().Format(siteTime)

	// The scheduling half. Written on every outcome, including failures — that is what moves the
	// account out of the queue head instead of it being re-claimed immediately.
	if _, err := tx.Exec(ctx, `
		UPDATE accounts
		SET stats_next_due_at  = $2,
		    stats_miss_streak  = $3,
		    sweep_tier         = $4,
		    sweep_error_streak = $5,
		    sweep_claimed_at   = NULL
		WHERE id = $1`,
		o.AccountID, nextDue, o.MissStreak, o.Tier, o.ErrorStreak); err != nil {
		return overflowed, fmt.Errorf("updating schedule for %d: %w", o.AccountID, err)
	}

	switch o.Kind {
	case "unranked":
		// Take them out of the sweep. Without this, one renamed account 404s every tick forever and
		// steals a slot from a healthy row. A re-probe job lifts them back.
		if _, err := tx.Exec(ctx, `
			UPDATE accounts
			SET status = 'unranked',
			    status_last_checked = to_char(now() at time zone 'utc', 'YYYY-MM-DD HH24:MI:SS')
			WHERE id = $1 AND status = 'active'`, o.AccountID); err != nil {
			return overflowed, fmt.Errorf("flagging %d unranked: %w", o.AccountID, err)
		}
		if err := insertEvent(ctx, tx, o.AccountID, "account.unranked",
			map[string]any{"rsn": o.Rsn}); err != nil {
			return overflowed, err
		}

	case "ok":
		// The overlay is reconciled against fresh hiscores on every successful poll, changed or
		// not: pruning keys the hiscores have caught up to is how a doubled plugin push stops
		// sitting above the real value forever.
		if o.LiveStatsChanged {
			var live *string
			if len(o.LiveStats) > 0 {
				blob, err := json.Marshal(o.LiveStats)
				if err != nil {
					return overflowed, fmt.Errorf("marshalling live stats for %d: %w", o.AccountID, err)
				}
				str := string(blob)
				live = &str
			}
			if _, err := tx.Exec(ctx,
				`UPDATE accounts SET live_stats = $2 WHERE id = $1`, o.AccountID, live); err != nil {
				return overflowed, fmt.Errorf("reconciling overlay for %d: %w", o.AccountID, err)
			}
		}

		if !o.Changed {
			break // nothing moved: no snapshot rewrite, no event. The common case.
		}

		blob, err := json.Marshal(o.Snapshot)
		if err != nil {
			return overflowed, fmt.Errorf("marshalling snapshot for %d: %w", o.AccountID, err)
		}

		// Only rewrite the blob when it would differ — an idle account's row stays untouched, which
		// is most of them, most of the time.
		// accounts.stats_overall_xp is `integer` (max 2,147,483,647), but a maxed OSRS account
		// carries ~4.6 BILLION total XP. pgx rejects the value while encoding, the transaction
		// rolls back, and the claim lease brings the same account back minutes later to fail
		// identically — a poison pill burning a request forever, for exactly the accounts most
		// likely to belong to a clan's best players.
		//
		// Forge cannot fix it: the column is Anvil.Site's, defined in its drizzle schema, and
		// widening it here would drift from schema.ts and be reverted by the next generate. So skip
		// just that column, keep the snapshot (which holds the true figure), and say so loudly.
		if o.OverallXp > math.MaxInt32 {
			overflowed = true
			if _, err := tx.Exec(ctx,
				`UPDATE accounts SET stats_last_snapshot = $2 WHERE id = $1`,
				o.AccountID, string(blob)); err != nil {
				return overflowed, fmt.Errorf("writing snapshot for %d: %w", o.AccountID, err)
			}
		} else if _, err := tx.Exec(ctx, `
			UPDATE accounts
			SET stats_overall_xp = $2,
			    stats_last_snapshot = $3
			WHERE id = $1`, o.AccountID, o.OverallXp, string(blob)); err != nil {
			return overflowed, fmt.Errorf("writing stats for %d: %w", o.AccountID, err)
		}

		deltas, err := json.Marshal(o.Deltas)
		if err != nil {
			return overflowed, fmt.Errorf("marshalling deltas for %d: %w", o.AccountID, err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO forge_player_events (account_id, kind, payload)
			VALUES ($1, 'snapshot.changed',
			        jsonb_build_object('capturedAt', $2::text, 'overallXp', $3::bigint,
			                           'deltas', $4::jsonb, 'snapshot', $5::jsonb))`,
			o.AccountID, o.CapturedAt.UTC().Format(time.RFC3339), o.OverallXp,
			string(deltas), string(blob)); err != nil {
			return overflowed, fmt.Errorf("emitting snapshot event for %d: %w", o.AccountID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return overflowed, fmt.Errorf("commit for %d: %w", o.AccountID, err)
	}
	return overflowed, nil
}

func insertEvent(ctx context.Context, tx pgx.Tx, accountID int64, kind string, payload map[string]any) error {
	blob, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshalling %s payload: %w", kind, err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO forge_player_events (account_id, kind, payload) VALUES ($1, $2, $3)`,
		accountID, kind, blob); err != nil {
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
		INSERT INTO forge_sweep_runs
		  (started_at, finished_at, claimed, fetched, changed, unranked, errors, backlog, shadow)
		VALUES ($1, now(), $2, $3, $4, $5, $6, $7, $8)`,
		started, st.Claimed, st.Fetched, st.Changed, st.Unranked, st.Errors, st.Backlog, s.DryRun)
	if err != nil {
		return fmt.Errorf("recording run: %w", err)
	}
	return nil
}
