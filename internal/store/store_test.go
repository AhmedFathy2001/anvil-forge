package store

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestJumpShare(t *testing.T) {
	cases := map[int]int{0: 0, 1: 1, 4: 1, 5: 1, 10: 2, 20: 4, 21: 4}
	for limit, want := range cases {
		if got := JumpShare(limit); got != want {
			t.Errorf("JumpShare(%d) = %d, want %d", limit, got, want)
		}
	}
}

// The claim against a real Postgres. Skipped unless FORGE_TEST_DATABASE_URL points at a server
// whose database may be scribbled on: it creates (and drops) its own schema.
func TestClaimLateJoinerLaneIsFirstButBounded(t *testing.T) {
	url := os.Getenv("FORGE_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("FORGE_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	schema := fmt.Sprintf("forge_claim_test_%d", time.Now().UnixNano())
	admin, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`) //nolint:errcheck

	cfg, _ := pgxpool.ParseConfig(url)
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	// The columns Claim reads, as the Site's migration makes them.
	if _, err := pool.Exec(ctx, `CREATE TABLE hiscores_fetches (
		id bigserial PRIMARY KEY, rsn text NOT NULL, rsn_key text NOT NULL, mode text NOT NULL DEFAULT 'main',
		priority smallint NOT NULL DEFAULT 1, requested_at timestamptz NOT NULL DEFAULT now(),
		claimed_at timestamptz, fetched_at timestamptz, outcome text, body text, error text, consumed_at timestamptz)`); err != nil {
		t.Fatal(err)
	}
	insert := func(prefix string, priority, n int) {
		for i := 0; i < n; i++ {
			name := fmt.Sprintf("%s%d", prefix, i)
			if _, err := pool.Exec(ctx, `INSERT INTO hiscores_fetches (rsn, rsn_key, priority) VALUES ($1, $1, $2)`, name, priority); err != nil {
				t.Fatal(err)
			}
		}
	}
	insert("comp", 0, 10)
	insert("late", -1, 10) // requested AFTER the competition rows, and still first
	insert("fill", 1, 5)

	s := &Store{pool: pool}
	got, err := s.Claim(ctx, 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 10 {
		t.Fatalf("claimed %d, want 10", len(got))
	}
	late, comp := 0, 0
	for i, r := range got {
		switch r.Rsn[:4] {
		case "late":
			late++
			if i >= 2 {
				t.Errorf("late joiner at position %d — the lane goes first", i)
			}
		case "comp":
			comp++
		default:
			t.Errorf("claimed %s while competition work was waiting", r.Rsn)
		}
	}
	if late != JumpShare(10) || comp != 10-JumpShare(10) {
		t.Fatalf("late=%d comp=%d, want %d late and the rest competition", late, comp, JumpShare(10))
	}

	// With nothing else pending, the lane still drains — a share per claim, claim after claim.
	if _, err := pool.Exec(ctx, `UPDATE hiscores_fetches SET fetched_at = now() WHERE priority >= 0`); err != nil {
		t.Fatal(err)
	}
	got, err = s.Claim(ctx, 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != JumpShare(10) {
		t.Fatalf("lane-only claim took %d, want %d", len(got), JumpShare(10))
	}
}
