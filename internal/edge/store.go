// Package edge serves the plugin's read path and accepts its writes.
//
// It is deliberately incurious. It resolves a bearer token to a pre-rendered payload or to a
// player id, and that is the entire extent of what it understands. Anvil.Site builds every byte it
// serves and interprets every byte it stores.
package edge

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store is the edge's database access.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore wraps a pool.
func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// ErrNotFound means there is nothing bound for this caller and kind.
var ErrNotFound = errors.New("no payload bound")

// HashToken reduces a bearer token to the key we actually store.
//
// Never store or log the token itself. This table is the hottest read in the service, so it will
// end up in slow-query logs, EXPLAIN output and backups — and a plugin token is a credential that
// lets the holder push stats as that player.
func HashToken(token string) []byte {
	sum := sha256.Sum256([]byte(strings.TrimSpace(token)))
	return sum[:]
}

// Payload is a rendered response ready to serve.
type Payload struct {
	ETag string
	Body []byte
	// Encoding is "gzip" or "identity", describing Body as stored.
	Encoding string
}

// BoundPayload returns the payload bound to a caller for a given kind.
//
// One indexed join, no domain tables, no payload construction. This is the query that runs
// thousands of times a second, and its cost is the reason the read path is worth extracting at all.
func (s *Store) BoundPayload(ctx context.Context, tokenHash []byte, kind string) (*Payload, error) {
	var p Payload
	err := s.pool.QueryRow(ctx, `
		SELECT pp.etag, pp.body, pp.encoding
		FROM forge_plugin_bindings pb
		JOIN forge_plugin_payloads pp ON pp.etag = pb.etag
		WHERE pb.token_hash = $1 AND pb.kind = $2`,
		tokenHash, kind).Scan(&p.ETag, &p.Body, &p.Encoding)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("reading bound payload (%s): %w", kind, err)
	}
	return &p, nil
}

// PublicPayload returns an unauthenticated payload, such as a board preview for a given event.
func (s *Store) PublicPayload(ctx context.Context, scopeKey string) (*Payload, error) {
	var p Payload
	err := s.pool.QueryRow(ctx, `
		SELECT pp.etag, pp.body, pp.encoding
		FROM forge_plugin_public_payloads ppp
		JOIN forge_plugin_payloads pp ON pp.etag = ppp.etag
		WHERE ppp.scope_key = $1`,
		scopeKey).Scan(&p.ETag, &p.Body, &p.Encoding)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("reading public payload (%s): %w", scopeKey, err)
	}
	return &p, nil
}

// Caller is an authenticated plugin client.
type Caller struct {
	TokenHash []byte
	AccountID *int64
	Subject   *string
}

// Authenticate resolves a bearer token. Returns nil when the token is unknown or revoked.
//
// Note what it does NOT do: no auto-linking, no auto-claiming, no rename reconciliation. Those are
// real domain decisions with genuine subtlety, they live in Anvil.Site's resolvePluginMember, and
// putting them on this path would mean every poll ran them. The Site publishes the ANSWER here;
// Forge only looks it up.
func (s *Store) Authenticate(ctx context.Context, token string) (*Caller, error) {
	if strings.TrimSpace(token) == "" {
		return nil, nil
	}
	hash := HashToken(token)

	c := Caller{TokenHash: hash}
	err := s.pool.QueryRow(ctx, `
		SELECT account_id, subject
		FROM forge_plugin_credentials
		WHERE token_hash = $1 AND revoked_at IS NULL`,
		hash).Scan(&c.AccountID, &c.Subject)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("authenticating: %w", err)
	}
	return &c, nil
}

// TouchSeen records that a token was used, at most once per interval.
//
// Rate-limited by the WHERE clause rather than by a read-then-write, so a 30-second poll from every
// installed plugin does not turn a pure-read path into a write on every request. At 200k clients
// that is the difference between a few writes a second and a few thousand.
func (s *Store) TouchSeen(ctx context.Context, tokenHash []byte, interval time.Duration) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE forge_plugin_credentials
		SET last_seen_at = now()
		WHERE token_hash = $1
		  AND (last_seen_at IS NULL OR last_seen_at < now() - $2::interval)`,
		tokenHash, interval.String())
	if err != nil {
		return fmt.Errorf("touching credential: %w", err)
	}
	return nil
}

// Ingest appends one push from the game client.
//
// A repeated dedupe key is a no-op: the plugin retries on a flaky connection, and a retried kill
// must not credit twice. Returns false when the event was a duplicate.
func (s *Store) Ingest(ctx context.Context, c *Caller, kind string, payload json.RawMessage, dedupeKey string) (bool, error) {
	var key *string
	if dedupeKey != "" {
		key = &dedupeKey
	}

	tag, err := s.pool.Exec(ctx, `
		INSERT INTO forge_plugin_ingest_events (account_id, subject, kind, payload, dedupe_key)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (dedupe_key) WHERE dedupe_key IS NOT NULL DO NOTHING`,
		c.AccountID, c.Subject, kind, payload, key)
	if err != nil {
		return false, fmt.Errorf("ingesting %s: %w", kind, err)
	}
	return tag.RowsAffected() > 0, nil
}

// Heartbeat stamps the player as online, which is the strongest signal the sweep ever gets.
//
// Separate from Ingest because it is far more frequent and carries no payload worth storing — the
// only fact is "this player is logged in right now", and the sweep reads it from forge_sweep_state.
func (s *Store) Heartbeat(ctx context.Context, playerID int64) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE forge_sweep_state SET live_seen_at = now() WHERE player_id = $1`, playerID)
	if err != nil {
		return fmt.Errorf("recording heartbeat for %d: %w", playerID, err)
	}
	return nil
}
