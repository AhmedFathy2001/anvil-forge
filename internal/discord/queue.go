package discord

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Queue is the durable delivery queue.
type Queue struct {
	pool *pgxpool.Pool
}

// NewQueue wraps a pool.
func NewQueue(pool *pgxpool.Pool) *Queue { return &Queue{pool: pool} }

// Delivery is one queued message.
type Delivery struct {
	ID         int64
	WebhookURL string
	Bucket     string
	Payload    json.RawMessage
	Attempts   int
	Priority   int16
}

// BucketFor derives the rate-limit bucket from a webhook URL.
//
// Hashed rather than stored raw because the bucket appears in logs and metrics, and a webhook URL
// is a credential — anyone holding it can post to the channel as the clan. A leaked log should not
// hand that out.
func BucketFor(webhookURL string) string {
	sum := sha256.Sum256([]byte(webhookURL))
	return hex.EncodeToString(sum[:8])
}

// Enqueue adds a message. A repeated dedupe key is a no-op, which is what makes the Site's
// consumer safe to replay: re-processing an outbox event cannot double-post to Discord.
func (q *Queue) Enqueue(ctx context.Context, webhookURL string, payload json.RawMessage, dedupeKey string, priority int16) (int64, error) {
	var id int64
	err := q.pool.QueryRow(ctx, `
		INSERT INTO discord_deliveries (webhook_url, bucket, payload, dedupe_key, priority)
		VALUES ($1, $2, $3, NULLIF($4, ''), $5)
		ON CONFLICT (dedupe_key) DO NOTHING
		RETURNING id`,
		webhookURL, BucketFor(webhookURL), payload, dedupeKey, priority).Scan(&id)
	if err != nil {
		// No row means the dedupe key already existed — the message is already queued or sent.
		if err.Error() == "no rows in result set" {
			return 0, nil
		}
		return 0, fmt.Errorf("enqueuing delivery: %w", err)
	}
	return id, nil
}

// Claim leases up to `limit` due deliveries.
//
// Ordered by priority then due time so an event-start announcement does not queue behind a backlog
// of routine drop notifications. The lease is written into next_attempt_at, exactly as the sweep
// does, so a crashed worker's messages become due again rather than being stuck in 'delivering'.
func (q *Queue) Claim(ctx context.Context, limit int, lease time.Duration) ([]Delivery, error) {
	rows, err := q.pool.Query(ctx, `
		UPDATE discord_deliveries d
		SET status = 'delivering',
		    next_attempt_at = now() + $2::interval,
		    attempts = d.attempts + 1
		FROM (
		  SELECT id FROM discord_deliveries
		  WHERE status IN ('pending', 'failed') AND next_attempt_at <= now()
		  ORDER BY priority DESC, next_attempt_at
		  LIMIT $1
		  FOR UPDATE SKIP LOCKED
		) due
		WHERE d.id = due.id
		RETURNING d.id, d.webhook_url, d.bucket, d.payload, d.attempts, d.priority`,
		limit, lease.String())
	if err != nil {
		return nil, fmt.Errorf("claiming deliveries: %w", err)
	}
	defer rows.Close()

	var out []Delivery
	for rows.Next() {
		var d Delivery
		if err := rows.Scan(&d.ID, &d.WebhookURL, &d.Bucket, &d.Payload, &d.Attempts, &d.Priority); err != nil {
			return nil, fmt.Errorf("scanning delivery: %w", err)
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// Complete records the outcome of an attempt.
func (q *Queue) Complete(ctx context.Context, d Delivery, res Result) error {
	switch res.Verdict {
	case VerdictDelivered:
		_, err := q.pool.Exec(ctx, `
			UPDATE discord_deliveries
			SET status = 'delivered', delivered_at = now(), last_status = $2, last_error = NULL
			WHERE id = $1`, d.ID, res.Status)
		return wrap(err, "marking delivered", d.ID)

	case VerdictDead:
		// Terminal and actionable: the clan's webhook is gone. Kept as a row rather than deleted
		// so an admin surface can say "these 40 messages failed because your webhook was deleted"
		// instead of the clan simply noticing that Discord went quiet.
		_, err := q.pool.Exec(ctx, `
			UPDATE discord_deliveries
			SET status = 'dead', last_status = $2, last_error = $3
			WHERE id = $1`, d.ID, res.Status, errText(res.Err))
		return wrap(err, "marking dead", d.ID)

	case VerdictDrop:
		_, err := q.pool.Exec(ctx, `
			UPDATE discord_deliveries
			SET status = 'failed', last_status = $2, last_error = $3,
			    next_attempt_at = 'infinity'::timestamptz
			WHERE id = $1`, d.ID, res.Status, errText(res.Err))
		return wrap(err, "dropping", d.ID)

	default: // retry
		if d.Attempts >= MaxAttempts {
			_, err := q.pool.Exec(ctx, `
				UPDATE discord_deliveries
				SET status = 'failed', last_status = $2, last_error = $3,
				    next_attempt_at = 'infinity'::timestamptz
				WHERE id = $1`, d.ID, res.Status, errText(res.Err))
			return wrap(err, "exhausting retries", d.ID)
		}
		// Discord's own instruction beats our guess whenever we have one.
		delay := res.RetryAfter
		if delay <= 0 {
			delay = Backoff(d.Attempts)
		}
		_, err := q.pool.Exec(ctx, `
			UPDATE discord_deliveries
			SET status = 'pending', next_attempt_at = now() + $2::interval,
			    last_status = $3, last_error = $4
			WHERE id = $1`, d.ID, delay.String(), res.Status, errText(res.Err))
		return wrap(err, "scheduling retry", d.ID)
	}
}

// Stats is a snapshot of queue health.
type Stats struct {
	Pending int
	Dead    int
	Failed  int
	Oldest  time.Duration
}

// Stats reports queue depth and how long the oldest undelivered message has waited. The age is the
// number that matters: depth alone cannot distinguish a busy queue from a stuck one.
func (q *Queue) Stats(ctx context.Context) (Stats, error) {
	var s Stats
	var oldest *time.Time
	err := q.pool.QueryRow(ctx, `
		SELECT
		  count(*) FILTER (WHERE status IN ('pending','delivering')),
		  count(*) FILTER (WHERE status = 'dead'),
		  count(*) FILTER (WHERE status = 'failed'),
		  min(created_at) FILTER (WHERE status IN ('pending','delivering'))
		FROM discord_deliveries`).Scan(&s.Pending, &s.Dead, &s.Failed, &oldest)
	if err != nil {
		return s, fmt.Errorf("reading queue stats: %w", err)
	}
	if oldest != nil {
		s.Oldest = time.Since(*oldest)
	}
	return s, nil
}

// Prune deletes delivered rows past their retention window. Delivered messages are only
// interesting for long enough to answer "did that post?", and at twenty thousand clans they are
// the fastest-growing table in the database.
func (q *Queue) Prune(ctx context.Context, keepDelivered time.Duration) (int64, error) {
	tag, err := q.pool.Exec(ctx, `
		DELETE FROM discord_deliveries
		WHERE status = 'delivered' AND delivered_at < now() - $1::interval`, keepDelivered.String())
	if err != nil {
		return 0, fmt.Errorf("pruning deliveries: %w", err)
	}
	return tag.RowsAffected(), nil
}

func wrap(err error, what string, id int64) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s delivery %d: %w", what, id, err)
}

func errText(err error) *string {
	if err == nil {
		return nil
	}
	s := truncate(err.Error(), 1000)
	return &s
}
