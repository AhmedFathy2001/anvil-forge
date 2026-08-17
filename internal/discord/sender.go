// Package discord delivers queued messages to Discord webhooks.
package discord

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

// Discord's documented webhook limit is 5 requests per 2 seconds, per webhook. We sit just under
// it: the cost of being slightly slow is invisible, and the cost of being slightly fast is a 429
// that delays the message anyway plus a worse reputation for the whole box.
const (
	WebhookRatePerSecond = 2.0
	WebhookBurst         = 4
)

// Verdict is what to do with a delivery after one attempt.
type Verdict int

const (
	// VerdictDelivered — Discord accepted it. Done.
	VerdictDelivered Verdict = iota
	// VerdictRetry — transient. Try again after RetryAfter.
	VerdictRetry
	// VerdictDead — the webhook no longer exists or we are not allowed to use it. Retrying can
	// never succeed, and the clan needs telling.
	VerdictDead
	// VerdictDrop — Discord rejected the message itself (malformed embed, too large). Retrying the
	// same bytes cannot help either, but unlike Dead the webhook is fine.
	VerdictDrop
)

func (v Verdict) String() string {
	switch v {
	case VerdictDelivered:
		return "delivered"
	case VerdictRetry:
		return "retry"
	case VerdictDead:
		return "dead"
	default:
		return "drop"
	}
}

// Result of one delivery attempt.
type Result struct {
	Verdict Verdict
	Status  int
	// RetryAfter is Discord's own instruction on how long to wait. Always prefer it over our
	// backoff — they know their limits and we are guessing.
	RetryAfter time.Duration
	Err        error
}

// Sender posts to Discord webhooks.
type Sender struct {
	HTTP      *http.Client
	UserAgent string
}

// NewSender returns a sender with a timeout short enough that a hung webhook cannot occupy a
// worker indefinitely.
func NewSender(userAgent string) *Sender {
	return &Sender{
		HTTP: &http.Client{
			Timeout: 15 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        256,
				MaxIdleConnsPerHost: 32,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		UserAgent: userAgent,
	}
}

// Send posts one payload and classifies the response.
func (s *Sender) Send(ctx context.Context, webhookURL string, payload json.RawMessage) Result {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, webhookURL, bytes.NewReader(payload))
	if err != nil {
		// A malformed URL cannot be fixed by retrying it.
		return Result{Verdict: VerdictDead, Err: fmt.Errorf("building request: %w", err)}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", s.UserAgent)

	resp, err := s.HTTP.Do(req)
	if err != nil {
		return Result{Verdict: VerdictRetry, Err: err}
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
		_ = resp.Body.Close()
	}()

	res := Result{Status: resp.StatusCode}

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		res.Verdict = VerdictDelivered
		return res

	case resp.StatusCode == http.StatusTooManyRequests:
		res.Verdict = VerdictRetry
		res.RetryAfter = parseRetryAfter(resp)
		res.Err = fmt.Errorf("rate limited, retry after %v", res.RetryAfter)
		return res

	case resp.StatusCode == http.StatusNotFound, resp.StatusCode == http.StatusUnauthorized,
		resp.StatusCode == http.StatusForbidden:
		// 404 means the webhook was deleted in Discord; 401/403 mean the token is wrong or the
		// channel's permissions changed. None of these heal on their own, and all of them are
		// something the clan can actually fix once told.
		res.Verdict = VerdictDead
		res.Err = fmt.Errorf("webhook rejected us with %d", resp.StatusCode)
		return res

	case resp.StatusCode == http.StatusBadRequest, resp.StatusCode == http.StatusRequestEntityTooLarge:
		// Our payload is wrong. The webhook is fine, so do not mark it dead — that would hide a
		// working integration behind one bad embed.
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		res.Verdict = VerdictDrop
		res.Err = fmt.Errorf("discord rejected the payload (%d): %s", resp.StatusCode, truncate(string(body), 500))
		return res

	default:
		res.Verdict = VerdictRetry
		res.Err = fmt.Errorf("discord returned %d", resp.StatusCode)
		return res
	}
}

// parseRetryAfter reads Discord's own backoff instruction.
//
// Discord sends it two ways depending on the edge that answers: a JSON body with `retry_after` in
// FRACTIONAL SECONDS, and a `Retry-After` header in whole seconds. Reading only the header rounds
// a 0.4s wait up to 1s or down to nothing depending on how it is parsed, so the body wins when
// present.
func parseRetryAfter(resp *http.Response) time.Duration {
	var body struct {
		RetryAfter float64 `json:"retry_after"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4096)).Decode(&body); err == nil && body.RetryAfter > 0 {
		return time.Duration(body.RetryAfter * float64(time.Second))
	}
	if h := resp.Header.Get("Retry-After"); h != "" {
		if secs, err := strconv.ParseFloat(h, 64); err == nil && secs > 0 {
			return time.Duration(secs * float64(time.Second))
		}
	}
	// Discord told us to slow down but not by how much. A second is long enough to matter and
	// short enough not to stall a queue.
	return time.Second
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// Backoff is the retry schedule for transient failures, used only when Discord does not tell us
// something better. Caps at 15 minutes: past that the message is stale enough that the clan has
// moved on, and we would rather spend the attempt on something current.
func Backoff(attempts int) time.Duration {
	switch {
	case attempts <= 1:
		return 5 * time.Second
	case attempts == 2:
		return 30 * time.Second
	case attempts == 3:
		return 2 * time.Minute
	case attempts == 4:
		return 5 * time.Minute
	default:
		return 15 * time.Minute
	}
}

// MaxAttempts before a delivery is abandoned. Six attempts spans roughly 23 minutes, which covers
// a Discord incident without keeping a dead message alive for hours.
const MaxAttempts = 6
