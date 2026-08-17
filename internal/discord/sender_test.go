package discord

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSendClassifiesResponses(t *testing.T) {
	cases := []struct {
		name    string
		status  int
		body    string
		want    Verdict
		wantFor time.Duration
	}{
		{"204 accepted", http.StatusNoContent, "", VerdictDelivered, 0},
		{"200 accepted", http.StatusOK, `{}`, VerdictDelivered, 0},
		// A deleted webhook. The clan can fix this, but only if we stop retrying and tell them.
		{"404 webhook deleted", http.StatusNotFound, `{"message":"Unknown Webhook"}`, VerdictDead, 0},
		{"401 bad token", http.StatusUnauthorized, ``, VerdictDead, 0},
		{"403 forbidden", http.StatusForbidden, ``, VerdictDead, 0},
		// Our payload is wrong; the webhook is fine. Marking it dead here would hide a working
		// integration behind one bad embed.
		{"400 bad payload", http.StatusBadRequest, `{"message":"Invalid Form Body"}`, VerdictDrop, 0},
		{"413 too large", http.StatusRequestEntityTooLarge, ``, VerdictDrop, 0},
		{"500 transient", http.StatusInternalServerError, ``, VerdictRetry, 0},
		{"502 transient", http.StatusBadGateway, ``, VerdictRetry, 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()

			res := NewSender("test").Send(context.Background(), srv.URL, json.RawMessage(`{"content":"hi"}`))
			if res.Verdict != tc.want {
				t.Errorf("verdict = %v, want %v (err: %v)", res.Verdict, tc.want, res.Err)
			}
			if res.Status != tc.status {
				t.Errorf("status = %d, want %d", res.Status, tc.status)
			}
		})
	}
}

// Discord reports its backoff two different ways depending on which edge answers, and getting this
// wrong means either hammering a rate-limited webhook or stalling a queue far longer than asked.
func TestRetryAfterPrefersTheJsonBody(t *testing.T) {
	t.Run("fractional seconds in the body", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Retry-After", "1") // whole seconds, less precise
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"retry_after":0.375,"global":false}`))
		}))
		defer srv.Close()

		res := NewSender("test").Send(context.Background(), srv.URL, json.RawMessage(`{}`))
		if res.Verdict != VerdictRetry {
			t.Fatalf("verdict = %v, want retry", res.Verdict)
		}
		// The body's 0.375s must win over the header's rounded 1s.
		if want := 375 * time.Millisecond; res.RetryAfter != want {
			t.Errorf("retryAfter = %v, want %v (body should beat header)", res.RetryAfter, want)
		}
	})

	t.Run("header only", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Retry-After", "3")
			w.WriteHeader(http.StatusTooManyRequests)
		}))
		defer srv.Close()

		res := NewSender("test").Send(context.Background(), srv.URL, json.RawMessage(`{}`))
		if res.RetryAfter != 3*time.Second {
			t.Errorf("retryAfter = %v, want 3s", res.RetryAfter)
		}
	})

	t.Run("no instruction at all", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusTooManyRequests)
		}))
		defer srv.Close()

		res := NewSender("test").Send(context.Background(), srv.URL, json.RawMessage(`{}`))
		// Must still be positive, or the retry fires immediately and 429s again.
		if res.RetryAfter <= 0 {
			t.Errorf("retryAfter = %v, want a positive default", res.RetryAfter)
		}
	})
}

func TestSendPostsTheExactPayload(t *testing.T) {
	// The payload is built by Anvil.Site and must arrive byte-identical — Forge does not know what
	// an embed is and must never reserialise one.
	want := `{"embeds":[{"title":"Tile complete","color":16766720}]}`
	var got string
	var contentType string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 4096)
		n, _ := r.Body.Read(buf)
		got = string(buf[:n])
		contentType = r.Header.Get("Content-Type")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	res := NewSender("test").Send(context.Background(), srv.URL, json.RawMessage(want))
	if res.Verdict != VerdictDelivered {
		t.Fatalf("verdict = %v, want delivered", res.Verdict)
	}
	if got != want {
		t.Errorf("posted %s, want %s", got, want)
	}
	if contentType != "application/json" {
		t.Errorf("content-type = %q, want application/json", contentType)
	}
}

func TestBackoffGrowsAndCaps(t *testing.T) {
	prev := time.Duration(0)
	for attempts := 1; attempts <= 10; attempts++ {
		got := Backoff(attempts)
		if got < prev {
			t.Errorf("attempt %d: backoff %v shrank from %v", attempts, got, prev)
		}
		if got > 15*time.Minute {
			t.Errorf("attempt %d: backoff %v exceeds the 15m cap", attempts, got)
		}
		prev = got
	}

	// The whole retry schedule should span an outage without keeping stale messages alive for
	// hours — a tile completion posted 90 minutes late is worse than not posted.
	var total time.Duration
	for i := 1; i <= MaxAttempts; i++ {
		total += Backoff(i)
	}
	t.Logf("%d attempts span %v", MaxAttempts, total)
	if total > time.Hour {
		t.Errorf("retry schedule spans %v, want under 1h", total)
	}
}

func TestBucketHidesTheWebhookUrl(t *testing.T) {
	// The bucket shows up in logs and metrics, and a webhook URL is a credential — anyone holding
	// it can post to the channel as the clan.
	url := "https://discord.com/api/webhooks/123456789/super-secret-token-value"
	bucket := BucketFor(url)

	if bucket == url {
		t.Fatal("bucket is the raw URL")
	}
	for _, leak := range []string{"super-secret-token-value", "123456789", "discord.com"} {
		if contains(bucket, leak) {
			t.Errorf("bucket %q leaks %q", bucket, leak)
		}
	}
	// Same URL must map to the same bucket, or per-webhook rate limiting does nothing.
	if BucketFor(url) != bucket {
		t.Error("bucket is not stable for the same URL")
	}
	if BucketFor(url+"x") == bucket {
		t.Error("different URLs share a bucket")
	}
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
