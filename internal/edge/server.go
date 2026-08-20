package edge

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// MaxIngestBody caps a single push. Generous enough for a full stats blob, small enough that a
// malformed or hostile client cannot make us buffer megabytes per request.
const MaxIngestBody = 512 << 10 // 512 KiB

// SeenInterval is how often a token's last_seen_at is refreshed. A 30-second poll from every
// install must not become a write per request.
const SeenInterval = 5 * time.Minute

// Server handles the plugin's HTTP surface.
type Server struct {
	Store *Store
	Log   *slog.Logger

	// Counters for the run log. The ratio of these is the whole justification for the read path
	// existing: if not-modified vastly exceeds modified, extraction paid for itself.
	notModified atomic.Int64
	served      atomic.Int64
	ingested    atomic.Int64
	duplicates  atomic.Int64
}

// Routes returns the plugin edge's handlers.
func (s *Server) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/plugin/config", s.handleConfig)
	mux.HandleFunc("/api/plugin/board", s.handleBoard)
	mux.HandleFunc("/api/plugin/ingest", s.handleIngest)
	mux.HandleFunc("/api/plugin/heartbeat", s.handleHeartbeat)
	return mux
}

// Stats returns counters for the run log and resets them.
func (s *Server) Stats() (served, notModified, ingested, duplicates int64) {
	return s.served.Swap(0), s.notModified.Swap(0), s.ingested.Swap(0), s.duplicates.Swap(0)
}

// ─────────────────────────────────────────────────────────────────────────────────────────────────
// Read path
// ─────────────────────────────────────────────────────────────────────────────────────────────────

func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	caller := s.authed(w, r)
	if caller == nil {
		return
	}
	s.servePayload(w, r, func(ctx context.Context) (*Payload, error) {
		return s.Store.BoundPayload(ctx, caller.TokenHash, "config")
	})

	// Liveness is a side effect of the poll, and cheap because the update is rate-limited in SQL.
	// Done after the response so it never adds latency to the hot path.
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := s.Store.TouchSeen(ctx, caller.TokenHash, SeenInterval); err != nil {
			s.Log.Debug("touching credential", "error", err)
		}
	}()
}

func (s *Server) handleBoard(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Anonymous preview path, mirroring the Site's `/board?eventId=`: any member can look at an
	// upcoming board, or a live one they are not competing in, without a team scope.
	if raw := r.URL.Query().Get("eventId"); raw != "" {
		eventID, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || eventID <= 0 {
			writeJSONError(w, http.StatusBadRequest, "Invalid eventId")
			return
		}
		s.servePayload(w, r, func(ctx context.Context) (*Payload, error) {
			return s.Store.PublicPayload(ctx, "board:event:"+strconv.FormatInt(eventID, 10))
		})
		return
	}

	caller := s.authed(w, r)
	if caller == nil {
		return
	}
	s.servePayload(w, r, func(ctx context.Context) (*Payload, error) {
		return s.Store.BoundPayload(ctx, caller.TokenHash, "board")
	})
}

// servePayload is the whole read path: look up bytes, compare ETags, answer.
func (s *Server) servePayload(w http.ResponseWriter, r *http.Request, load func(context.Context) (*Payload, error)) {
	payload, err := load(r.Context())
	if errors.Is(err, ErrNotFound) {
		// Nothing bound is not an error condition — it is the ordinary state of a player who is not
		// enrolled anywhere right now. 404 with a shape the plugin already understands.
		writeJSONError(w, http.StatusNotFound, "No payload available")
		return
	}
	if err != nil {
		s.Log.Error("loading payload", "path", r.URL.Path, "error", err)
		writeJSONError(w, http.StatusInternalServerError, "Internal error")
		return
	}

	w.Header().Set("ETag", payload.ETag)
	// Caches must key on the token, and a bearer-authenticated response must not be stored by a
	// shared proxy at all.
	w.Header().Set("Cache-Control", "private, no-cache")
	w.Header().Set("Vary", "Authorization, Accept-Encoding")

	// The common case by a wide margin: the plugin polls every 30 seconds and almost nothing has
	// changed. A 304 costs one indexed lookup and no body.
	if matchesETag(r.Header.Get("If-None-Match"), payload.ETag) {
		s.notModified.Add(1)
		w.WriteHeader(http.StatusNotModified)
		return
	}

	s.served.Add(1)
	w.Header().Set("Content-Type", "application/json")

	body := payload.Body
	if payload.Encoding == "gzip" {
		if acceptsGzip(r.Header.Get("Accept-Encoding")) {
			// Stored pre-compressed, so the hot path never spends CPU producing identical bytes.
			w.Header().Set("Content-Encoding", "gzip")
		} else {
			decoded, err := gunzip(body)
			if err != nil {
				s.Log.Error("decompressing payload", "etag", payload.ETag, "error", err)
				writeJSONError(w, http.StatusInternalServerError, "Internal error")
				return
			}
			body = decoded
		}
	}

	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	if _, err := w.Write(body); err != nil {
		s.Log.Debug("writing payload", "error", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────────────────────────
// Write path
// ─────────────────────────────────────────────────────────────────────────────────────────────────

type ingestRequest struct {
	Kind      string          `json:"kind"`
	Payload   json.RawMessage `json:"payload"`
	DedupeKey string          `json:"dedupeKey"`
	// Events lets a client batch a session's worth of pushes into one request, which matters on a
	// flaky connection far more than it does for our load.
	Events []ingestRequest `json:"events"`
}

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	caller := s.authed(w, r)
	if caller == nil {
		return
	}

	var req ingestRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, MaxIngestBody)).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "Malformed body")
		return
	}

	batch := req.Events
	if len(batch) == 0 {
		batch = []ingestRequest{req}
	}

	accepted, duplicates := 0, 0
	for _, e := range batch {
		if strings.TrimSpace(e.Kind) == "" {
			writeJSONError(w, http.StatusBadRequest, "Each event needs a kind")
			return
		}
		payload := e.Payload
		if len(payload) == 0 {
			payload = json.RawMessage(`{}`)
		}
		stored, err := s.Store.Ingest(r.Context(), caller, e.Kind, payload, e.DedupeKey)
		if err != nil {
			s.Log.Error("ingesting event", "kind", e.Kind, "error", err)
			writeJSONError(w, http.StatusInternalServerError, "Internal error")
			return
		}
		if stored {
			accepted++
		} else {
			duplicates++
		}
	}

	s.ingested.Add(int64(accepted))
	s.duplicates.Add(int64(duplicates))

	// 202, not 200: the events are durably stored but nothing has been SCORED yet. Anvil.Site
	// consumes the table and decides what any of it means, so promising more than "we have it"
	// would be a lie the plugin might act on.
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]int{"accepted": accepted, "duplicates": duplicates})
}

func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	caller := s.authed(w, r)
	if caller == nil {
		return
	}
	if caller.AccountID == nil {
		// A linked token with no resolved player yet. Not an error — the Site links asynchronously.
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err := s.Store.Heartbeat(r.Context(), *caller.AccountID); err != nil {
		s.Log.Error("recording heartbeat", "error", err)
		writeJSONError(w, http.StatusInternalServerError, "Internal error")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ─────────────────────────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────────────────────────

// authed resolves the bearer token, writing a 401 and returning nil when it cannot.
func (s *Server) authed(w http.ResponseWriter, r *http.Request) *Caller {
	token := bearer(r.Header.Get("Authorization"))
	if token == "" {
		writeJSONError(w, http.StatusUnauthorized, "Unauthorized. Provide Authorization: Bearer <accountToken>")
		return nil
	}
	caller, err := s.Store.Authenticate(r.Context(), token)
	if err != nil {
		s.Log.Error("authenticating", "error", err)
		writeJSONError(w, http.StatusInternalServerError, "Internal error")
		return nil
	}
	if caller == nil {
		writeJSONError(w, http.StatusUnauthorized, "Unauthorized. Provide Authorization: Bearer <accountToken>")
		return nil
	}
	return caller
}

func bearer(header string) string {
	const prefix = "Bearer "
	if len(header) <= len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return ""
	}
	return strings.TrimSpace(header[len(prefix):])
}

// matchesETag implements If-None-Match against a single current tag.
//
// Handles the comma-separated list form and `*`, because a conditional request that we fail to
// recognise costs a full body — which on this path is the entire saving.
func matchesETag(ifNoneMatch, current string) bool {
	if ifNoneMatch == "" || current == "" {
		return false
	}
	for _, candidate := range strings.Split(ifNoneMatch, ",") {
		candidate = strings.TrimSpace(candidate)
		if candidate == "*" || candidate == current {
			return true
		}
		// Weak comparison: W/"x" and "x" are the same entity for this purpose, and some HTTP
		// stacks strip or add the prefix in transit.
		if strings.TrimPrefix(candidate, "W/") == strings.TrimPrefix(current, "W/") {
			return true
		}
	}
	return false
}

func acceptsGzip(header string) bool {
	for _, part := range strings.Split(header, ",") {
		if strings.EqualFold(strings.TrimSpace(strings.Split(part, ";")[0]), "gzip") {
			return true
		}
	}
	return false
}

func gunzip(compressed []byte) ([]byte, error) {
	zr, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, err
	}
	defer func() { _ = zr.Close() }()
	return io.ReadAll(zr)
}

// Gzip compresses a payload for storage. Exported because the Site's payload writer is the only
// thing that should ever produce these bytes, and a helper here keeps the two consistent.
func Gzip(body []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	if err != nil {
		return nil, err
	}
	if _, err := zw.Write(body); err != nil {
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func writeJSONError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}
