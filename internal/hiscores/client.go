package hiscores

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// BaseURL is the official OSRS hiscores, main gamemode. The other tables are siblings of it.
const BaseURL = "https://secure.runescape.com/m=hiscore_oldschool"

// StatsPath is the official JSON endpoint's path, relative to a table's base URL.
const StatsPath = "/index_lite.json?player="

// Mode names one hiscores table. Values are what Anvil.Site writes into hiscores_fetches.mode.
type Mode string

const (
	ModeMain     Mode = "main"
	ModeIronman  Mode = "ironman"
	ModeHardcore Mode = "hardcore"
	ModeUltimate Mode = "ultimate"
)

// tableSuffix maps a mode onto its table's URL: m=hiscore_oldschool<suffix>/index_lite.json.
var tableSuffix = map[Mode]string{
	ModeMain:     "",
	ModeIronman:  "_ironman",
	ModeHardcore: "_hardcore_ironman",
	ModeUltimate: "_ultimate",
}

// ErrUnknownMode is returned for a mode Forge has no table for. Terminal for that request.
var ErrUnknownMode = errors.New("unknown hiscores mode")

// Outcome classifies a fetch so the Site can react to a terminal miss differently from a blip.
type Outcome int

const (
	// OutcomeOK — the endpoint answered with a stats body.
	OutcomeOK Outcome = iota
	// OutcomeUnranked — the hiscores 404'd, or the RSN cannot be looked up at all. Terminal.
	OutcomeUnranked
	// OutcomeTransient — network, timeout, rate limit, or a body that is not the endpoint's shape.
	OutcomeTransient
)

// String is the value stored in hiscores_fetches.outcome.
func (o Outcome) String() string {
	switch o {
	case OutcomeOK:
		return "ok"
	case OutcomeUnranked:
		return "unranked"
	default:
		return "transient"
	}
}

// Result is a tagged fetch outcome. Body is set only when Outcome is OutcomeOK.
//
// Forge deliberately does not parse the body. Anvil.Site parses it with the same code its in-app
// fetch uses, so there is no second parser here to fall out of step when a boss is added.
type Result struct {
	Outcome Outcome
	Body    []byte
	// Err carries the underlying cause for logging. Never nil unless Outcome is OutcomeOK.
	Err error
}

// ErrInvalidRsn is returned for names that cannot be looked up at all.
var ErrInvalidRsn = errors.New("rsn is not a valid OSRS account name")

// Client reads the official hiscores. Safe for concurrent use.
type Client struct {
	HTTP    *http.Client
	BaseURL string
	// UserAgent identifies us to Jagex. Being identifiable is the cheapest insurance against a
	// blanket block: an operator who can see who we are can ask us to slow down instead.
	UserAgent string
	// Retries is the number of ATTEMPTS per fetch, not retries after the first (2 = one retry).
	Retries int
	// RetryDelay is the pause between attempts.
	RetryDelay time.Duration
}

// NewClient returns a client with the same timeout and retry shape as the Site's
// fetchSnapshotWithRetry: an 8s per-attempt timeout and one retry 1.5s later.
func NewClient(userAgent string) *Client {
	return &Client{
		HTTP: &http.Client{
			Timeout: 8 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        64,
				MaxIdleConnsPerHost: 32,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		BaseURL:    BaseURL,
		UserAgent:  userAgent,
		Retries:    2,
		RetryDelay: 1500 * time.Millisecond,
	}
}

// statsShape is the minimum a body must have to be worth handing to the Site.
type statsShape struct {
	Skills     []json.RawMessage `json:"skills"`
	Activities []json.RawMessage `json:"activities"`
}

// Fetch reads one player's raw stats body from one table, retrying once on a transient failure.
//
// A terminal outcome short-circuits: a 404 or an unlookupable name fails identically on a second
// attempt, and spending the retry to confirm that is how renamed accounts eat the budget.
func (c *Client) Fetch(ctx context.Context, rsn string, mode Mode) Result {
	clean := SanitizeRsn(rsn)
	if !IsPlausibleRsn(clean) {
		return Result{Outcome: OutcomeUnranked, Err: fmt.Errorf("%w: %q", ErrInvalidRsn, rsn)}
	}
	suffix, ok := tableSuffix[mode]
	if !ok {
		return Result{Outcome: OutcomeUnranked, Err: fmt.Errorf("%w: %q", ErrUnknownMode, mode)}
	}

	attempts := c.Retries
	if attempts < 1 {
		attempts = 1
	}

	var last error
	for attempt := 1; attempt <= attempts; attempt++ {
		body, err := c.fetchOnce(ctx, clean, suffix)
		if err == nil {
			return Result{Outcome: OutcomeOK, Body: body}
		}
		last = err
		if errors.Is(err, errNotFound) {
			return Result{Outcome: OutcomeUnranked, Err: err}
		}
		if ctx.Err() != nil {
			return Result{Outcome: OutcomeTransient, Err: ctx.Err()}
		}
		if attempt < attempts {
			select {
			case <-ctx.Done():
				return Result{Outcome: OutcomeTransient, Err: ctx.Err()}
			case <-time.After(c.RetryDelay):
			}
		}
	}
	return Result{Outcome: OutcomeTransient, Err: last}
}

var errNotFound = errors.New("player not found")

func (c *Client) fetchOnce(ctx context.Context, rsn, suffix string) ([]byte, error) {
	endpoint := c.BaseURL + suffix + StatsPath + url.QueryEscape(rsn)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", c.UserAgent)
	req.Header.Set("Accept", "application/json")

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		// Drain before closing so the connection returns to the idle pool.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
		_ = resp.Body.Close()
	}()

	switch {
	case resp.StatusCode == http.StatusNotFound:
		return nil, fmt.Errorf("%w: %s", errNotFound, rsn)
	case resp.StatusCode == http.StatusTooManyRequests:
		return nil, fmt.Errorf("hiscores rate limited (429) for %s", rsn)
	case resp.StatusCode >= 400:
		return nil, fmt.Errorf("hiscores returned %d for %s", resp.StatusCode, rsn)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("reading hiscores response for %s: %w", rsn, err)
	}
	// Jagex occasionally answers 200 with an HTML error page. Checked here so the Site is never
	// handed something its parser will choke on, and so the retry gets a second chance at it.
	var shape statsShape
	if err := json.NewDecoder(bytes.NewReader(body)).Decode(&shape); err != nil || len(shape.Skills) == 0 {
		return nil, fmt.Errorf("hiscores response for %s is not a stats body", rsn)
	}
	return body, nil
}
