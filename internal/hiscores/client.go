package hiscores

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// BaseURL is the official OSRS hiscores, main gamemode.
const BaseURL = "https://secure.runescape.com/m=hiscore_oldschool/"

// Bosses that are LIVE on the official hiscores but missing from the osrs-json-hiscores release
// Anvil.Site pins. The lib's parser silently drops activities it does not know, which zeroes
// BOTW and boss-tile tracking for them — so a new boss is one line here away from being tracked.
//
// Mirrors EXTRA_HISCORE_BOSSES in Anvil.Site's src/lib/hiscores.ts. KEEP THE TWO IN SYNC: if only
// one side knows a boss, the sweep and the Site disagree about whether a tile scored. Prune from
// both once a lib bump ships it (harmless to leave — the generated table wins when both know it).
var extraBosses = map[string]string{
	"Maggot King": "maggotKing",
	"Mad Angel":   "madAngel",
}

// Outcome classifies a fetch so the scheduler can react to a terminal miss differently from a blip.
type Outcome int

const (
	// OutcomeOK — a snapshot was read.
	OutcomeOK Outcome = iota
	// OutcomeUnranked — the hiscores 404'd, or the RSN cannot be looked up at all. Terminal:
	// stop polling and let a re-probe or a human lift the player back.
	OutcomeUnranked
	// OutcomeTransient — network, timeout, rate limit, or parse failure. Try again later.
	OutcomeTransient
)

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

// Result is a tagged fetch outcome. Snapshot is non-nil only when Outcome is OutcomeOK.
type Result struct {
	Outcome  Outcome
	Snapshot *Snapshot
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
				MaxIdleConns:        512,
				MaxIdleConnsPerHost: 256,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		BaseURL:    BaseURL,
		UserAgent:  userAgent,
		Retries:    2,
		RetryDelay: 1500 * time.Millisecond,
	}
}

// rawStats is the official endpoint's response shape.
type rawStats struct {
	Skills []struct {
		Name  string `json:"name"`
		Rank  int64  `json:"rank"`
		Level int64  `json:"level"`
		Xp    int64  `json:"xp"`
	} `json:"skills"`
	Activities []struct {
		Name  string `json:"name"`
		Rank  int64  `json:"rank"`
		Score int64  `json:"score"`
	} `json:"activities"`
}

// Fetch reads one player's snapshot, retrying once on a transient failure.
//
// A terminal outcome short-circuits: a 404 or an unlookupable name fails identically on a second
// attempt, and spending the retry (plus a rate-limit token) to confirm that is how a few thousand
// renamed accounts turn into a meaningful slice of the polling budget.
func (c *Client) Fetch(ctx context.Context, rsn string) Result {
	clean := SanitizeRsn(rsn)
	if !IsPlausibleRsn(clean) {
		return Result{Outcome: OutcomeUnranked, Err: fmt.Errorf("%w: %q", ErrInvalidRsn, rsn)}
	}

	attempts := c.Retries
	if attempts < 1 {
		attempts = 1
	}

	var last error
	for attempt := 1; attempt <= attempts; attempt++ {
		snap, err := c.fetchOnce(ctx, clean)
		if err == nil {
			return Result{Outcome: OutcomeOK, Snapshot: snap}
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

func (c *Client) fetchOnce(ctx context.Context, rsn string) (*Snapshot, error) {
	endpoint := c.BaseURL + StatsPath + url.QueryEscape(rsn)

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
		// Drain before closing so the connection returns to the idle pool. At a few hundred
		// requests a second, leaking sockets instead is the difference between a steady worker
		// pool and one that runs out of file descriptors overnight.
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

	var raw rawStats
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&raw); err != nil {
		return nil, fmt.Errorf("decoding hiscores response for %s: %w", rsn, err)
	}
	return parse(&raw), nil
}

// parse maps the endpoint's by-display-name rows onto our canonical keys, exactly as the Site's
// parseJsonStats does — matching on a case-folded name because the numeric ids are NOT stable
// across game updates.
func parse(raw *rawStats) *Snapshot {
	skillByName := make(map[string]int, len(raw.Skills))
	for i, s := range raw.Skills {
		skillByName[strings.ToLower(s.Name)] = i
	}
	activityByName := make(map[string]int, len(raw.Activities))
	for i, a := range raw.Activities {
		activityByName[strings.ToLower(a.Name)] = i
	}

	activity := func(displayName string) ScoreEntry {
		if i, ok := activityByName[strings.ToLower(displayName)]; ok {
			return ScoreEntry{Rank: raw.Activities[i].Rank, Score: raw.Activities[i].Score}
		}
		return Unranked
	}

	snap := &Snapshot{
		Skills:       make(map[string]SkillEntry, len(SkillOrder)),
		Clues:        make(map[string]ScoreEntry, len(ClueOrder)),
		Bosses:       make(map[string]ScoreEntry, len(BossOrder)),
		BountyHunter: make(map[string]ScoreEntry, len(BountyHunterOrder)),
	}

	for _, key := range SkillOrder {
		if i, ok := skillByName[strings.ToLower(SkillNames[key])]; ok {
			snap.Skills[key] = SkillEntry{Rank: raw.Skills[i].Rank, Level: raw.Skills[i].Level, Xp: raw.Skills[i].Xp}
		} else {
			snap.Skills[key] = UnrankedSkill
		}
	}
	for _, key := range ClueOrder {
		snap.Clues[key] = activity(ClueNames[key])
	}
	for _, key := range BossOrder {
		snap.Bosses[key] = activity(BossNames[key])
	}

	// Bosses the pinned lib release does not know yet, merged by name. Never overwrites a key the
	// generated table already filled, so a lib bump silently takes over without a code change.
	for displayName, key := range extraBosses {
		if _, known := snap.Bosses[key]; known {
			continue
		}
		if i, ok := activityByName[strings.ToLower(displayName)]; ok {
			snap.Bosses[key] = ScoreEntry{Rank: raw.Activities[i].Rank, Score: raw.Activities[i].Score}
		}
	}

	for _, key := range BountyHunterOrder {
		snap.BountyHunter[key] = activity(BountyHunterNames[key])
	}
	snap.LeaguePoints = activity(ActivityLeaguePoints)
	snap.DeadmanPoints = activity(ActivityDeadmanPoints)
	snap.LastManStanding = activity(ActivityLastManStanding)
	snap.PvpArena = activity(ActivityPvpArena)
	snap.SoulWarsZeal = activity(ActivitySoulWarsZeal)
	snap.RiftsClosed = activity(ActivityRiftsClosed)
	snap.ColosseumGlory = activity(ActivityColosseumGlory)
	snap.CollectionsLogged = activity(ActivityCollectionsLogged)

	return snap
}
