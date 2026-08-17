// Package config reads Forge's settings from the environment.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config is the whole of Forge's configuration. Deliberately small: anything that wants to be
// tuned per clan belongs in the Site's settings, not here.
type Config struct {
	DatabaseURL string

	// HiscoresRPS is the GLOBAL request rate to Jagex, shared by every worker in the process.
	//
	// This is the number that matters most in the entire service, and it is far smaller than
	// intuition suggests. Wise Old Man — which tracks a large fraction of the whole OSRS
	// playerbase — runs its production limiter at `{ max: 1, duration: 250 }`, i.e. FOUR requests
	// per second, globally, with no periodic all-player sweep at all. See docs/RATE_BUDGET.md.
	//
	// We default marginally above that because we additionally need to detect tile completions
	// mid-event for players without the plugin. Raising it further is a decision about someone
	// else's infrastructure that we do not pay for and cannot be granted more of by asking;
	// exceeding what they tolerate blocks the box's IP, which takes tracking down for every clan
	// at once. Exhaust plugin adoption, a wider ladder, and boundary/on-demand polling first.
	HiscoresRPS float64

	// HiscoresBurst lets short bursts through while holding the long-run average at HiscoresRPS.
	HiscoresBurst int

	// Workers is how many fetches are in flight at once. Bounded by the rate limiter regardless,
	// so this only needs to be high enough that slow responses do not idle the budget.
	Workers int

	// ClaimBatch is how many players one claim query leases at a time.
	ClaimBatch int

	// ClaimLease is how long a claimed player is held before returning to the queue. A crashed
	// worker costs its players this much delay and nothing else.
	ClaimLease time.Duration

	// TickInterval is how often the runner looks for due players. Not the polling cadence — that
	// is per player, from the ladder.
	TickInterval time.Duration

	// UserAgent identifies us to Jagex. Being identifiable is cheap insurance: an operator who can
	// see who we are can ask us to slow down instead of simply blocking us.
	UserAgent string

	// DryRun runs the whole sweep — claim, fetch, decide — and writes nothing but the run log.
	// The way to observe real behaviour and real request rates against production data before
	// letting it touch a row.
	DryRun bool

	// HTTPAddr serves health and metrics.
	HTTPAddr string
}

// Load reads configuration from the environment, applying defaults sized for a single box.
func Load() (*Config, error) {
	c := &Config{
		DatabaseURL:   os.Getenv("DATABASE_URL"),
		HiscoresRPS:   envFloat("FORGE_HISCORES_RPS", 5),
		HiscoresBurst: envInt("FORGE_HISCORES_BURST", 10),
		Workers:       envInt("FORGE_WORKERS", 8),
		ClaimBatch:    envInt("FORGE_CLAIM_BATCH", 200),
		ClaimLease:    envDuration("FORGE_CLAIM_LEASE", 5*time.Minute),
		TickInterval:  envDuration("FORGE_TICK_INTERVAL", 10*time.Second),
		UserAgent:     envStr("FORGE_USER_AGENT", "Anvil.Forge/1.0 (+https://anvilosrs.com; contact@anvilosrs.com)"),
		DryRun:        envBool("FORGE_DRY_RUN", false),
		HTTPAddr:      envStr("FORGE_HTTP_ADDR", ":8080"),
	}

	if c.DatabaseURL == "" {
		return nil, fmt.Errorf("DATABASE_URL is required")
	}
	if c.HiscoresRPS <= 0 {
		return nil, fmt.Errorf("FORGE_HISCORES_RPS must be positive, got %v", c.HiscoresRPS)
	}
	if c.Workers < 1 {
		return nil, fmt.Errorf("FORGE_WORKERS must be at least 1, got %d", c.Workers)
	}
	// A lease shorter than the fetch it protects would hand the same player to a second worker
	// while the first is still mid-request, doubling our spend on exactly the accounts that are
	// already slow.
	if c.ClaimLease < time.Minute {
		return nil, fmt.Errorf("FORGE_CLAIM_LEASE must be at least 1m, got %v", c.ClaimLease)
	}
	return c, nil
}

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseFloat(v, 64); err == nil {
			return n
		}
	}
	return def
}

func envBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
