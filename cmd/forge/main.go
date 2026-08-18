// Command forge runs Anvil's data plane: the hiscores sweep, the plugin edge, and the Discord
// delivery queue.
//
// It deliberately does NOT evaluate anything. Whether a snapshot completes a tile, whether a drop
// is worth announcing, what a weekly standing is — all decided by Anvil.Site, which consumes the
// outbox tables Forge writes. See docs/BOUNDARY.md for why that line is where it is.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/time/rate"

	"github.com/anvilosrs/forge/internal/config"
	"github.com/anvilosrs/forge/internal/discord"
	"github.com/anvilosrs/forge/internal/edge"
	"github.com/anvilosrs/forge/internal/hiscores"
	"github.com/anvilosrs/forge/internal/ratelimit"
	"github.com/anvilosrs/forge/internal/store"
	"github.com/anvilosrs/forge/internal/sweep"
)

// gitSHA is stamped at build time (-ldflags "-X main.gitSHA=..."). Reported on /health so a
// deploy can be verified from outside without shelling into the box — the question after every
// rollout is "is the new one actually running", and this is the cheapest possible answer.
var gitSHA = "dev"

func main() {
	// Self-probe mode for the container healthcheck. The image is distroless — no shell, no curl —
	// so the binary has to be able to check itself. Exits 0 if the local HTTP server answers.
	if len(os.Args) > 1 && os.Args[1] == "-healthcheck" {
		os.Exit(healthcheck())
	}

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel()}))
	slog.SetDefault(log)

	if err := run(log); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("forge exited", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := store.Open(ctx, cfg.DatabaseURL, cfg.DryRun)
	if err != nil {
		return err
	}
	defer db.Close()

	if cfg.EnableSweep {
		if err := db.EnsurePartitions(ctx); err != nil {
			return err
		}
	}

	mux := http.NewServeMux()
	registerHealth(mux, db)

	var edgeServer *edge.Server
	if cfg.EnableEdge {
		edgeServer = &edge.Server{Store: edge.NewStore(db.Pool()), Log: log}
		for pattern, handler := range routesOf(edgeServer) {
			mux.Handle(pattern, handler)
		}
	}

	srv := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: mux,
		// The plugin edge is public, so these are the guard against a slow or hostile client
		// occupying a connection indefinitely.
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server failed", "error", err)
		}
	}()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Info("forge starting",
		"sha", gitSHA,
		"sweep", cfg.EnableSweep,
		"edge", cfg.EnableEdge,
		"discord", cfg.EnableDiscord,
		"hiscoresRps", cfg.HiscoresRPS,
		"dryRun", cfg.DryRun,
		"httpAddr", cfg.HTTPAddr,
	)
	if cfg.DryRun {
		log.Warn("DRY RUN: polling for real, writing nothing but the run log")
	}

	var wg sync.WaitGroup

	if cfg.EnableSweep {
		runner := &sweep.Runner{
			Store:    db,
			Hiscores: hiscores.NewClient(cfg.UserAgent),
			Log:      log,
			// One limiter for the process. Per-worker limits compose into a total nobody chose,
			// which is exactly how a safe per-clan-container rate became an unsafe per-box one.
			Limiter:      rate.NewLimiter(rate.Limit(cfg.HiscoresRPS), cfg.HiscoresBurst),
			Workers:      cfg.Workers,
			ClaimBatch:   cfg.ClaimBatch,
			ClaimLease:   cfg.ClaimLease,
			TickInterval: cfg.TickInterval,
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := runner.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Error("sweep stopped", "error", err)
			}
		}()

		// Derive global player identity from the Site's per-clan memberships.
		//
		// Anvil.Site keeps membership per clan, so one account in three clans is three rows with
		// three independent polling states. Collapsing them is the largest single saving available
		// on the hiscores budget, and it has to run continuously because rosters change.
		wg.Add(1)
		go func() {
			defer wg.Done()
			reconcileLoop(ctx, db, cfg.ReconcileInterval, log)
		}()

		// Keep the snapshot partitions ahead of the calendar. Not tidiness: an insert into an
		// uncovered range is a hard error, so missing this fails every write at midnight on the
		// first of the month.
		wg.Add(1)
		go func() {
			defer wg.Done()
			everyDay(ctx, func() {
				if err := db.EnsurePartitions(ctx); err != nil {
					log.Error("ensuring partitions", "error", err)
				}
			})
		}()
	}

	if cfg.EnableDiscord {
		worker := &discord.Worker{
			Queue:  discord.NewQueue(db.Pool()),
			Sender: discord.NewSender(cfg.UserAgent),
			Log:    log,
			// Keyed by webhook because Discord limits per webhook. A global limiter would throttle
			// a quiet clan on account of a busy one and still exceed the limit on the busy one.
			Limiter:       ratelimit.NewKeyed(discord.WebhookRatePerSecond, discord.WebhookBurst, 15*time.Minute),
			Workers:       cfg.DiscordWorkers,
			ClaimBatch:    cfg.DiscordClaimBatch,
			ClaimLease:    cfg.DiscordClaimLease,
			TickEvery:     cfg.DiscordTickEvery,
			KeepDelivered: cfg.DiscordKeepDelivered,
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := worker.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Error("discord worker stopped", "error", err)
			}
		}()
	}

	if edgeServer != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reportEdge(ctx, edgeServer, log)
		}()
	}

	<-ctx.Done()
	wg.Wait()
	return ctx.Err()
}

// routesOf adapts the edge's mux into patterns we can mount alongside health.
func routesOf(s *edge.Server) map[string]http.Handler {
	inner := s.Routes()
	return map[string]http.Handler{
		"/api/plugin/": inner,
	}
}

func registerHealth(mux *http.ServeMux, db *store.Store) {
	// Liveness: the process is up. Deliberately does not touch the database — a health check that
	// fails when Postgres blips gets the container killed exactly when restarting is least useful.
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok", "sha": gitSHA})
	})

	// Readiness: can we actually do work? This one does check the database, because a Forge that
	// cannot reach Postgres should be taken out of rotation.
	mux.HandleFunc("/ready", func(w http.ResponseWriter, req *http.Request) {
		ctx, cancel := context.WithTimeout(req.Context(), 2*time.Second)
		defer cancel()
		w.Header().Set("Content-Type", "application/json")
		if err := db.Pool().Ping(ctx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "unready", "error": err.Error()})
			return
		}
		backlog, err := db.Backlog(ctx)
		if err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "unready", "error": err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "backlog": backlog})
	})
}

// reportEdge logs the edge's counters periodically.
//
// The ratio of 304s to 200s is the whole justification for extracting the read path: if not-modified
// vastly exceeds modified, the extraction is paying for itself and the number to watch is egress.
// If it does not, something is churning the ETag and the payload builder needs looking at.
func reportEdge(ctx context.Context, s *edge.Server, log *slog.Logger) {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			served, notModified, ingested, duplicates := s.Stats()
			if served+notModified+ingested+duplicates == 0 {
				continue
			}
			var hitRate float64
			if total := served + notModified; total > 0 {
				hitRate = float64(notModified) / float64(total)
			}
			log.Info("edge.minute",
				"served", served,
				"notModified", notModified,
				"notModifiedRate", round2(hitRate),
				"ingested", ingested,
				"duplicates", duplicates,
			)
		}
	}
}

// reconcileLoop keeps forge_players in step with the Site's rosters.
//
// Runs immediately then on an interval: a Forge that has just started with an empty mapping would
// otherwise sit idle until the first tick, which at a long interval looks exactly like a broken
// sweep.
func reconcileLoop(ctx context.Context, db *store.Store, every time.Duration, log *slog.Logger) {
	run := func() {
		r, err := db.Reconcile(ctx)
		if err != nil {
			if ctx.Err() == nil {
				log.Error("reconciling players", "error", err)
			}
			return
		}
		players, memberships, err := db.Fanout(ctx)
		if err != nil {
			log.Warn("reading fanout", "error", err)
		}
		// dedup is the number that says whether the global identity is earning its complexity —
		// and the honest denominator for any claim about the polling budget.
		dedup := 1.0
		if players > 0 {
			dedup = float64(memberships) / float64(players)
		}
		log.Info("reconcile",
			"playersUpserted", r.PlayersInserted,
			"linksUpserted", r.LinksInserted,
			"linksPruned", r.LinksPruned,
			"enrolled", r.Enrolled,
			"unenrolled", r.Unenrolled,
			"accounts", players,
			"memberships", memberships,
			"dedup", round2(dedup),
		)
	}

	run()
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			run()
		}
	}
}

func everyDay(ctx context.Context, fn func()) {
	t := time.NewTicker(24 * time.Hour)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			fn()
		}
	}
}

func round2(f float64) float64 { return float64(int(f*100+0.5)) / 100 }

// healthcheck hits our own /health and reports 0 for healthy, 1 otherwise.
//
// Deliberately checks LIVENESS, not readiness: a container killed because Postgres blipped is a
// container restarting at the exact moment restarting helps least.
func healthcheck() int {
	addr := os.Getenv("FORGE_HTTP_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	if strings.HasPrefix(addr, ":") {
		addr = "127.0.0.1" + addr
	}

	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get("http://" + addr + "/health")
	if err != nil {
		fmt.Fprintf(os.Stderr, "healthcheck: %v\n", err)
		return 1
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "healthcheck: status %d\n", resp.StatusCode)
		return 1
	}
	return 0
}

func logLevel() slog.Level {
	switch os.Getenv("FORGE_LOG_LEVEL") {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
