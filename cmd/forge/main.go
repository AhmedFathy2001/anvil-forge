// Command forge runs Anvil's data plane: the hiscores sweep today, plugin ingest and the Discord
// fan-out queue as they are extracted.
//
// It deliberately does NOT evaluate anything. Whether a snapshot completes a tile, crosses a
// milestone, or moves a weekly standing is decided by Anvil.Site, which consumes the player_events
// outbox. See docs/BOUNDARY.md for why that line is where it is.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/time/rate"

	"github.com/anvilosrs/forge/internal/config"
	"github.com/anvilosrs/forge/internal/hiscores"
	"github.com/anvilosrs/forge/internal/store"
	"github.com/anvilosrs/forge/internal/sweep"
)

func main() {
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

	if err := db.EnsurePartitions(ctx); err != nil {
		return err
	}

	runner := &sweep.Runner{
		Store:        db,
		Hiscores:     hiscores.NewClient(cfg.UserAgent),
		Log:          log,
		Limiter:      rate.NewLimiter(rate.Limit(cfg.HiscoresRPS), cfg.HiscoresBurst),
		Workers:      cfg.Workers,
		ClaimBatch:   cfg.ClaimBatch,
		ClaimLease:   cfg.ClaimLease,
		TickInterval: cfg.TickInterval,
	}

	srv := healthServer(cfg.HTTPAddr, db, log)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("health server failed", "error", err)
		}
	}()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	// Keep the snapshot partitions ahead of the calendar. An insert into an uncovered range is a
	// hard error, so this is not a tidiness job — miss it and every write fails at midnight on the
	// first of the month.
	go func() {
		t := time.NewTicker(24 * time.Hour)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := db.EnsurePartitions(ctx); err != nil {
					log.Error("ensuring partitions", "error", err)
				}
			}
		}
	}()

	log.Info("forge starting",
		"hiscoresRps", cfg.HiscoresRPS,
		"workers", cfg.Workers,
		"claimBatch", cfg.ClaimBatch,
		"dryRun", cfg.DryRun,
		"httpAddr", cfg.HTTPAddr,
	)
	if cfg.DryRun {
		log.Warn("DRY RUN: polling for real, writing nothing but the run log")
	}

	return runner.Run(ctx)
}

func healthServer(addr string, db *store.Store, log *slog.Logger) *http.Server {
	mux := http.NewServeMux()

	// Liveness: the process is up. Deliberately does not touch the database — a health check that
	// fails when Postgres blips gets the container killed exactly when restarting is least useful.
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
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

	return &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
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
