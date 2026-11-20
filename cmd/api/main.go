// SafePath Live — Production API Entry Point.
//
// This file orchestrates the lifecycle of the entire 100k RPS service:
//  1. Initializes structured JSON logging (slog).
//  2. Loads configuration settings from environment variables.
//  3. Sets up PostgreSQL (durable storage) and Redis (ultra-fast in-memory path) connection pools.
//  4. Launches the Async Batch Flusher (consuming Redis Stream -> bulk binary pgx.CopyFrom).
//  5. Wires the in-memory SSE BroadcastHub and rate limiters.
//  6. Attaches zero-allocation Prometheus metrics and panic recovery middlewares.
//  7. Starts a hardened HTTP server with tuned network timeouts.
//  8. Handles OS termination signals (SIGINT/SIGTERM) with graceful multi-stage draining.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/rueidis"

	"safepath-live/internal/config"
	"safepath-live/internal/handler"
	"safepath-live/internal/middleware"
	"safepath-live/internal/repository"
	"safepath-live/internal/service"
)

func main() {
	// ── 1. Structured JSON Logging ──
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// ── 2. Configuration ──
	cfg := config.Load()
	logger.Info("application starting",
		"port", cfg.Port,
		"redis_url_set", cfg.RedisURL != "",
		"redis_addr", cfg.RedisAddr,
		"pprof_enabled", cfg.EnablePprof,
		"flush_batch_size", cfg.FlushBatchSize,
		"flush_interval", cfg.FlushInterval,
	)

	// Root application context cancelled upon shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ── 3. PostgreSQL Connection Pool ──
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Error("failed to initialize postgres connection pool", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	// Test database connectivity on boot
	if err := pool.Ping(ctx); err != nil {
		logger.Warn("postgres ping failed (ensure database is up before creating trips)", "error", err)
	} else {
		logger.Info("postgres connection established successfully")
	}

	// ── 4. Redis Client (rueidis with Auto-Pipelining & Cloud TLS Support) ──
	var rdbOpt rueidis.ClientOption
	if cfg.RedisURL != "" {
		parsedOpt, parseErr := rueidis.ParseURL(cfg.RedisURL)
		if parseErr != nil {
			logger.Error("failed to parse REDIS_URL", "error", parseErr)
			os.Exit(1)
		}
		parsedOpt.DisableCache = true
		rdbOpt = parsedOpt
	} else {
		rdbOpt = rueidis.ClientOption{
			InitAddress: []string{cfg.RedisAddr},
		}
	}

	rdb, err := rueidis.NewClient(rdbOpt)
	if err != nil {
		logger.Error("failed to initialize redis client", "error", err)
		os.Exit(1)
	}
	defer rdb.Close()

	// Test Redis connectivity on boot
	pingCmd := rdb.B().Ping().Build()
	if err := rdb.Do(ctx, pingCmd).Error(); err != nil {
		logger.Warn("redis ping failed (ensure Redis is running before pinging)", "error", err)
	} else {
		logger.Info("redis connection established successfully")
	}

	// ── 5. Repositories & Internal Services ──
	pgRepo := repository.NewPostgresRepo(pool)
	redisRepo := repository.NewRedisRepo(rdb, logger)
	broadcastHub := service.NewBroadcastHub(rdb, logger)

	// ── 6. Async Batch Flusher (Zero Postgres on Hot Ping Path) ──
	// Background consumer reading coordinates from Redis Stream `stream:pings`
	// and binary-copying batches to PostgreSQL via pgx.CopyFrom.
	flusherCfg := service.DefaultBatchFlusherConfig()
	if cfg.FlushBatchSize > 0 {
		flusherCfg.BatchSize = cfg.FlushBatchSize
	}
	if cfg.FlushInterval > 0 {
		flusherCfg.FlushInterval = cfg.FlushInterval
	}
	flusher := service.NewBatchFlusher(rdb, pgRepo, flusherCfg, logger)

	flusherCtx, flusherCancel := context.WithCancel(ctx)
	defer flusherCancel()

	go func() {
		if err := flusher.Start(flusherCtx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("batch flusher failed", "error", err)
		}
	}()

	// ── 7. Distributed Rate Limiter ──
	var ipLimiter, pingLimiter func(http.Handler) http.Handler
	if cfg.DisableRateLimiter {
		logger.Warn("rate limiting disabled (BENCHMARK MODE)")
		passthrough := func(next http.Handler) http.Handler { return next }
		ipLimiter = passthrough
		pingLimiter = passthrough
	} else {
		rateLimiter := middleware.NewRateLimiter(rdb, logger)
		ipLimiter = rateLimiter.LimitByIP(60, 1.0)           // 60 req/min for creating trips
		pingLimiter = rateLimiter.LimitByShareCode(20, 10.0) // 10 pings/sec per vehicle (burst 20)
	}

	// ── 8. HTTP Handlers ──
	tripHandler := handler.NewTripHandler(pgRepo, redisRepo, logger)
	streamHandler := handler.NewStreamHandler(broadcastHub, redisRepo, pgRepo, logger)
	webHandler := handler.NewWebHandler()

	// ── 9. HTTP Routing (Standard Library Go 1.22+) ──
	mux := http.NewServeMux()

	// Embedded Web UI & Health Probe
	mux.HandleFunc("GET /", webHandler.Index)
	mux.HandleFunc("GET /healthz", webHandler.Healthz)

	// Prometheus Live Metrics (Scraped by Prometheus / Grafana agent)
	mux.HandleFunc("GET /metrics", middleware.GlobalMetrics.ExposePrometheus)

	// Trip CRUD & Hot-Path Ingestion
	mux.Handle("POST /v1/trips", ipLimiter(http.HandlerFunc(tripHandler.Create)))
	mux.Handle("POST /v1/trips/{code}/ping", pingLimiter(http.HandlerFunc(tripHandler.Ping)))
	mux.HandleFunc("GET /v1/trips/{code}", tripHandler.Get)
	mux.HandleFunc("POST /v1/trips/{code}/end", tripHandler.End)

	// Real-Time Server-Sent Events (SSE) Stream
	mux.HandleFunc("GET /v1/trips/{code}/live", streamHandler.Live)

	// ── 10. Global High-Throughput Middleware Pipeline ──
	//  1. Recoverer: Prevents dropped sockets by intercepting panics and logging stack traces.
	//  2. RequestID: Attaches a unique 32-char hex trace ID to every request.
	//  3. GlobalMetrics: Lock-free atomic counters for RPS, in-flight concurrency, and latency histogram.
	//  4. BodyLimit: Caps inbound bodies to 4KB to eliminate memory exhaustion vectors.
	var rootHandler http.Handler = mux
	rootHandler = middleware.BodyLimit(4096)(rootHandler)
	rootHandler = middleware.GlobalMetrics.Middleware(rootHandler)
	rootHandler = middleware.RequestID(rootHandler)
	rootHandler = middleware.Recoverer(logger)(rootHandler)

	// ── 11. Optional pprof Diagnostic Profiler ──
	if cfg.EnablePprof {
		pprofMux := http.NewServeMux()
		pprofMux.HandleFunc("GET /debug/pprof/", pprof.Index)
		pprofMux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
		pprofMux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
		pprofMux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
		pprofMux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
		go func() {
			logger.Info("pprof diagnostic server started", "addr", ":6060")
			if err := http.ListenAndServe(":6060", pprofMux); err != nil {
				logger.Error("pprof server error", "error", err)
			}
		}()
	}

	// ── 12. Hardened HTTP Server Configuration ──
	// ReadHeaderTimeout (2s): Mitigates Slowloris attacks.
	// ReadTimeout (5s): Caps request body read duration.
	// IdleTimeout (60s): Keeps TCP connections warm for high-speed reuse.
	// WriteTimeout (0): Required for persistent Server-Sent Events (SSE) connections.
	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           rootHandler,
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		logger.Info("HTTP API server listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server exited unexpectedly", "error", err)
			os.Exit(1)
		}
	}()

	// ── 13. Graceful Multi-Stage Shutdown ──
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	sig := <-sigCh
	logger.Info("shutdown signal intercepted; draining server", "signal", sig.String())

	// Step A: Stop accepting new HTTP requests
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer shutCancel()

	if err := srv.Shutdown(shutCtx); err != nil {
		logger.Error("error during HTTP server shutdown", "error", err)
	}

	// Step B: Stop batch flusher (flushes remaining buffered pings to Postgres)
	flusherCancel()
	time.Sleep(300 * time.Millisecond)

	logger.Info("SafePath Live API shutdown completed cleanly")
}
