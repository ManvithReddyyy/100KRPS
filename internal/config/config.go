// Package config handles loading and parsing of all environment variables.
//
// If an environment variable is not explicitly provided, it falls back
// to sensible defaults so that developers can run the project locally
// without needing a complex configuration setup.
package config

import (
	"os"
	"strconv"
	"time"
)

// Config stores all the operational settings for the SafePath Live service.
// Each field can be customized using environment variables.
type Config struct {
	// Port is the HTTP port the web server listens on (e.g., "8081").
	Port string

	// DatabaseURL is the PostgreSQL connection string.
	// Supports local Postgres or cloud providers like Neon.tech with SSL:
	// "postgresql://user:pass@ep-xyz.aws.neon.tech/neondb?sslmode=require"
	DatabaseURL string

	// RedisURL is a full Redis connection string, supporting passwords and TLS:
	// e.g. "rediss://default:password@host:port" (Upstash / AWS ElastiCache)
	// If set, takes precedence over RedisAddr.
	RedisURL string

	// RedisAddr is the "host:port" address of a local plaintext Redis server.
	// Used as fallback when RedisURL is not set (default: "localhost:6379").
	RedisAddr string

	// EnablePprof turns on Go's internal profiling endpoints on port :6060.
	// Useful during load testing to capture CPU and memory flame graphs.
	EnablePprof bool

	// TripMaxExpiry is the maximum allowed duration for a trip (default: 12 hours).
	// Prevents orphaned or forgotten trips from consuming resources indefinitely.
	TripMaxExpiry time.Duration

	// TripMinExpiry is the minimum allowed duration for a trip (default: 5 minutes).
	TripMinExpiry time.Duration

	// FlushBatchSize is the number of GPS points accumulated before writing
	// a batch to PostgreSQL using binary COPY (pgx.CopyFrom). Default: 1000.
	FlushBatchSize int

	// FlushInterval is the maximum time to wait before flushing partial batches
	// to PostgreSQL, ensuring low data loss even during quiet periods. Default: 250ms.
	FlushInterval time.Duration

	// DisableRateLimiter turns off token-bucket throttling for high-throughput load testing.
	DisableRateLimiter bool
}

// Load reads all configuration values from the system environment.
// If any setting is missing or invalid, it gracefully falls back to local development defaults.
func Load() Config {
	return Config{
		// Default to HTTP port 8081 (8080 is often reserved by Windows NAT/Hyper-V)
		Port: envOr("PORT", "8081"),

		// Disable rate limiter during 100k benchmarks (default: false)
		DisableRateLimiter: envOr("DISABLE_RATE_LIMITER", "false") == "true",

		// Default local PostgreSQL or cloud Neon connection string
		DatabaseURL: envOr("DATABASE_URL", "postgres://safepath:safepath@localhost:5432/safepath?sslmode=disable"),

		// Cloud Redis URL (e.g. Upstash rediss://...)
		RedisURL: os.Getenv("REDIS_URL"),

		// Default local Redis instance on port 6379
		RedisAddr: envOr("REDIS_ADDR", "localhost:6379"),

		// Disabled by default to protect production environments
		EnablePprof: envOr("ENABLE_PPROF", "false") == "true",

		// Maximum trip duration: 12 hours (720 minutes)
		TripMaxExpiry: envDuration("TRIP_MAX_EXPIRY", 720*time.Minute),

		// Minimum trip duration: 5 minutes
		TripMinExpiry: envDuration("TRIP_MIN_EXPIRY", 5*time.Minute),

		// Flush up to 1,000 GPS points per database write
		FlushBatchSize: envInt("FLUSH_BATCH_SIZE", 1000),

		// Flush at least every 250 milliseconds so coordinates are saved promptly
		FlushInterval: envDuration("FLUSH_INTERVAL", 250*time.Millisecond),
	}
}

// envOr retrieves the value of an environment variable as a string.
// If the variable is empty or unset, it returns the provided fallback value.
func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// envDuration retrieves the value of an environment variable as a time.Duration
// (for example: "10s", "5m", "250ms"). If unset or invalid, it returns the fallback value.
func envDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}

// envInt retrieves the value of an environment variable as an integer.
// If unset or invalid, it returns the fallback value.
func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}
