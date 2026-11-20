// Package middleware provides HTTP middlewares for rate limiting, panic recovery,
// request tracing, and Prometheus metrics.
package middleware

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/redis/rueidis"
)

// ──────────────────────────────────────────────────────────────
// Atomic Redis Token Bucket Lua Script
//
// KEYS[1] = "ratelimit:{identifier}" (Redis Hash)
// ARGV[1] = capacity (max burst tokens)
// ARGV[2] = refill_rate (tokens added per second)
// ARGV[3] = now_timestamp (unix seconds with millisecond precision)
// ARGV[4] = requested_tokens (usually 1)
//
// Returns an array: [allowed (0 or 1), remaining_tokens, retry_after_seconds]
// ──────────────────────────────────────────────────────────────
var tokenBucketScript = rueidis.NewLuaScript(`
local key              = KEYS[1]
local capacity         = tonumber(ARGV[1])
local refill_rate      = tonumber(ARGV[2])
local now              = tonumber(ARGV[3])
local requested        = tonumber(ARGV[4])

-- Load state from hash
local data = redis.call('HMGET', key, 'tokens', 'last_updated')
local tokens = tonumber(data[1])
local last_updated = tonumber(data[2])

if not tokens then
    tokens = capacity
    last_updated = now
else
    -- Calculate tokens accumulated since last check
    local elapsed = math.max(0, now - last_updated)
    tokens = math.min(capacity, tokens + (elapsed * refill_rate))
    last_updated = now
end

if tokens >= requested then
    tokens = tokens - requested
    redis.call('HMSET', key, 'tokens', tokens, 'last_updated', last_updated)
    -- Expire key after bucket would fully refill from 0
    local ttl = math.ceil(capacity / refill_rate) * 2
    redis.call('EXPIRE', key, math.max(ttl, 60))
    return {1, math.floor(tokens), 0}
else
    local missing = requested - tokens
    local retry_after = math.ceil(missing / refill_rate)
    return {0, math.floor(tokens), retry_after}
end
`)

// RateLimiter coordinates distributed token-bucket rate limits across API instances.
type RateLimiter struct {
	client rueidis.Client
	log    *slog.Logger
}

// NewRateLimiter initializes the Redis-backed distributed rate limiter.
func NewRateLimiter(client rueidis.Client, log *slog.Logger) *RateLimiter {
	return &RateLimiter{
		client: client,
		log:    log,
	}
}

// Allow executes the atomic token bucket script in Redis.
// Returns (allowed, remainingTokens, retryAfterSeconds, error).
func (rl *RateLimiter) Allow(ctx context.Context, key string, capacity, refillRatePerSec float64) (bool, int64, int64, error) {
	if rl.client == nil {
		// If Redis is nil (e.g. unit tests), fail open
		return true, int64(capacity), 0, nil
	}

	nowSec := float64(time.Now().UnixNano()) / 1e9
	keys := []string{"ratelimit:" + key}
	args := []string{
		strconv.FormatFloat(capacity, 'f', -1, 64),
		strconv.FormatFloat(refillRatePerSec, 'f', -1, 64),
		strconv.FormatFloat(nowSec, 'f', 4, 64),
		"1", // 1 token per HTTP request
	}

	resp := tokenBucketScript.Exec(ctx, rl.client, keys, args)
	resArr, err := resp.ToArray()
	if err != nil {
		return false, 0, 0, fmt.Errorf("token bucket lua: %w", err)
	}
	if len(resArr) < 3 {
		return false, 0, 0, fmt.Errorf("unexpected lua result length: %d", len(resArr))
	}

	allowedInt, _ := resArr[0].ToInt64()
	remaining, _ := resArr[1].ToInt64()
	retryAfter, _ := resArr[2].ToInt64()

	return allowedInt == 1, remaining, retryAfter, nil
}

// LimitByIP creates an HTTP middleware that limits requests by client IP.
// E.g. capacity = 60, refillRate = 1.0 (60 req/min with burst up to 60).
func (rl *RateLimiter) LimitByIP(capacity, refillRatePerSec float64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ip := extractClientIP(r)
			key := "ip:" + ip

			allowed, remaining, retryAfter, err := rl.Allow(r.Context(), key, capacity, refillRatePerSec)
			if err != nil {
				rl.log.Warn("rate limiter error, failing open", "key", key, "error", err)
				next.ServeHTTP(w, r)
				return
			}

			w.Header().Set("X-RateLimit-Remaining", strconv.FormatInt(remaining, 10))

			if !allowed {
				writeRateLimitExceeded(w, retryAfter)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// LimitByShareCode limits ping requests by trip share code (e.g., 10 pings/sec max).
// Prevents malfunctioning GPS trackers or DDoS scripts from flooding a single trip.
func (rl *RateLimiter) LimitByShareCode(capacity, refillRatePerSec float64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Extract share_code from path /v1/trips/{code}/ping
			code := r.PathValue("code")
			if code == "" {
				// Fallback to IP if code is not present in URL path
				code = extractClientIP(r)
			}
			key := "trip:" + code

			allowed, remaining, retryAfter, err := rl.Allow(r.Context(), key, capacity, refillRatePerSec)
			if err != nil {
				rl.log.Warn("rate limiter error, failing open", "key", key, "error", err)
				next.ServeHTTP(w, r)
				return
			}

			w.Header().Set("X-RateLimit-Remaining", strconv.FormatInt(remaining, 10))

			if !allowed {
				writeRateLimitExceeded(w, retryAfter)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// writeRateLimitExceeded writes a clean RFC-compliant 429 Too Many Requests response.
func writeRateLimitExceeded(w http.ResponseWriter, retryAfter int64) {
	if retryAfter <= 0 {
		retryAfter = 1
	}
	w.Header().Set("Retry-After", strconv.FormatInt(retryAfter, 10))
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusTooManyRequests)

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"error":               "rate limit exceeded",
		"retry_after_seconds": retryAfter,
	})
}

// extractClientIP extracts real client IP from X-Forwarded-For, X-Real-IP, or RemoteAddr.
func extractClientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		ip := strings.TrimSpace(parts[0])
		if ip != "" {
			return ip
		}
	}
	if xrip := r.Header.Get("X-Real-IP"); xrip != "" {
		return strings.TrimSpace(xrip)
	}

	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil && host != "" {
		return host
	}
	return r.RemoteAddr
}
