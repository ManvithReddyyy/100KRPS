// Package handler provides HTTP endpoints for managing trips and streaming live updates.
package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"safepath-live/internal/domain"
	"safepath-live/internal/repository"
)

// TripHandler manages the REST API endpoints for creating, pinging, fetching, and ending trips.
type TripHandler struct {
	pg    *repository.PostgresRepo
	redis *repository.RedisRepo
	log   *slog.Logger
}

// NewTripHandler creates a new handler instance.
func NewTripHandler(pg *repository.PostgresRepo, redis *repository.RedisRepo, log *slog.Logger) *TripHandler {
	return &TripHandler{pg: pg, redis: redis, log: log}
}

// ──────────────────────────────────────────────────────────────
// POST /v1/trips — Start a New Trip
// ──────────────────────────────────────────────────────────────

// Create initializes a new live trip session.
//
// What happens during creation:
//  1. Validates destination coordinates and requested expiration duration.
//  2. Generates an unguessable 8-character share code (Base32) for viewers.
//  3. Generates a 32-byte secret owner token (hex string) for the sender.
//  4. Persists the initial record to PostgreSQL (cold path).
//  5. Hydrates the trip metadata into Redis with an automatic TTL so subsequent
//     GPS pings hit only fast in-memory RAM.
//  6. Returns the share code, owner token, shareable tracking URL, and expiration time.
func (h *TripHandler) Create(w http.ResponseWriter, r *http.Request) {
	// Set a 5-second deadline for trip creation (cold path involves a Postgres write)
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// Read and validate request body, limiting max size to 4KB to prevent memory abuse
	var req domain.CreateTripRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResp("invalid JSON body: please verify fields and syntax"))
		return
	}
	if err := req.Validate(); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResp(err.Error()))
		return
	}

	// Generate 8-character share code (e.g. "7X8K2M9P")
	shareCode, err := domain.GenerateShareCode()
	if err != nil {
		h.log.Error("failed to generate share code", "error", err)
		writeJSON(w, http.StatusInternalServerError, errorResp("internal error generating trip code"))
		return
	}

	// Generate 32-byte owner token and its SHA-256 hash
	ownerToken, tokenHash, err := domain.GenerateOwnerToken()
	if err != nil {
		h.log.Error("failed to generate owner token", "error", err)
		writeJSON(w, http.StatusInternalServerError, errorResp("internal error generating security credentials"))
		return
	}

	now := time.Now().UTC()
	expiresAt := now.Add(time.Duration(req.ExpiresMinutes) * time.Minute)
	trip := &domain.Trip{
		ID:             domain.GenerateUUID(),
		ShareCode:      shareCode,
		OwnerTokenHash: tokenHash,
		DestLat:        req.DestLat,
		DestLng:        req.DestLng,
		ArrivalRadius:  req.ArrivalRadius,
		Status:         domain.StatusActive,
		ExpiresAt:      expiresAt,
		CreatedAt:      now,
	}

	// Step 1: Save to PostgreSQL (durable record)
	if err := h.pg.CreateTrip(ctx, trip); err != nil {
		h.log.Error("postgres create trip failed", "error", err, "share_code", shareCode)
		writeJSON(w, http.StatusInternalServerError, errorResp("failed to persist trip"))
		return
	}

	// Step 2: Hydrate Redis Hash with matching TTL
	ttl := time.Until(expiresAt) + time.Minute // 1 minute safety buffer past expiry
	if err := h.redis.SetTrip(ctx, trip, ttl); err != nil {
		h.log.Error("redis set trip failed", "error", err, "share_code", shareCode)
		writeJSON(w, http.StatusInternalServerError, errorResp("failed to initialize trip cache"))
		return
	}

	// Build the public share link based on incoming request host/protocol
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if fwd := r.Header.Get("X-Forwarded-Proto"); fwd != "" {
		scheme = fwd
	}
	shareURL := fmt.Sprintf("%s://%s/?code=%s", scheme, r.Host, shareCode)

	h.log.Info("new trip initiated successfully", "share_code", shareCode, "trip_id", trip.ID)

	// Return credentials to the trip owner
	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"share_code":  shareCode,
		"owner_token": ownerToken,
		"share_url":   shareURL,
		"expires_at":  expiresAt.Format(time.RFC3339),
	})
}

// ──────────────────────────────────────────────────────────────
// POST /v1/trips/{code}/ping — Live GPS Ingestion (Hot Path)
// ──────────────────────────────────────────────────────────────

// Ping receives live GPS coordinates from the traveler's device.
//
// Performance & Architecture:
//  - ZERO PostgreSQL queries occur inside this handler.
//  - Executes our single atomic Redis Lua script which updates coordinates,
//    checks the geofence, publishes to SSE viewers, and enqueues to the Redis Stream.
//  - Returns 202 Accepted with status "OK" or "ARRIVED".
func (h *TripHandler) Ping(w http.ResponseWriter, r *http.Request) {
	code := r.PathValue("code")

	// Verify Authorization header (Bearer <token>)
	token := extractBearerToken(r)
	if token == "" {
		writeJSON(w, http.StatusUnauthorized, errorResp("missing Authorization: Bearer <owner_token> header"))
		return
	}

	// Parse JSON coordinates
	var req domain.PingRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResp("invalid JSON body"))
		return
	}
	if err := req.Validate(); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResp(err.Error()))
		return
	}

	// Hash the incoming bearer token to match against the hash stored in Redis
	tokenHashHex, err := domain.TokenHashHex(token)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, errorResp("invalid owner token format"))
		return
	}

	// Fetch trip metadata from Redis RAM
	tripFromRedis, err := h.redis.GetTrip(r.Context(), code)
	if err != nil {
		h.log.Error("redis get trip failed during ping", "error", err, "code", code)
		writeJSON(w, http.StatusInternalServerError, errorResp("internal cache error"))
		return
	}
	if tripFromRedis == nil {
		writeJSON(w, http.StatusNotFound, errorResp("trip not found or expired"))
		return
	}

	// Execute atomic Redis Lua script (auth + update + geofence + pub/sub + stream)
	status, err := h.redis.PingTrip(r.Context(), code, tokenHashHex,
		req.Lat, req.Lng, req.SpeedMPS, tripFromRedis.ID)
	if err != nil {
		h.log.Error("ping lua execution failed", "error", err, "code", code)
		writeJSON(w, http.StatusInternalServerError, errorResp("internal ping processing failure"))
		return
	}

	// Handle the status result returned from Redis
	switch status {
	case "NOT_FOUND":
		writeJSON(w, http.StatusNotFound, errorResp("trip not found"))
	case "UNAUTHORIZED":
		writeJSON(w, http.StatusForbidden, errorResp("invalid owner token: unauthorized"))
	case "INACTIVE":
		writeJSON(w, http.StatusConflict, errorResp("trip is no longer active"))
	case "OK", "ARRIVED":
		// If the user crossed into the destination arrival radius, update PostgreSQL asynchronously
		if status == "ARRIVED" {
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := h.pg.UpdateTripStatus(ctx, tripFromRedis.ID, domain.StatusArrived); err != nil {
					h.log.Error("async postgres status update failed", "error", err, "trip_id", tripFromRedis.ID)
				}
			}()
		}
		// 202 Accepted signifies the GPS point was successfully accepted and processed
		writeJSON(w, http.StatusAccepted, map[string]string{"status": status})
	default:
		h.log.Error("unexpected status code from ping lua", "status", status, "code", code)
		writeJSON(w, http.StatusInternalServerError, errorResp("unexpected processing status"))
	}
}

// ──────────────────────────────────────────────────────────────
// GET /v1/trips/{code} — Fetch Current Trip Snapshot
// ──────────────────────────────────────────────────────────────

// Get returns the current snapshot of a trip.
//
// Strategy:
//  1. Check Redis RAM first. If active, returns live coordinates and speed.
//  2. If expired or not in Redis, falls back to PostgreSQL cold storage.
//  3. If not found in either, returns 404 Not Found.
func (h *TripHandler) Get(w http.ResponseWriter, r *http.Request) {
	code := r.PathValue("code")

	// 1. Check Redis RAM
	trip, err := h.redis.GetTrip(r.Context(), code)
	if err != nil {
		h.log.Warn("redis get trip failed, falling back to postgres", "error", err, "code", code)
	}
	if trip != nil {
		writeJSON(w, http.StatusOK, trip)
		return
	}

	// 2. Fall back to PostgreSQL for completed/historical trips
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	trip, err = h.pg.GetTripByCode(ctx, code)
	if err != nil {
		h.log.Error("postgres get trip failed", "error", err, "code", code)
		writeJSON(w, http.StatusInternalServerError, errorResp("internal lookup error"))
		return
	}
	if trip == nil {
		writeJSON(w, http.StatusNotFound, errorResp("trip not found"))
		return
	}

	writeJSON(w, http.StatusOK, trip)
}

// ──────────────────────────────────────────────────────────────
// POST /v1/trips/{code}/end — Manually Conclude a Trip
// ──────────────────────────────────────────────────────────────

// End allows the trip owner to manually conclude their journey.
// Authenticated via the owner's Bearer token.
func (h *TripHandler) End(w http.ResponseWriter, r *http.Request) {
	code := r.PathValue("code")

	token := extractBearerToken(r)
	if token == "" {
		writeJSON(w, http.StatusUnauthorized, errorResp("missing Authorization: Bearer <token>"))
		return
	}

	tokenHashHex, err := domain.TokenHashHex(token)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, errorResp("invalid token format"))
		return
	}

	// Execute atomic End Trip Lua script
	result, err := h.redis.EndTrip(r.Context(), code, tokenHashHex)
	if err != nil {
		h.log.Error("end trip lua failed", "error", err, "code", code)
		writeJSON(w, http.StatusInternalServerError, errorResp("internal error ending trip"))
		return
	}

	switch result {
	case "NOT_FOUND":
		writeJSON(w, http.StatusNotFound, errorResp("trip not found"))
	case "UNAUTHORIZED":
		writeJSON(w, http.StatusForbidden, errorResp("invalid owner token"))
	case "INACTIVE":
		writeJSON(w, http.StatusConflict, errorResp("trip is already completed or expired"))
	default:
		// Result is the trip_id UUID — update PostgreSQL asynchronously
		tripID := result
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := h.pg.UpdateTripStatus(ctx, tripID, domain.StatusArrived); err != nil {
				h.log.Error("async postgres status update failed on manual end", "error", err, "trip_id", tripID)
			}
		}()
		h.log.Info("trip ended manually by owner", "code", code, "trip_id", tripID)
		writeJSON(w, http.StatusOK, map[string]string{"status": domain.StatusArrived})
	}
}

// ──────────────────────────────────────────────────────────────
// JSON Formatting & Security Helpers
// ──────────────────────────────────────────────────────────────

// writeJSON formats and writes a structured JSON response with the appropriate status code.
func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

// errorResp wraps an error message in a consistent {"error": "..."} JSON shape.
func errorResp(msg string) map[string]string {
	return map[string]string{"error": msg}
}

// extractBearerToken extracts the raw token from an "Authorization: Bearer <token>" HTTP header.
func extractBearerToken(r *http.Request) string {
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "Bearer ") {
		return ""
	}
	return strings.TrimPrefix(auth, "Bearer ")
}
