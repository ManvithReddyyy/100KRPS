// Package domain defines the core data models, request validation rules,
// geographic math calculations, and cryptographic security helpers.
//
// This package contains zero dependencies on external web frameworks or databases,
// ensuring pure business logic that is easy to reason about and thoroughly test.
package domain

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base32"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

// ──────────────────────────────────────────────────────────────
// Data Models
// ──────────────────────────────────────────────────────────────

// Trip represents an active or completed journey.
// It tracks where the user started, where they are heading, their arrival radius,
// and their current live location.
type Trip struct {
	// ID is the unique internal UUID (v4) for this trip.
	ID string `json:"id"`

	// ShareCode is a short, human-friendly 8-character uppercase code (e.g., "7X8K2M9P").
	// Users can share this code or a link containing it with friends and family.
	ShareCode string `json:"share_code"`

	// OwnerTokenHash is the SHA-256 cryptographic hash of the owner's secret token.
	// We only store the hash (never the raw token) so that even if the database is read,
	// the owner's secret key cannot be compromised.
	// The `json:"-"` tag ensures it is never accidentally sent over HTTP to clients.
	OwnerTokenHash []byte `json:"-"`

	// DestLat and DestLng are the destination GPS coordinates (e.g. user's home or office).
	DestLat float64 `json:"dest_lat"`
	DestLng float64 `json:"dest_lng"`

	// ArrivalRadius is the distance in meters around the destination.
	// When the user gets within this distance, the trip automatically transitions to "ARRIVED".
	ArrivalRadius float64 `json:"arrival_radius_m"`

	// Status represents the current state: either "ACTIVE" or "ARRIVED".
	Status string `json:"status"`

	// ExpiresAt is the timestamp after which this trip will automatically expire.
	ExpiresAt time.Time `json:"expires_at"`

	// CreatedAt is the timestamp when the trip was first initiated.
	CreatedAt time.Time `json:"created_at"`

	// Live location fields:
	// These are populated dynamically from the Redis cache during an active trip.
	Lat       float64   `json:"lat,omitempty"`
	Lng       float64   `json:"lng,omitempty"`
	SpeedMPS  float64   `json:"speed_mps,omitempty"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
}

// Possible trip lifecycle status values:
const (
	// StatusActive indicates the trip is currently in progress.
	// The server will accept live GPS pings for this trip.
	StatusActive = "ACTIVE"

	// StatusArrived indicates the user has successfully reached their destination
	// (or manually ended the trip). No further pings will be accepted.
	StatusArrived = "ARRIVED"
)

// LocationPing represents a single GPS point recorded in time.
// These are queued up in Redis Streams and later batch-inserted into PostgreSQL
// for historical record keeping without slowing down the real-time API.
type LocationPing struct {
	TripID     string
	Lat        float64
	Lng        float64
	SpeedMPS   float32
	RecordedAt time.Time
}

// PingEvent is the lightweight JSON payload published across Redis Pub/Sub
// whenever a new GPS coordinate is accepted. It is forwarded directly to
// connected SSE (Server-Sent Events) viewers watching the trip on their map.
type PingEvent struct {
	TripID    string  `json:"trip_id"`
	ShareCode string  `json:"share_code"`
	Lat       float64 `json:"lat"`
	Lng       float64 `json:"lng"`
	SpeedMPS  float64 `json:"speed_mps"`
	Status    string  `json:"status"`
	UpdatedAt string  `json:"updated_at"`
}

// ──────────────────────────────────────────────────────────────
// HTTP Request Payloads & Validation Rules
// ──────────────────────────────────────────────────────────────

// CreateTripRequest is the JSON payload received when a user creates a new trip (POST /v1/trips).
type CreateTripRequest struct {
	// DestLat is the destination latitude (-90 to +90 degrees).
	DestLat float64 `json:"dest_lat"`

	// DestLng is the destination longitude (-180 to +180 degrees).
	DestLng float64 `json:"dest_lng"`

	// ArrivalRadius is the geofence radius in meters (optional; defaults to 50 meters).
	ArrivalRadius float64 `json:"arrival_radius_m,omitempty"`

	// ExpiresMinutes is how long the trip should stay alive (between 5 and 720 minutes).
	ExpiresMinutes int `json:"expires_in_minutes"`
}

// Validate ensures all input fields contain valid geographical and temporal bounds.
// Rejecting invalid data early protects downstream database operations.
func (r *CreateTripRequest) Validate() error {
	if r.DestLat < -90 || r.DestLat > 90 {
		return errors.New("dest_lat must be a valid latitude between -90 and 90")
	}
	if r.DestLng < -180 || r.DestLng > 180 {
		return errors.New("dest_lng must be a valid longitude between -180 and 180")
	}
	if r.ExpiresMinutes < 5 || r.ExpiresMinutes > 720 {
		return errors.New("expires_in_minutes must be between 5 minutes and 12 hours (720 minutes)")
	}
	// If the client did not specify an arrival radius, apply the safe 50-meter default.
	if r.ArrivalRadius <= 0 {
		r.ArrivalRadius = 50
	}
	return nil
}

// PingRequest is the JSON payload sent by the traveler's phone on every location update (POST /v1/trips/{code}/ping).
type PingRequest struct {
	// Lat is the current GPS latitude (-90 to +90).
	Lat float64 `json:"lat"`

	// Lng is the current GPS longitude (-180 to +180).
	Lng float64 `json:"lng"`

	// SpeedMPS is the user's travel speed in meters per second (optional).
	SpeedMPS float64 `json:"speed_mps,omitempty"`
}

// Validate verifies that coordinates are mathematically valid GPS points.
func (r *PingRequest) Validate() error {
	if r.Lat < -90 || r.Lat > 90 {
		return errors.New("lat must be between -90 and 90")
	}
	if r.Lng < -180 || r.Lng > 180 {
		return errors.New("lng must be between -180 and 180")
	}
	return nil
}

// ──────────────────────────────────────────────────────────────
// Geographic Math: Haversine Distance
// ──────────────────────────────────────────────────────────────

// earthRadiusM is the average radius of the Earth in meters (~6,371 km).
const earthRadiusM = 6_371_000.0

// HaversineMeters calculates the great-circle distance in meters between two GPS coordinates.
//
// Why Haversine?
// Standard Euclidean distance (Pythagoras: sqrt(dx^2 + dy^2)) produces severe distortion
// on spherical surfaces like the Earth. Haversine accounts for the Earth's curvature,
// providing accurate meter-level precision for geofencing (detecting if someone has reached home).
func HaversineMeters(lat1, lng1, lat2, lng2 float64) float64 {
	dLat := toRad(lat2 - lat1)
	dLng := toRad(lng2 - lng1)

	// Haversine formula:
	// a = sin²(Δlat/2) + cos(lat1) * cos(lat2) * sin²(Δlng/2)
	// c = 2 * atan2(√a, √(1−a))
	// distance = Earth_radius * c
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(toRad(lat1))*math.Cos(toRad(lat2))*
			math.Sin(dLng/2)*math.Sin(dLng/2)

	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	return earthRadiusM * c
}

// toRad converts degrees to radians.
func toRad(deg float64) float64 {
	return deg * math.Pi / 180
}

// ──────────────────────────────────────────────────────────────
// Cryptography & Token Helpers
// ──────────────────────────────────────────────────────────────

// GenerateUUID generates a cryptographically secure Version 4 UUID string.
// It uses crypto/rand directly so we do not need heavy third-party UUID packages.
func GenerateUUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("critical security failure: crypto/rand source unavailable: " + err.Error())
	}
	// Set UUID version 4 and variant bits according to RFC 4122
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10xx
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

// GenerateShareCode produces a cryptographically random, unguessable 8-character code.
//
// Why Base32?
// Standard Base64 includes confusing characters (like 0 vs O, 1 vs I, + and /).
// Standard Base32 uses only unambiguous uppercase letters and numbers (A-Z, 2-7),
// making it easy to read over the phone or type on a mobile keyboard.
func GenerateShareCode() (string, error) {
	// 5 random bytes (40 bits) encode into exactly 8 Base32 characters (each char represents 5 bits).
	b := make([]byte, 5)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate share code: %w", err)
	}
	code := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b)
	return strings.ToUpper(code), nil
}

// GenerateOwnerToken creates a 32-byte secret random token for the trip creator.
// It returns:
//  1. token: The 64-character hex string given to the user's device (the "password").
//  2. hash: The SHA-256 byte slice stored in the database.
//
// By storing only the SHA-256 hash in our databases, even a database dump will
// not compromise the owner's active bearer token.
func GenerateOwnerToken() (token string, hash []byte, err error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", nil, fmt.Errorf("generate owner token: %w", err)
	}
	h := sha256.Sum256(raw)
	return hex.EncodeToString(raw), h[:], nil
}

// HashToken computes the SHA-256 checksum of an incoming hex-encoded bearer token.
// Used to verify authentication on incoming API requests.
func HashToken(hexToken string) ([]byte, error) {
	raw, err := hex.DecodeString(hexToken)
	if err != nil {
		return nil, fmt.Errorf("invalid hex token: %w", err)
	}
	h := sha256.Sum256(raw)
	return h[:], nil
}

// TokenHashHex converts a hex-encoded token into its SHA-256 hash, returned as a hex string.
// This allows straightforward matching against the hex-encoded hash stored inside Redis hashes.
func TokenHashHex(hexToken string) (string, error) {
	h, err := HashToken(hexToken)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(h), nil
}

// ConstantTimeHashCompare compares two byte slices in constant time.
//
// Why is this critical?
// Standard string or byte comparisons (like `a == b`) stop checking at the very first
// mismatched character. An attacker can measure nanosecond response differences to deduce
// character by character what the correct hash is (a "timing attack").
// ConstantTimeCompare takes the exact same number of CPU cycles regardless of whether
// the match succeeds or fails, neutralizing timing side-channels.
func ConstantTimeHashCompare(a, b []byte) bool {
	return subtle.ConstantTimeCompare(a, b) == 1
}
