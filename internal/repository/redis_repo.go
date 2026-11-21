package repository

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/redis/rueidis"

	"safepath-live/internal/domain"
)

// ──────────────────────────────────────────────────────────────
// Atomic Ping Lua Script
//
// Why execute a Lua script inside Redis?
// In a high-throughput system handling tens of thousands of requests per second,
// sending multiple separate commands (e.g. HGET, HSET, PUBLISH, XADD) creates
// two major problems:
//   1. High Network Latency: 4 or 5 round-trips over the network per ping.
//   2. Race Conditions: Two simultaneous pings might read stale data and conflict.
//
// By bundling the entire write logic into one Lua script, Redis executes it:
//   - ATOMICALLY: Nothing else interrupts it. Geofencing transitions (ACTIVE -> ARRIVED)
//     happen exactly once without duplicate events.
//   - IN A SINGLE NETWORK ROUND-TRIP: The Go backend sends 1 command and receives 1 response.
//
// What this script does in order:
//   Step 1: Check if the trip exists in the Redis hash (trip:{code}). If not -> NOT_FOUND.
//   Step 2: Check if the trip is currently ACTIVE. If already ARRIVED -> INACTIVE.
//   Step 3: Authenticate the caller by matching the owner token's SHA-256 hash -> UNAUTHORIZED.
//   Step 4: Update the user's latest latitude, longitude, speed, and timestamp.
//   Step 5: Calculate the Haversine distance to the destination. If distance <= arrival radius,
//           atomically mark the trip status as ARRIVED.
//   Step 6: PUBLISH a compact JSON event to channel:trip:{code} so live SSE map viewers receive
//           the new location in real time.
//   Step 7: XADD the point to the Redis Stream (stream:pings) with MAXLEN capping so the background
//           flusher can write it to PostgreSQL without building an unbounded queue.
//   Step 8: Return the status string: "OK" or "ARRIVED".
//
// Keys passed to Lua:
//   KEYS[1] = "trip:{share_code}"         (Redis Hash storing trip state)
//   KEYS[2] = "channel:trip:{share_code}" (Redis Pub/Sub channel for live map viewers)
//   KEYS[3] = "stream:pings"              (Redis Stream for async PostgreSQL persistence)
//
// Arguments passed to Lua:
//   ARGV[1] = owner_token_hash (hex)
//   ARGV[2] = current latitude
//   ARGV[3] = current longitude
//   ARGV[4] = current speed in meters/second
//   ARGV[5] = current timestamp (RFC3339Nano)
//   ARGV[6] = trip internal UUID
//   ARGV[7] = trip share code
// ──────────────────────────────────────────────────────────────
var pingScript = rueidis.NewLuaScript(`
-- 1. Load existing trip data from Redis hash
local trip = redis.call('HGETALL', KEYS[1])
if #trip == 0 then
    return 'NOT_FOUND'
end

-- Convert flat array [key1, val1, key2, val2] into a Lua lookup table
local data = {}
for i = 1, #trip, 2 do
    data[trip[i]] = trip[i+1]
end

-- 2. Verify the trip is still active
if data['status'] ~= 'ACTIVE' then
    return 'INACTIVE'
end

-- 3. Verify owner authentication token
if data['owner_token_hash'] ~= ARGV[1] then
    return 'UNAUTHORIZED'
end

-- 4. Update the live coordinates in the trip hash
redis.call('HSET', KEYS[1],
    'lat',        ARGV[2],
    'lng',        ARGV[3],
    'speed_mps',  ARGV[4],
    'updated_at', ARGV[5])

-- 5. Calculate great-circle Haversine distance to destination (in meters)
local lat1     = tonumber(ARGV[2])
local lng1     = tonumber(ARGV[3])
local dest_lat = tonumber(data['dest_lat'])
local dest_lng = tonumber(data['dest_lng'])
local radius   = tonumber(data['radius_m'])
local rad      = math.pi / 180

local dlat = (dest_lat - lat1) * rad
local dlng = (dest_lng - lng1) * rad
local a = math.sin(dlat / 2) ^ 2
        + math.cos(lat1 * rad) * math.cos(dest_lat * rad)
        * math.sin(dlng / 2) ^ 2
local c    = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
local dist = 6371000 * c

-- If inside destination radius, transition status to ARRIVED
local status = 'OK'
local trip_id = data['trip_id'] or ARGV[6] or ''
if dist <= radius then
    redis.call('HSET', KEYS[1], 'status', 'ARRIVED')
    status = 'ARRIVED'
end

-- 6. Fan out the live location update to all SSE map viewers via Pub/Sub
local event = cjson.encode({
    trip_id    = trip_id,
    share_code = ARGV[7],
    lat        = tonumber(ARGV[2]),
    lng        = tonumber(ARGV[3]),
    speed_mps  = tonumber(ARGV[4]),
    status     = status,
    updated_at = ARGV[5]
})
redis.call('PUBLISH', KEYS[2], event)

-- 7. Append coordinate to the stream for async PostgreSQL batch insertion.
-- MAXLEN ~ 1000000 ensures memory stays bounded even if PostgreSQL is temporarily offline.
redis.call('XADD', KEYS[3], 'MAXLEN', '~', '1000000', '*',
    'trip_id',     trip_id,
    'lat',         ARGV[2],
    'lng',         ARGV[3],
    'speed_mps',   ARGV[4],
    'recorded_at', ARGV[5])

-- 8. Return result code to the Go backend
if status == 'ARRIVED' then
    return status .. ':' .. trip_id
end
return status
`)

// ──────────────────────────────────────────────────────────────
// End Trip Lua Script
//
// Allows the trip owner to manually terminate their journey early.
// Verifies authorization, updates the status to ARRIVED, and publishes
// a final event to notify all connected viewers.
// ──────────────────────────────────────────────────────────────
var endScript = rueidis.NewLuaScript(`
-- 1. Verify trip exists
local trip = redis.call('HGETALL', KEYS[1])
if #trip == 0 then
    return 'NOT_FOUND'
end

local data = {}
for i = 1, #trip, 2 do
    data[trip[i]] = trip[i+1]
end

-- 2. Verify trip is active
if data['status'] ~= 'ACTIVE' then
    return 'INACTIVE'
end

-- 3. Verify owner token
if data['owner_token_hash'] ~= ARGV[1] then
    return 'UNAUTHORIZED'
end

-- 4. Mark status as ARRIVED
redis.call('HSET', KEYS[1], 'status', 'ARRIVED', 'updated_at', ARGV[2])

-- 5. Broadcast arrival event to all viewers
local event = cjson.encode({
    trip_id    = data['trip_id'],
    share_code = KEYS[1]:sub(6),
    lat        = tonumber(data['lat']),
    lng        = tonumber(data['lng']),
    status     = 'ARRIVED',
    updated_at = ARGV[2]
})
redis.call('PUBLISH', KEYS[2], event)

-- Return trip_id so the Go caller can update PostgreSQL in the background
return data['trip_id']
`)

// ──────────────────────────────────────────────────────────────

// RedisRepo manages real-time caching, atomic Lua execution, Pub/Sub,
// and stream queues using rueidis with auto-pipelining.
type RedisRepo struct {
	client rueidis.Client
	log    *slog.Logger
}

// NewRedisRepo initializes the Redis repository.
func NewRedisRepo(client rueidis.Client, log *slog.Logger) *RedisRepo {
	return &RedisRepo{client: client, log: log}
}

// Helper functions for consistent Redis key naming:
func tripKey(code string) string    { return "trip:" + code }
func channelKey(code string) string { return "channel:trip:" + code }

const streamKey = "stream:pings"

// StreamKey returns the Redis stream key used for asynchronous ping persistence.
func StreamKey() string { return streamKey }

// SetTrip saves initial trip details into a Redis Hash and sets an automatic TTL
// matching the trip's expiration time.
//
// Called once when a trip is created (POST /v1/trips). This ensures that subsequent
// GPS pings (POST /v1/trips/{code}/ping) can read all necessary metadata straight
// from Redis RAM with zero database disk lookups.
func (r *RedisRepo) SetTrip(ctx context.Context, trip *domain.Trip, ttl time.Duration) error {
	key := tripKey(trip.ShareCode)
	hashHex := fmt.Sprintf("%x", trip.OwnerTokenHash)

	// Build the HSET command containing all trip metadata
	hsetCmd := r.client.B().Arbitrary("HSET").Keys(key).Args(
		"trip_id", trip.ID,
		"owner_token_hash", hashHex,
		"dest_lat", strconv.FormatFloat(trip.DestLat, 'f', -1, 64),
		"dest_lng", strconv.FormatFloat(trip.DestLng, 'f', -1, 64),
		"radius_m", strconv.FormatFloat(trip.ArrivalRadius, 'f', -1, 64),
		"status", trip.Status,
		"expires_at", trip.ExpiresAt.Format(time.RFC3339),
		"lat", "0",
		"lng", "0",
		"speed_mps", "0",
		"updated_at", trip.CreatedAt.Format(time.RFC3339Nano),
	).Build()

	// Set an automatic TTL on the hash key so stale data self-cleans
	expireCmd := r.client.B().Expire().Key(key).Seconds(int64(ttl.Seconds())).Build()

	// Send both commands to Redis in a single pipelined request
	for _, resp := range r.client.DoMulti(ctx, hsetCmd, expireCmd) {
		if err := resp.Error(); err != nil {
			return fmt.Errorf("redis set trip: %w", err)
		}
	}
	return nil
}

// GetTrip retrieves the latest live snapshot of a trip from Redis RAM.
// Returns (nil, nil) if the trip does not exist or has expired.
func (r *RedisRepo) GetTrip(ctx context.Context, code string) (*domain.Trip, error) {
	cmd := r.client.B().Hgetall().Key(tripKey(code)).Build()
	m, err := r.client.Do(ctx, cmd).AsStrMap()
	if err != nil {
		return nil, fmt.Errorf("redis get trip: %w", err)
	}
	if len(m) == 0 {
		return nil, nil
	}

	t := &domain.Trip{
		ID:        m["trip_id"],
		ShareCode: code,
		Status:    m["status"],
	}

	t.DestLat, _ = strconv.ParseFloat(m["dest_lat"], 64)
	t.DestLng, _ = strconv.ParseFloat(m["dest_lng"], 64)
	t.ArrivalRadius, _ = strconv.ParseFloat(m["radius_m"], 64)
	t.Lat, _ = strconv.ParseFloat(m["lat"], 64)
	t.Lng, _ = strconv.ParseFloat(m["lng"], 64)
	t.SpeedMPS, _ = strconv.ParseFloat(m["speed_mps"], 64)

	if ea, ok := m["expires_at"]; ok {
		t.ExpiresAt, _ = time.Parse(time.RFC3339, ea)
	}
	if ua, ok := m["updated_at"]; ok {
		t.UpdatedAt, _ = time.Parse(time.RFC3339Nano, ua)
	}

	return t, nil
}

// PingTrip runs our atomic Ping Lua script.
//
// Performs authentication, coordinate update, geofence calculation, Pub/Sub broadcast,
// and Stream append in a SINGLE Redis call.
//
// Returns one of:
//   - "OK": Coordinate recorded; trip remains active.
//   - "ARRIVED": Coordinate was within arrival_radius_m; trip marked arrived.
//   - "NOT_FOUND": Trip does not exist in Redis.
//   - "UNAUTHORIZED": Provided owner token hash does not match.
//   - "INACTIVE": Trip is already completed or expired.
func (r *RedisRepo) PingTrip(ctx context.Context, code, tokenHashHex string,
	lat, lng, speedMPS float64) (string, string, error) {

	now := time.Now().UTC().Format(time.RFC3339Nano)
	keys := []string{tripKey(code), channelKey(code), streamKey}
	args := []string{
		tokenHashHex,
		strconv.FormatFloat(lat, 'f', -1, 64),
		strconv.FormatFloat(lng, 'f', -1, 64),
		strconv.FormatFloat(speedMPS, 'f', -1, 64),
		now,
		"", // trip_id is loaded directly from the Redis hash in Lua
		code,
	}

	resp := pingScript.Exec(ctx, r.client, keys, args)
	resStr, err := resp.ToString()
	if err != nil {
		return "", "", fmt.Errorf("ping lua exec: %w", err)
	}
	parts := strings.SplitN(resStr, ":", 2)
	status := parts[0]
	tripID := ""
	if len(parts) > 1 {
		tripID = parts[1]
	}
	return status, tripID, nil
}

// EndTrip runs our atomic End Trip Lua script to manually conclude a trip.
// Returns the trip UUID on success so the caller can update PostgreSQL in the background.
func (r *RedisRepo) EndTrip(ctx context.Context, code, tokenHashHex string) (string, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	keys := []string{tripKey(code), channelKey(code)}
	args := []string{tokenHashHex, now}

	resp := endScript.Exec(ctx, r.client, keys, args)
	result, err := resp.ToString()
	if err != nil {
		return "", fmt.Errorf("end lua exec: %w", err)
	}
	return result, nil
}

// Client returns the underlying rueidis.Client.
// Useful for packages like pubsub_hub and batch_flusher that need direct Redis capabilities.
func (r *RedisRepo) Client() rueidis.Client {
	return r.client
}
