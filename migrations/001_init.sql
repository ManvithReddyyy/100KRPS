-- SafePath Live: Schema for trips and time-partitioned location pings.
-- Applied automatically by Postgres on first boot via docker-entrypoint-initdb.d.

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ─────────────────────────────────────────────
-- Trips: one row per shared journey
-- ─────────────────────────────────────────────
CREATE TABLE trips (
    id                UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    share_code        VARCHAR(12)     UNIQUE NOT NULL,
    owner_token_hash  BYTEA           NOT NULL,       -- SHA-256 of raw 32-byte token
    dest_lat          DOUBLE PRECISION NOT NULL,
    dest_lng          DOUBLE PRECISION NOT NULL,
    arrival_radius_m  DOUBLE PRECISION NOT NULL DEFAULT 50,
    status            VARCHAR(16)     NOT NULL DEFAULT 'ACTIVE',
    expires_at        TIMESTAMPTZ     NOT NULL,
    created_at        TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- Fast lookup by share_code (already UNIQUE but explicit for clarity).
CREATE INDEX idx_trips_status_active ON trips (status) WHERE status = 'ACTIVE';

-- ─────────────────────────────────────────────
-- Location Pings: append-heavy, time-partitioned
-- ─────────────────────────────────────────────
CREATE TABLE location_pings (
    trip_id      UUID             NOT NULL REFERENCES trips(id),
    lat          DOUBLE PRECISION NOT NULL,
    lng          DOUBLE PRECISION NOT NULL,
    speed_mps    REAL,
    recorded_at  TIMESTAMPTZ      NOT NULL
) PARTITION BY RANGE (recorded_at);

-- Default partition catches everything until date-specific partitions are created.
CREATE TABLE location_pings_default PARTITION OF location_pings DEFAULT;

-- Composite index for efficient per-trip queries ordered by time.
CREATE INDEX idx_pings_trip_recorded ON location_pings (trip_id, recorded_at DESC);
