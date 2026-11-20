// Package repository provides database access layers for both PostgreSQL (permanent storage)
// and Redis (high-speed in-memory state and streaming).
package repository

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"safepath-live/internal/domain"
)

// PostgresRepo manages all communication with the PostgreSQL database.
//
// In our 100k RPS architecture, PostgreSQL is deliberately kept OFF the high-speed
// write path (POST /ping). It is only used for:
//  1. Creating a new trip record at the start of a journey.
//  2. Looking up historical or completed trips when they are no longer in Redis.
//  3. Bulk-inserting batches of GPS coordinates asynchronously via pgx.CopyFrom.
type PostgresRepo struct {
	pool *pgxpool.Pool
}

// NewPostgresRepo creates a new repository using an existing pgx connection pool.
func NewPostgresRepo(pool *pgxpool.Pool) *PostgresRepo {
	return &PostgresRepo{pool: pool}
}

// CreateTrip inserts a brand new trip record into PostgreSQL.
//
// This is executed only once per trip during POST /v1/trips (cold path).
// Notice that the trip ID must already be generated (using domain.GenerateUUID)
// so that the exact same ID is used synchronously in both Postgres and Redis.
func (r *PostgresRepo) CreateTrip(ctx context.Context, trip *domain.Trip) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO trips (
			id, 
			share_code, 
			owner_token_hash, 
			dest_lat, 
			dest_lng,
			arrival_radius_m, 
			status, 
			expires_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		trip.ID,
		trip.ShareCode,
		trip.OwnerTokenHash,
		trip.DestLat,
		trip.DestLng,
		trip.ArrivalRadius,
		trip.Status,
		trip.ExpiresAt,
	)
	if err != nil {
		return fmt.Errorf("postgres insert trip: %w", err)
	}
	return nil
}

// GetTripByCode finds a trip by its 8-character share code.
//
// This serves as a durable fallback: if a user visits a completed or expired trip
// whose key has already been evicted from Redis, this method loads the permanent
// trip record from PostgreSQL.
// Returns (nil, nil) if no trip matches the share code.
func (r *PostgresRepo) GetTripByCode(ctx context.Context, code string) (*domain.Trip, error) {
	t := &domain.Trip{}
	err := r.pool.QueryRow(ctx, `
		SELECT 
			id, 
			share_code, 
			owner_token_hash, 
			dest_lat, 
			dest_lng,
			arrival_radius_m, 
			status, 
			expires_at, 
			created_at
		FROM trips
		WHERE share_code = $1`, code).
		Scan(
			&t.ID,
			&t.ShareCode,
			&t.OwnerTokenHash,
			&t.DestLat,
			&t.DestLng,
			&t.ArrivalRadius,
			&t.Status,
			&t.ExpiresAt,
			&t.CreatedAt,
		)
	if err == pgx.ErrNoRows {
		// Cleanly return nil when the code doesn't exist
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("postgres get trip by code: %w", err)
	}
	return t, nil
}

// UpdateTripStatus updates the status column (e.g. from ACTIVE to ARRIVED) for a given trip.
// Called when a trip concludes either via geofence trigger or manual end request.
func (r *PostgresRepo) UpdateTripStatus(ctx context.Context, tripID, status string) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE trips 
		SET status = $1 
		WHERE id = $2`, status, tripID)
	if err != nil {
		return fmt.Errorf("postgres update trip status: %w", err)
	}
	return nil
}

// BulkInsertPings writes an entire slice of GPS pings into PostgreSQL using the binary COPY protocol.
//
// Why pgx.CopyFrom instead of standard INSERT?
// Standard multi-row "INSERT INTO location_pings VALUES (...)" requires PostgreSQL to
// parse the SQL query, plan execution, and manage transaction overhead per row.
// pgx.CopyFrom streams raw binary data directly into the Postgres storage engine buffers,
// achieving speeds of 50,000+ to 100,000+ rows/second with minimal CPU usage.
//
// Returns the number of rows successfully inserted.
func (r *PostgresRepo) BulkInsertPings(ctx context.Context, pings []domain.LocationPing) (int64, error) {
	if len(pings) == 0 {
		return 0, nil
	}

	// Prepare rows in the exact column order expected by pgx.CopyFrom
	rows := make([][]interface{}, len(pings))
	for i, p := range pings {
		rows[i] = []interface{}{
			p.TripID,
			p.Lat,
			p.Lng,
			p.SpeedMPS,
			p.RecordedAt,
		}
	}

	insertedCount, err := r.pool.CopyFrom(ctx,
		pgx.Identifier{"location_pings"},
		[]string{"trip_id", "lat", "lng", "speed_mps", "recorded_at"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return 0, fmt.Errorf("postgres bulk insert pings via copy: %w", err)
	}
	return insertedCount, nil
}

// Pool provides access to the underlying connection pool for health probes or custom queries.
func (r *PostgresRepo) Pool() *pgxpool.Pool {
	return r.pool
}
