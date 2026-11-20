// Package service contains long-running background processors and domain services.
package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/rueidis"

	"safepath-live/internal/domain"
	"safepath-live/internal/repository"
)

// BatchFlusherConfig configures the stream consumer and bulk persistence pipeline.
type BatchFlusherConfig struct {
	// StreamKey is the Redis Stream holding incoming GPS coordinates (default: "stream:pings").
	StreamKey string

	// GroupName is the Redis Consumer Group name (default: "flusher_group").
	GroupName string

	// ConsumerName is this worker instance's unique consumer identifier.
	ConsumerName string

	// BatchSize is the maximum number of items read from Redis and flushed to Postgres in one go.
	BatchSize int

	// FlushInterval is the maximum duration to wait before writing buffered pings.
	FlushInterval time.Duration

	// BlockDuration is how long XREADGROUP blocks waiting for new stream messages (e.g. 200ms).
	BlockDuration time.Duration

	// ClaimInterval is how often stale/orphaned pending messages are recovered via XAUTOCLAIM.
	ClaimInterval time.Duration

	// MinIdleClaimTime is the minimum age of a pending un-ACKed message before claiming (default: 30s).
	MinIdleClaimTime time.Duration
}

// DefaultBatchFlusherConfig returns sensible production defaults for a 100k RPS environment.
func DefaultBatchFlusherConfig() BatchFlusherConfig {
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "worker-1"
	}

	return BatchFlusherConfig{
		StreamKey:        repository.StreamKey(),
		GroupName:        "flusher_group",
		ConsumerName:     fmt.Sprintf("flusher_%s_%d", hostname, os.Getpid()),
		BatchSize:        1000,
		FlushInterval:    250 * time.Millisecond,
		BlockDuration:    200 * time.Millisecond,
		ClaimInterval:    60 * time.Second,
		MinIdleClaimTime: 30 * time.Second,
	}
}

// BatchFlusher consumes coordinate pings from Redis Stream and flushes them
// to PostgreSQL using binary COPY (pgx.CopyFrom).
//
// Key High-Throughput Guarantees:
//  1. Zero PostgreSQL on the hot write path: Clients get 200 OK after writing to Redis in RAM.
//  2. At-Least-Once Delivery: Messages are only XACK'd AFTER Postgres transaction commits.
//  3. Crash Resilience: If an instance crashes mid-batch, XAUTOCLAIM reclaims and commits the pings.
type BatchFlusher struct {
	redisClient  rueidis.Client
	postgresRepo *repository.PostgresRepo
	cfg          BatchFlusherConfig
	log          *slog.Logger

	// Atomic operational counters (thread-safe, zero allocation)
	flushedTotal atomic.Int64
	claimedTotal atomic.Int64
	errorsTotal  atomic.Int64
	lastFlushDur atomic.Int64 // in microseconds

	stopOnce sync.Once
	done     chan struct{}
}

// NewBatchFlusher initializes a new stream-to-database flusher.
func NewBatchFlusher(
	redisClient rueidis.Client,
	postgresRepo *repository.PostgresRepo,
	cfg BatchFlusherConfig,
	log *slog.Logger,
) *BatchFlusher {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 1000
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 250 * time.Millisecond
	}
	if cfg.BlockDuration <= 0 {
		cfg.BlockDuration = 200 * time.Millisecond
	}
	if cfg.ClaimInterval <= 0 {
		cfg.ClaimInterval = 60 * time.Second
	}
	if cfg.MinIdleClaimTime <= 0 {
		cfg.MinIdleClaimTime = 30 * time.Second
	}

	return &BatchFlusher{
		redisClient:  redisClient,
		postgresRepo: postgresRepo,
		cfg:          cfg,
		log:          log,
		done:         make(chan struct{}),
	}
}

// Start spins up the background consumer loop and the stale message claimer.
// It blocks until ctx is canceled, ensuring all in-flight pings are cleanly flushed.
func (f *BatchFlusher) Start(ctx context.Context) error {
	defer close(f.done)

	// Step 1: Ensure consumer group exists on the stream
	if err := f.ensureConsumerGroup(ctx); err != nil {
		return fmt.Errorf("ensure consumer group: %w", err)
	}

	f.log.Info("batch flusher started",
		"stream", f.cfg.StreamKey,
		"group", f.cfg.GroupName,
		"consumer", f.cfg.ConsumerName,
		"batch_size", f.cfg.BatchSize,
		"flush_interval", f.cfg.FlushInterval,
	)

	// Step 2: Run startup recovery of unacknowledged messages
	f.claimStaleMessages(ctx)

	// Step 3: Run periodic stale claim ticker
	go f.runClaimerLoop(ctx)

	// Step 4: Main consumer loop
	f.runConsumerLoop(ctx)

	f.log.Info("batch flusher stopped cleanly",
		"total_flushed", f.flushedTotal.Load(),
		"total_claimed", f.claimedTotal.Load(),
		"total_errors", f.errorsTotal.Load(),
	)
	return nil
}

// ensureConsumerGroup creates the Redis consumer group if it does not yet exist.
func (f *BatchFlusher) ensureConsumerGroup(ctx context.Context) error {
	cmd := f.redisClient.B().XgroupCreate().
		Key(f.cfg.StreamKey).
		Group(f.cfg.GroupName).
		Id("0").
		Mkstream().
		Build()

	err := f.redisClient.Do(ctx, cmd).Error()
	if err != nil {
		// BUSYGROUP Consumer Group name already exists is expected and not an error
		if rueidis.IsRedisNil(err) || isBusyGroup(err) {
			return nil
		}
		return err
	}
	return nil
}

// runConsumerLoop continuously reads new stream messages and batches them for PostgreSQL.
func (f *BatchFlusher) runConsumerLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Read up to BatchSize messages from stream:pings
		cmd := f.redisClient.B().Xreadgroup().
			Group(f.cfg.GroupName, f.cfg.ConsumerName).
			Count(int64(f.cfg.BatchSize)).
			Block(int64(f.cfg.BlockDuration.Milliseconds())).
			Streams().
			Key(f.cfg.StreamKey).
			Id(">").
			Build()

		resp, err := f.redisClient.Do(ctx, cmd).AsXRead()
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			if !rueidis.IsRedisNil(err) {
				f.errorsTotal.Add(1)
				f.log.Warn("xreadgroup error", "error", err)
				time.Sleep(100 * time.Millisecond)
			}
			continue
		}

		entries, ok := resp[f.cfg.StreamKey]
		if !ok || len(entries) == 0 {
			continue
		}

		if err := f.processAndFlush(ctx, entries); err != nil {
			f.errorsTotal.Add(1)
			f.log.Error("failed to process and flush batch", "count", len(entries), "error", err)
			time.Sleep(200 * time.Millisecond)
		}
	}
}

// processAndFlush parses stream entries into domain objects, bulk-inserts them into Postgres,
// and acknowledges (XACK) them in Redis.
func (f *BatchFlusher) processAndFlush(ctx context.Context, entries []rueidis.XRangeEntry) error {
	if len(entries) == 0 {
		return nil
	}

	start := time.Now()
	pings := make([]domain.LocationPing, 0, len(entries))
	msgIDs := make([]string, 0, len(entries))

	for _, entry := range entries {
		ping, err := parseStreamPing(entry.FieldValues)
		if err != nil {
			f.log.Warn("skipping malformed stream entry", "id", entry.ID, "error", err)
			msgIDs = append(msgIDs, entry.ID) // Still ACK to avoid poisoning queue
			continue
		}
		pings = append(pings, ping)
		msgIDs = append(msgIDs, entry.ID)
	}

	// Bulk insert into PostgreSQL via binary COPY (pgx.CopyFrom)
	if len(pings) > 0 {
		inserted, err := f.postgresRepo.BulkInsertPings(ctx, pings)
		if err != nil {
			return fmt.Errorf("bulk insert pings: %w", err)
		}
		f.flushedTotal.Add(inserted)
	}

	// Acknowledge successfully persisted message IDs
	if len(msgIDs) > 0 {
		if err := f.ackMessages(ctx, msgIDs); err != nil {
			f.log.Error("failed to ack messages in stream", "count", len(msgIDs), "error", err)
			// Non-fatal: pings are already safe in PostgreSQL; claimer will handle duplicate ACKs cleanly
		}
	}

	dur := time.Since(start)
	f.lastFlushDur.Store(dur.Microseconds())
	return nil
}

// ackMessages acknowledges a list of stream IDs in Redis.
func (f *BatchFlusher) ackMessages(ctx context.Context, ids []string) error {
	cmd := f.redisClient.B().Xack().
		Key(f.cfg.StreamKey).
		Group(f.cfg.GroupName).
		Id(ids...).
		Build()

	return f.redisClient.Do(ctx, cmd).Error()
}

// runClaimerLoop periodically looks for stale pending messages from dead consumers.
func (f *BatchFlusher) runClaimerLoop(ctx context.Context) {
	ticker := time.NewTicker(f.cfg.ClaimInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			f.claimStaleMessages(ctx)
		}
	}
}

// claimStaleMessages uses XAUTOCLAIM to reclaim and flush unacknowledged messages
// whose previous consumer crashed or disconnected.
func (f *BatchFlusher) claimStaleMessages(ctx context.Context) {
	startID := "0-0"
	minIdleMs := strconv.FormatInt(f.cfg.MinIdleClaimTime.Milliseconds(), 10)

	for {
		cmd := f.redisClient.B().Arbitrary("XAUTOCLAIM").
			Keys(f.cfg.StreamKey).
			Args(
				f.cfg.GroupName,
				f.cfg.ConsumerName,
				minIdleMs,
				startID,
				"COUNT", strconv.Itoa(f.cfg.BatchSize),
			).Build()

		resp := f.redisClient.Do(ctx, cmd)
		if err := resp.Error(); err != nil {
			if !rueidis.IsRedisNil(err) && !errors.Is(err, context.Canceled) {
				f.log.Warn("xautoclaim check failed", "error", err)
			}
			return
		}

		arr, err := resp.ToArray()
		if err != nil || len(arr) < 2 {
			return
		}

		nextID, _ := arr[0].ToString()
		entriesArr, _ := arr[1].ToArray()

		if len(entriesArr) == 0 {
			return
		}

		entries := make([]rueidis.XRangeEntry, 0, len(entriesArr))
		for _, rawEntry := range entriesArr {
			parts, err := rawEntry.ToArray()
			if err != nil || len(parts) < 2 {
				continue
			}
			id, _ := parts[0].ToString()
			fieldMap, _ := parts[1].AsStrMap()

			entries = append(entries, rueidis.XRangeEntry{
				ID:          id,
				FieldValues: fieldMap,
			})
		}

		if len(entries) > 0 {
			f.claimedTotal.Add(int64(len(entries)))
			if err := f.processAndFlush(ctx, entries); err != nil {
				f.log.Error("failed to process claimed messages", "count", len(entries), "error", err)
				return
			}
		}

		if nextID == "0-0" || nextID == startID {
			break
		}
		startID = nextID
	}
}

// parseStreamPing parses key-value fields from a stream entry into domain.LocationPing.
func parseStreamPing(m map[string]string) (domain.LocationPing, error) {
	tripID := m["trip_id"]
	if tripID == "" {
		return domain.LocationPing{}, errors.New("missing trip_id")
	}

	lat, err := strconv.ParseFloat(m["lat"], 64)
	if err != nil {
		return domain.LocationPing{}, fmt.Errorf("invalid lat: %w", err)
	}

	lng, err := strconv.ParseFloat(m["lng"], 64)
	if err != nil {
		return domain.LocationPing{}, fmt.Errorf("invalid lng: %w", err)
	}

	speed, _ := strconv.ParseFloat(m["speed_mps"], 64)

	var recordedAt time.Time
	if rawTs, ok := m["recorded_at"]; ok && rawTs != "" {
		if t, err := time.Parse(time.RFC3339Nano, rawTs); err == nil {
			recordedAt = t
		} else if t, err := time.Parse(time.RFC3339, rawTs); err == nil {
			recordedAt = t
		}
	}
	if recordedAt.IsZero() {
		recordedAt = time.Now().UTC()
	}

	return domain.LocationPing{
		TripID:     tripID,
		Lat:        lat,
		Lng:        lng,
		SpeedMPS:   float32(speed),
		RecordedAt: recordedAt,
	}, nil
}

// isBusyGroup checks if Redis error is BUSYGROUP (already created).
func isBusyGroup(err error) bool {
	if err == nil {
		return false
	}
	return err.Error() == "BUSYGROUP Consumer Group name already exists"
}

// Stats returns a snapshot of flusher operational metrics.
type FlusherStats struct {
	TotalFlushed    int64
	TotalClaimed    int64
	TotalErrors     int64
	LastFlushMicros int64
}

// Stats returns the live thread-safe counters.
func (f *BatchFlusher) Stats() FlusherStats {
	return FlusherStats{
		TotalFlushed:    f.flushedTotal.Load(),
		TotalClaimed:    f.claimedTotal.Load(),
		TotalErrors:     f.errorsTotal.Load(),
		LastFlushMicros: f.lastFlushDur.Load(),
	}
}
