package service

import (
	"errors"
	"testing"
	"time"
)

func TestParseStreamPing(t *testing.T) {
	t.Run("valid entry with RFC3339Nano", func(t *testing.T) {
		now := time.Now().UTC()
		nowStr := now.Format(time.RFC3339Nano)

		m := map[string]string{
			"trip_id":     "550e8400-e29b-41d4-a716-446655440000",
			"lat":         "17.3850",
			"lng":         "78.4867",
			"speed_mps":   "12.5",
			"recorded_at": nowStr,
		}

		ping, err := parseStreamPing(m)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if ping.TripID != "550e8400-e29b-41d4-a716-446655440000" {
			t.Errorf("expected trip_id, got %s", ping.TripID)
		}
		if ping.Lat != 17.3850 {
			t.Errorf("expected lat 17.3850, got %f", ping.Lat)
		}
		if ping.Lng != 78.4867 {
			t.Errorf("expected lng 78.4867, got %f", ping.Lng)
		}
		if ping.SpeedMPS != 12.5 {
			t.Errorf("expected speed 12.5, got %f", ping.SpeedMPS)
		}
		if ping.RecordedAt.IsZero() {
			t.Errorf("expected non-zero recordedAt")
		}
	})

	t.Run("valid entry with fallback timestamp", func(t *testing.T) {
		m := map[string]string{
			"trip_id": "test-uuid",
			"lat":     "12.9716",
			"lng":     "77.5946",
		}

		ping, err := parseStreamPing(m)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ping.TripID != "test-uuid" {
			t.Errorf("expected test-uuid, got %s", ping.TripID)
		}
		if ping.RecordedAt.IsZero() {
			t.Errorf("expected non-zero recordedAt when timestamp is omitted")
		}
	})

	t.Run("missing trip_id error", func(t *testing.T) {
		m := map[string]string{
			"lat": "12.9716",
			"lng": "77.5946",
		}
		_, err := parseStreamPing(m)
		if err == nil {
			t.Error("expected error for missing trip_id, got nil")
		}
	})

	t.Run("invalid lat error", func(t *testing.T) {
		m := map[string]string{
			"trip_id": "test-uuid",
			"lat":     "invalid-number",
			"lng":     "77.5946",
		}
		_, err := parseStreamPing(m)
		if err == nil {
			t.Error("expected error for invalid lat, got nil")
		}
	})

	t.Run("invalid lng error", func(t *testing.T) {
		m := map[string]string{
			"trip_id": "test-uuid",
			"lat":     "12.9716",
			"lng":     "not-a-number",
		}
		_, err := parseStreamPing(m)
		if err == nil {
			t.Error("expected error for invalid lng, got nil")
		}
	})
}

func TestIsBusyGroup(t *testing.T) {
	if !isBusyGroup(errors.New("BUSYGROUP Consumer Group name already exists")) {
		t.Errorf("expected true for BUSYGROUP error")
	}
	if isBusyGroup(errors.New("other error")) {
		t.Errorf("expected false for other error")
	}
	if isBusyGroup(nil) {
		t.Errorf("expected false for nil error")
	}
}

func TestBatchFlusher_Stats(t *testing.T) {
	f := NewBatchFlusher(nil, nil, DefaultBatchFlusherConfig(), nil)
	f.flushedTotal.Store(42)
	f.claimedTotal.Store(10)
	f.errorsTotal.Store(1)
	f.lastFlushDur.Store(1500)

	s := f.Stats()
	if s.TotalFlushed != 42 {
		t.Errorf("expected TotalFlushed 42, got %d", s.TotalFlushed)
	}
	if s.TotalClaimed != 10 {
		t.Errorf("expected TotalClaimed 10, got %d", s.TotalClaimed)
	}
	if s.TotalErrors != 1 {
		t.Errorf("expected TotalErrors 1, got %d", s.TotalErrors)
	}
	if s.LastFlushMicros != 1500 {
		t.Errorf("expected LastFlushMicros 1500, got %d", s.LastFlushMicros)
	}
}
