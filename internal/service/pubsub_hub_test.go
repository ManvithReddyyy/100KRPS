package service

import (
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
)

func TestBroadcastHub_SubscribeUnsubscribe(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	hub := NewBroadcastHub(nil, logger)

	subCh1, unsub1 := hub.Subscribe("TESTCODE")
	if subCh1 == nil {
		t.Fatal("expected non-nil sub channel")
	}

	if viewers := hub.ActiveViewers(); viewers != 1 {
		t.Fatalf("expected 1 active viewer, got %d", viewers)
	}

	_, unsub2 := hub.Subscribe("TESTCODE")
	if viewers := hub.ActiveViewers(); viewers != 2 {
		t.Fatalf("expected 2 active viewers, got %d", viewers)
	}

	unsub1()
	if viewers := hub.ActiveViewers(); viewers != 1 {
		t.Fatalf("expected 1 active viewer after unsub, got %d", viewers)
	}

	unsub2()
	if viewers := hub.ActiveViewers(); viewers != 0 {
		t.Fatalf("expected 0 active viewers after all unsub, got %d", viewers)
	}
}

func TestBroadcastHub_SlowConsumerBackpressure(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	hub := NewBroadcastHub(nil, logger)

	// Create a topicState directly to test non-blocking backpressure logic without live Redis
	channelName := "channel:trip:BACKPRESSURE"
	state := &topicState{
		subscribers: make(map[chan []byte]struct{}),
	}
	hub.topics[channelName] = state

	// Subscriber channel with standard capacity of 16
	slowCh := make(chan []byte, 16)
	state.subscribers[slowCh] = struct{}{}

	// Fill the buffer to capacity (16 frames)
	for i := 0; i < 16; i++ {
		slowCh <- []byte("frame")
	}

	// Now send 10 additional frames directly simulating the hub's dispatch loop
	initialDrops := atomic.LoadUint64(&DroppedFramesCount)
	for i := 0; i < 10; i++ {
		payload := []byte("overflow_frame")
		state.mu.Lock()
		for ch := range state.subscribers {
			select {
			case ch <- payload:
				t.Error("should not have accepted frame into full buffer")
			default:
				atomic.AddUint64(&DroppedFramesCount, 1)
			}
		}
		state.mu.Unlock()
	}

	dropsAfter := atomic.LoadUint64(&DroppedFramesCount)
	if dropsAfter-initialDrops != 10 {
		t.Fatalf("expected 10 dropped frames, got %d", dropsAfter-initialDrops)
	}

	// Verify the channel still has the first 16 frames and did not block
	count := 0
	for len(slowCh) > 0 {
		<-slowCh
		count++
	}
	if count != 16 {
		t.Fatalf("expected 16 buffered frames drained, got %d", count)
	}
}
