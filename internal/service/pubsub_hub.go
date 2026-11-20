// Package service provides internal background engines, including the Pub/Sub
// broadcast hub for real-time map viewers and the batch flusher for durable persistence.
package service

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/redis/rueidis"
)

// DroppedFramesCount tracks how many location update frames were dropped because
// a viewer's connection was too slow to accept them in time.
// Exported as a Prometheus metric to monitor subscriber health.
var DroppedFramesCount uint64

// BroadcastHub multiplexes Redis Pub/Sub channels to in-memory viewer channels.
//
// Why is BroadcastHub crucial for 100k+ RPS?
// Imagine 5,000 friends and family members connect to watch a live marathon runner.
// If every SSE viewer created their own direct Redis subscription, Redis would be overwhelmed
// managing 5,000 separate TCP subscriber streams for the exact same data.
//
// With BroadcastHub:
//  1. The first viewer triggers ONE Redis subscription for "channel:trip:{code}".
//  2. When Redis publishes an update, BroadcastHub receives it once and fans it out in Go RAM
//     to all 5,000 local viewer channels in sub-millisecond time.
//  3. When the last viewer disconnects, BroadcastHub automatically cancels the Redis subscription,
//     reclaiming network and CPU resources.
type BroadcastHub struct {
	client rueidis.Client
	log    *slog.Logger

	mu     sync.RWMutex
	topics map[string]*topicState
}

// topicState tracks all active in-memory subscriber channels for one specific trip.
type topicState struct {
	mu          sync.Mutex
	subscribers map[chan []byte]struct{}
	cancel      context.CancelFunc
}

// NewBroadcastHub initializes the in-memory Pub/Sub hub.
func NewBroadcastHub(client rueidis.Client, log *slog.Logger) *BroadcastHub {
	return &BroadcastHub{
		client: client,
		log:    log,
		topics: make(map[string]*topicState),
	}
}

// Subscribe attaches a new SSE viewer to a trip's real-time broadcast stream.
//
// Parameters:
//   - shareCode: The 8-character trip identifier.
//
// Returns:
//   - <-chan []byte: A read-only channel delivering JSON location frames.
//   - func(): A cleanup function the caller MUST invoke when the viewer disconnects.
//
// Backpressure Architecture:
// The returned channel has a bounded capacity of 16. If a slow consumer cannot keep up,
// the hub drops stale frames instead of stalling other viewers or exhausting server memory.
func (h *BroadcastHub) Subscribe(shareCode string) (<-chan []byte, func()) {
	channelName := "channel:trip:" + shareCode
	subCh := make(chan []byte, 16) // bounded buffer for slow-consumer backpressure

	h.mu.Lock()
	state, exists := h.topics[channelName]
	if !exists {
		// First viewer for this trip: initialize topic state and open 1 Redis subscription
		ctx, cancel := context.WithCancel(context.Background())
		state = &topicState{
			subscribers: make(map[chan []byte]struct{}),
			cancel:      cancel,
		}
		h.topics[channelName] = state
		go h.listenRedis(ctx, channelName, state)
	}
	h.mu.Unlock()

	// Register this viewer's channel
	state.mu.Lock()
	state.subscribers[subCh] = struct{}{}
	state.mu.Unlock()

	// Cleanup callback to be deferred by the HTTP handler
	cleanup := func() {
		h.unsubscribe(channelName, subCh)
	}

	return subCh, cleanup
}

// unsubscribe removes a viewer's channel and tears down the Redis subscription
// if no other local viewers remain on this topic.
func (h *BroadcastHub) unsubscribe(channelName string, subCh chan []byte) {
	h.mu.Lock()
	state, exists := h.topics[channelName]
	if !exists {
		h.mu.Unlock()
		return
	}

	state.mu.Lock()
	delete(state.subscribers, subCh)
	remaining := len(state.subscribers)
	state.mu.Unlock()

	// If nobody else is watching this trip on this Go node, stop the Redis listener
	if remaining == 0 {
		delete(h.topics, channelName)
		state.cancel()
	}
	h.mu.Unlock()

	// Safely close the channel so the SSE handler loop terminates
	close(subCh)
}

// listenRedis connects to Redis Pub/Sub for a given channel and fans out incoming messages.
func (h *BroadcastHub) listenRedis(ctx context.Context, channelName string, state *topicState) {
	if h.client == nil {
		// Nil client in unit test mode; do not attempt network subscribe
		return
	}

	h.log.Debug("opening shared redis pub/sub subscription", "channel", channelName)

	err := h.client.Receive(ctx, h.client.B().Subscribe().Channel(channelName).Build(), func(msg rueidis.PubSubMessage) {
		payload := []byte(msg.Message)

		state.mu.Lock()
		defer state.mu.Unlock()

		for ch := range state.subscribers {
			select {
			case ch <- payload:
				// Frame delivered into the subscriber's buffer successfully
			default:
				// SLOW-CONSUMER BACKPRESSURE RULE:
				// If the viewer's 16-slot buffer is completely full (e.g. mobile client lagging
				// on 2G or frozen background tab), DROP the stale frame immediately!
				//
				// Why?
				// 1. In live GPS tracking, a 5-second-old coordinate is useless — fresh coordinates matter.
				// 2. Blocking here would stall the Redis reader and starve every other connected viewer.
				// 3. Creating an unbounded queue would cause an Out-Of-Memory (OOM) crash under load.
				atomic.AddUint64(&DroppedFramesCount, 1)
			}
		}
	})

	if err != nil && ctx.Err() == nil {
		h.log.Warn("redis pub/sub subscription closed unexpectedly", "channel", channelName, "error", err)
	}
}

// ActiveViewers counts the total number of connected SSE viewer channels across all trips.
func (h *BroadcastHub) ActiveViewers() int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	total := 0
	for _, t := range h.topics {
		t.mu.Lock()
		total += len(t.subscribers)
		t.mu.Unlock()
	}
	return total
}
