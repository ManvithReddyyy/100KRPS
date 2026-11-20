package handler

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"safepath-live/internal/domain"
	"safepath-live/internal/repository"
	"safepath-live/internal/service"
)

// StreamHandler manages Server-Sent Events (SSE) connections for live map tracking.
//
// Why SSE instead of WebSockets?
// For live tracking viewers, communication is purely one-way (server pushes location to browser).
// Server-Sent Events (SSE):
//  1. Runs over standard HTTP (port 80/443), passing cleanly through all corporate firewalls and NATs.
//  2. Eliminates WebSocket upgrade handshake overhead.
//  3. Built-in browser auto-reconnect via the standard EventSource API.
//  4. Substantially lower memory footprint per connection, allowing 100k+ concurrent viewers.
type StreamHandler struct {
	hub   *service.BroadcastHub
	redis *repository.RedisRepo
	pg    *repository.PostgresRepo
	log   *slog.Logger
}

// NewStreamHandler initializes the stream handler.
func NewStreamHandler(
	hub *service.BroadcastHub,
	redis *repository.RedisRepo,
	pg *repository.PostgresRepo,
	log *slog.Logger,
) *StreamHandler {
	return &StreamHandler{
		hub:   hub,
		redis: redis,
		pg:    pg,
		log:   log,
	}
}

// Live handles GET /v1/trips/{code}/live.
//
// Protocol & Lifecycle:
//
// Step 1: Set hardened SSE headers.
//   - "Content-Type: text/event-stream" tells the client this is a continuous streaming connection.
//   - "Cache-Control: no-cache, no-transform" ensures intermediary proxies never cache or modify chunks.
//   - "X-Accel-Buffering: no" is critical: it instructs NGINX reverse proxies NOT to buffer chunks,
//     ensuring viewers see GPS pings in true real time (<10ms).
//
// Step 2: Push initial trip snapshot immediately.
//   - As soon as a user opens the map, we send an "event: snapshot" containing destination,
//     arrival radius, and the last known position so the map initializes instantly without
//     waiting for the next GPS ping.
//
// Step 3: Check completion status.
//   - If the trip has already arrived, send an "event: arrived" and close cleanly.
//
// Step 4: Register with the shared in-memory BroadcastHub.
//   - Receives GPS updates via Go channels without creating redundant Redis subscriptions.
//
// Step 5: Event loop.
//   - Pushes live updates as "event: ping\ndata: {JSON}\n\n".
//   - Sends a heartbeat comment (": keepalive\n\n") every 15 seconds to prevent mobile carriers,
//     firewalls, and load balancers from terminating idle TCP connections.
//   - Terminates cleanly when the trip completes or when the user closes their browser tab.
func (h *StreamHandler) Live(w http.ResponseWriter, r *http.Request) {
	code := r.PathValue("code")
	if len(code) == 0 {
		http.Error(w, "missing trip code in path", http.StatusBadRequest)
		return
	}

	// Verify that the ResponseWriter supports flushing chunks immediately to the network
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported by client connection", http.StatusInternalServerError)
		return
	}

	// 1. Set SSE headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	// 2. Fetch current trip snapshot (check fast Redis first, fall back to Postgres)
	trip, err := h.redis.GetTrip(r.Context(), code)
	if err != nil {
		h.log.Warn("stream failed to get redis trip snapshot", "code", code, "error", err)
	}
	if trip == nil {
		trip, err = h.pg.GetTripByCode(r.Context(), code)
		if err != nil {
			h.log.Error("stream failed to get postgres trip snapshot", "code", code, "error", err)
		}
	}

	// If trip does not exist in either store, emit error event and exit
	if trip == nil {
		_, _ = fmt.Fprintf(w, "event: error\ndata: {\"error\":\"trip not found\"}\n\n")
		flusher.Flush()
		return
	}

	// Send initial snapshot event so the browser map can render markers immediately
	snapBytes, _ := json.Marshal(trip)
	_, _ = fmt.Fprintf(w, "event: snapshot\ndata: %s\n\n", snapBytes)
	flusher.Flush()

	// If the trip is already completed, inform the viewer and conclude the stream
	if trip.Status == domain.StatusArrived {
		_, _ = fmt.Fprintf(w, "event: arrived\ndata: {\"status\":\"ARRIVED\"}\n\n")
		flusher.Flush()
		return
	}

	// 3. Subscribe to the shared in-memory broadcast hub
	subCh, unsubscribe := h.hub.Subscribe(code)
	defer unsubscribe() // Guarantees cleanup when this HTTP request ends

	// 4. Heartbeat ticker (15 seconds)
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	// Detect when client closes connection or browser tab
	clientDone := r.Context().Done()

	// 5. Main event streaming loop
	for {
		select {
		case <-clientDone:
			// Browser tab was closed or network disconnected
			return

		case <-ticker.C:
			// SSE comment line (starts with colon ':'). Ignored by browser EventSource,
			// but keeps the underlying TCP socket active through proxy timeouts.
			if _, err := fmt.Fprintf(w, ": keepalive\n\n"); err != nil {
				return
			}
			flusher.Flush()

		case payload, ok := <-subCh:
			if !ok {
				// Subscriber channel was closed by the hub
				return
			}

			// Format and transmit standard SSE data frame
			if _, err := fmt.Fprintf(w, "event: ping\ndata: %s\n\n", payload); err != nil {
				return
			}
			flusher.Flush()

			// Check if this location ping triggered the arrival geofence
			var event domain.PingEvent
			if err := json.Unmarshal(payload, &event); err == nil {
				if event.Status == domain.StatusArrived {
					_, _ = fmt.Fprintf(w, "event: arrived\ndata: %s\n\n", payload)
					flusher.Flush()
					return // Trip finished; close stream cleanly
				}
			}
		}
	}
}
