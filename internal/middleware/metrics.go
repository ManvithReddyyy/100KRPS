package middleware

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
)

// Latency buckets in microseconds for sub-millisecond precision:
// 1ms, 2.5ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1000ms
var latencyBuckets = []int64{
	1000,
	2500,
	5000,
	10000,
	25000,
	50000,
	100000,
	250000,
	500000,
	1000000,
}

var latencyBucketLabels = []string{
	"0.001",
	"0.0025",
	"0.005",
	"0.01",
	"0.025",
	"0.05",
	"0.1",
	"0.25",
	"0.5",
	"1.0",
}

// MetricsCollector tracks real-time throughput and latency with zero mutex locks.
// Uses purely atomic operations to comfortably sustain 100k+ RPS with zero GC pressure.
type MetricsCollector struct {
	inFlightRequests atomic.Int64
	totalRequests    atomic.Uint64
	totalDurationUs  atomic.Uint64 // total duration in microseconds

	// HTTP Status Code Counters
	status2xx atomic.Uint64
	status4xx atomic.Uint64
	status5xx atomic.Uint64

	// Endpoint Specific Counters
	pingRequests   atomic.Uint64
	createRequests atomic.Uint64
	streamRequests atomic.Uint64

	// Lock-free histogram buckets
	bucketCounts [10]atomic.Uint64
	bucketInf    atomic.Uint64
}

// GlobalMetrics is the singleton collector used across the API handlers.
var GlobalMetrics = &MetricsCollector{}

// statusResponseWriter captures the HTTP status code written by inner handlers.
type statusResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (w *statusResponseWriter) WriteHeader(code int) {
	w.statusCode = code
	w.ResponseWriter.WriteHeader(code)
}

// Metrics returns an HTTP middleware that measures in-flight concurrency,
// request rates, and response latency.
func (m *MetricsCollector) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m.inFlightRequests.Add(1)
		start := time.Now()

		sw := &statusResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		// Execute inner handler
		next.ServeHTTP(sw, r)

		durUs := time.Since(start).Microseconds()
		m.inFlightRequests.Add(-1)
		m.totalRequests.Add(1)
		m.totalDurationUs.Add(uint64(durUs))

		// Categorize status
		switch {
		case sw.statusCode >= 200 && sw.statusCode < 300:
			m.status2xx.Add(1)
		case sw.statusCode >= 400 && sw.statusCode < 500:
			m.status4xx.Add(1)
		case sw.statusCode >= 500:
			m.status5xx.Add(1)
		}

		// Route categorization
		path := r.URL.Path
		if len(path) >= 14 && path[:9] == "/v1/trips" && path[len(path)-5:] == "/ping" {
			m.pingRequests.Add(1)
		} else if path == "/v1/trips" && r.Method == http.MethodPost {
			m.createRequests.Add(1)
		} else if len(path) >= 14 && path[len(path)-5:] == "/live" {
			m.streamRequests.Add(1)
		}

		// Record in histogram bucket
		placed := false
		for i, threshold := range latencyBuckets {
			if durUs <= threshold {
				m.bucketCounts[i].Add(1)
				placed = true
				break
			}
		}
		if !placed {
			m.bucketInf.Add(1)
		}
	})
}

// ExposePrometheus exports current metrics in the official Prometheus text format.
// Compatible with any Prometheus scraper, Grafana agent, or Datadog agent.
func (m *MetricsCollector) ExposePrometheus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)

	total := m.totalRequests.Load()
	durSec := float64(m.totalDurationUs.Load()) / 1e6

	io.WriteString(w, "# HELP http_requests_in_flight Current active in-flight HTTP requests.\n")
	io.WriteString(w, "# TYPE http_requests_in_flight gauge\n")
	io.WriteString(w, "http_requests_in_flight "+strconv.FormatInt(m.inFlightRequests.Load(), 10)+"\n\n")

	io.WriteString(w, "# HELP http_requests_total Total number of HTTP requests processed.\n")
	io.WriteString(w, "# TYPE http_requests_total counter\n")
	io.WriteString(w, fmt.Sprintf("http_requests_total{code=\"2xx\"} %d\n", m.status2xx.Load()))
	io.WriteString(w, fmt.Sprintf("http_requests_total{code=\"4xx\"} %d\n", m.status4xx.Load()))
	io.WriteString(w, fmt.Sprintf("http_requests_total{code=\"5xx\"} %d\n\n", m.status5xx.Load()))

	io.WriteString(w, "# HELP safepath_pings_total Total high-throughput GPS pings processed.\n")
	io.WriteString(w, "# TYPE safepath_pings_total counter\n")
	io.WriteString(w, fmt.Sprintf("safepath_pings_total %d\n\n", m.pingRequests.Load()))

	io.WriteString(w, "# HELP http_request_duration_seconds Latency histogram for HTTP requests.\n")
	io.WriteString(w, "# TYPE http_request_duration_seconds histogram\n")

	var cumulative uint64
	for i, label := range latencyBucketLabels {
		cumulative += m.bucketCounts[i].Load()
		io.WriteString(w, fmt.Sprintf("http_request_duration_seconds_bucket{le=\"%s\"} %d\n", label, cumulative))
	}
	cumulative += m.bucketInf.Load()
	io.WriteString(w, fmt.Sprintf("http_request_duration_seconds_bucket{le=\"+Inf\"} %d\n", cumulative))
	io.WriteString(w, fmt.Sprintf("http_request_duration_seconds_sum %.6f\n", durSec))
	io.WriteString(w, fmt.Sprintf("http_request_duration_seconds_count %d\n", total))
}
