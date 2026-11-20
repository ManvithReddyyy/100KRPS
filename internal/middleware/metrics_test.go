package middleware

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetricsCollector_MiddlewareAndExposition(t *testing.T) {
	col := &MetricsCollector{}
	mw := col.Middleware

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/fail" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))

	// Hit with 200 OK
	req1 := httptest.NewRequest(http.MethodPost, "/v1/trips/7X8K2M9P/ping", nil)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	// Hit with 404
	req2 := httptest.NewRequest(http.MethodGet, "/fail", nil)
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	if col.totalRequests.Load() != 2 {
		t.Errorf("expected 2 total requests, got %d", col.totalRequests.Load())
	}
	if col.status2xx.Load() != 1 {
		t.Errorf("expected 1 2xx request, got %d", col.status2xx.Load())
	}
	if col.status4xx.Load() != 1 {
		t.Errorf("expected 1 4xx request, got %d", col.status4xx.Load())
	}
	if col.pingRequests.Load() != 1 {
		t.Errorf("expected 1 ping request, got %d", col.pingRequests.Load())
	}

	// Test exposition
	reqMetrics := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	recMetrics := httptest.NewRecorder()
	col.ExposePrometheus(recMetrics, reqMetrics)

	if recMetrics.Code != http.StatusOK {
		t.Errorf("expected 200 from /metrics, got %d", recMetrics.Code)
	}

	output := recMetrics.Body.String()
	expectedSubstrings := []string{
		"http_requests_total{code=\"2xx\"} 1",
		"http_requests_total{code=\"4xx\"} 1",
		"safepath_pings_total 1",
		"http_request_duration_seconds_bucket{le=\"+Inf\"} 2",
		"http_request_duration_seconds_count 2",
	}

	for _, sub := range expectedSubstrings {
		if !strings.Contains(output, sub) {
			t.Errorf("expected output to contain %q, but got:\n%s", sub, output)
		}
	}
}
