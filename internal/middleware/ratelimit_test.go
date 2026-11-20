package middleware

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestExtractClientIP(t *testing.T) {
	tests := []struct {
		name       string
		headers    map[string]string
		remoteAddr string
		expected   string
	}{
		{
			name: "from X-Forwarded-For single",
			headers: map[string]string{
				"X-Forwarded-For": "203.0.113.195",
			},
			remoteAddr: "10.0.0.1:1234",
			expected:   "203.0.113.195",
		},
		{
			name: "from X-Forwarded-For multiple takes first",
			headers: map[string]string{
				"X-Forwarded-For": "203.0.113.195, 70.41.3.18, 150.172.238.178",
			},
			remoteAddr: "10.0.0.1:1234",
			expected:   "203.0.113.195",
		},
		{
			name: "from X-Real-IP",
			headers: map[string]string{
				"X-Real-IP": "198.51.100.1",
			},
			remoteAddr: "10.0.0.1:1234",
			expected:   "198.51.100.1",
		},
		{
			name:       "from RemoteAddr hostport",
			headers:    nil,
			remoteAddr: "192.168.1.50:54321",
			expected:   "192.168.1.50",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.RemoteAddr = tt.remoteAddr
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}

			ip := extractClientIP(req)
			if ip != tt.expected {
				t.Errorf("expected IP %s, got %s", tt.expected, ip)
			}
		})
	}
}

func TestWriteRateLimitExceeded(t *testing.T) {
	rec := httptest.NewRecorder()
	writeRateLimitExceeded(rec, 5)

	if rec.Code != http.StatusTooManyRequests {
		t.Errorf("expected status 429, got %d", rec.Code)
	}

	if ra := rec.Header().Get("Retry-After"); ra != "5" {
		t.Errorf("expected Retry-After 5, got %s", ra)
	}

	var body map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode json body: %v", err)
	}

	if body["error"] != "rate limit exceeded" {
		t.Errorf("expected error message, got %v", body["error"])
	}
	if body["retry_after_seconds"] != float64(5) {
		t.Errorf("expected retry_after_seconds 5, got %v", body["retry_after_seconds"])
	}
}

func TestRateLimiter_LimitByIP_FailOpen(t *testing.T) {
	rl := NewRateLimiter(nil, nil)
	mw := rl.LimitByIP(60, 1.0)

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))

	req := httptest.NewRequest(http.MethodGet, "/v1/trips", nil)
	req.RemoteAddr = "127.0.0.1:8080"
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 OK when failing open, got %d", rec.Code)
	}
	if rec.Header().Get("X-RateLimit-Remaining") != "60" {
		t.Errorf("expected remaining 60, got %s", rec.Header().Get("X-RateLimit-Remaining"))
	}
}
