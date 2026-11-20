package handler

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWebHandler_Index(t *testing.T) {
	h := NewWebHandler()

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rr := httptest.NewRecorder()

	h.Index(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}

	body := rr.Body.String()
	if !strings.Contains(body, "SafePath Live") {
		t.Errorf("expected HTML to contain 'SafePath Live', got: %s", body[:100])
	}
	if !strings.Contains(body, "leaflet") {
		t.Errorf("expected HTML to reference 'leaflet'")
	}
}

func TestWebHandler_Healthz(t *testing.T) {
	h := NewWebHandler()

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rr := httptest.NewRecorder()

	h.Healthz(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}
	if rr.Body.String() != "ok" {
		t.Fatalf("expected 'ok', got %q", rr.Body.String())
	}
}
