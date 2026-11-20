package handler

import (
	"net/http"

	"safepath-live/web"
)

// WebHandler serves the embedded single-page application and system health checks.
type WebHandler struct{}

// NewWebHandler creates a new web handler instance.
func NewWebHandler() *WebHandler {
	return &WebHandler{}
}

// Index serves the embedded Leaflet.js real-time tracking map (web/index.html).
// Because the HTML, CSS, and JS are embedded directly into the Go binary at compile time,
// this allows the entire service to be deployed as a single self-contained binary
// with zero external file system dependencies.
func (h *WebHandler) Index(w http.ResponseWriter, r *http.Request) {
	// Only serve on root path "/"
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(web.IndexHTML)
}

// Healthz responds with a lightweight 200 OK text response.
// Used by container orchestrators (Kubernetes, Docker healthchecks, NGINX) to verify
// that this replica is alive and ready to receive traffic.
func (h *WebHandler) Healthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}
