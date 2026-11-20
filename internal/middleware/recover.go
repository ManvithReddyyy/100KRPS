package middleware

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"runtime/debug"
)

// Recoverer catches unhandled panics inside HTTP handlers, logs the stack trace,
// and returns a structured 500 Internal Server Error rather than dropping the socket.
func Recoverer(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rvr := recover(); rvr != nil {
					if rvr == http.ErrAbortHandler {
						panic(rvr)
					}

					stack := string(debug.Stack())
					log.Error("panic recovered in http handler",
						"panic", rvr,
						"path", r.URL.Path,
						"method", r.Method,
						"stack", stack,
					)

					w.Header().Set("Content-Type", "application/json; charset=utf-8")
					w.WriteHeader(http.StatusInternalServerError)
					_ = json.NewEncoder(w).Encode(map[string]string{
						"error": "internal server error",
					})
				}
			}()

			next.ServeHTTP(w, r)
		})
	}
}

// RequestID ensures every incoming HTTP request has a unique tracing ID.
// If the client provided an X-Request-ID header, it is preserved; otherwise,
// a fast 16-byte random hex ID is assigned.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := r.Header.Get("X-Request-ID")
		if reqID == "" {
			var b [16]byte
			_, _ = rand.Read(b[:])
			reqID = hex.EncodeToString(b[:])
		}

		w.Header().Set("X-Request-ID", reqID)
		next.ServeHTTP(w, r)
	})
}

// BodyLimit restricts the maximum readable request body to maxBytes (default 4096).
// In a 100k RPS environment, accepting oversized JSON payloads can quickly exhaust RAM.
func BodyLimit(maxBytes int64) func(http.Handler) http.Handler {
	if maxBytes <= 0 {
		maxBytes = 4096 // 4 KB is plenty for GPS coordinates
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
			next.ServeHTTP(w, r)
		})
	}
}
