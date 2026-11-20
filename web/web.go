// Package web bundles and embeds frontend static assets directly into the compiled Go binary.
//
// By using Go's standard //go:embed directive, the single-page Leaflet.js web application
// is compiled into the executable itself. This eliminates the need to distribute separate
// static file folders, set up NGINX static file roots, or configure external S3/CDN buckets
// for the local development and benchmark environments.
package web

import _ "embed"

// IndexHTML contains the complete raw UTF-8 content of web/index.html.
//
//go:embed index.html
var IndexHTML []byte
