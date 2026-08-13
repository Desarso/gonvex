package server

import (
	"net/http"
	"time"
)

const (
	defaultReadHeaderTimeout = 10 * time.Second
	defaultIdleTimeout       = 2 * time.Minute
	defaultMaxHeaderBytes    = 1 << 20
)

// NewHTTPServer applies connection-level safeguards without imposing a whole-
// request deadline on file uploads, streaming responses, or WebSockets.
func NewHTTPServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: defaultReadHeaderTimeout,
		IdleTimeout:       defaultIdleTimeout,
		MaxHeaderBytes:    defaultMaxHeaderBytes,
	}
}
