package server

import (
	"compress/gzip"
	"net/http"
	"strings"
)

type gzipResponseWriter struct {
	http.ResponseWriter
	writer      *gzip.Writer
	wroteHeader bool
}

func (w *gzipResponseWriter) WriteHeader(status int) {
	if w.wroteHeader {
		return
	}
	w.wroteHeader = true
	header := w.Header()
	header.Del("content-length")
	header.Set("content-encoding", "gzip")
	appendVary(header, "Accept-Encoding")
	w.ResponseWriter.WriteHeader(status)
}

func (w *gzipResponseWriter) Write(data []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.writer.Write(data)
}

func withGzip(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.EqualFold(r.Header.Get("upgrade"), "websocket") || !strings.Contains(r.Header.Get("accept-encoding"), "gzip") || r.Method == http.MethodOptions {
			next.ServeHTTP(w, r)
			return
		}

		writer := gzip.NewWriter(w)
		defer writer.Close()
		next.ServeHTTP(&gzipResponseWriter{ResponseWriter: w, writer: writer}, r)
	})
}

func appendVary(header http.Header, value string) {
	current := header.Get("vary")
	if current == "" {
		header.Set("vary", value)
		return
	}
	for _, part := range strings.Split(current, ",") {
		if strings.EqualFold(strings.TrimSpace(part), value) {
			return
		}
	}
	header.Set("vary", current+", "+value)
}
