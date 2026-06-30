package server

import (
	"io"
	"net/http"
	"path"
	"strconv"
)

// maxUploadBytes caps a single storage-proxy upload. Frontends enforce their own
// per-feature limits; this is a backstop against unbounded request bodies.
const maxUploadBytes = 128 << 20 // 128 MiB

// handleStorageUpload terminates a Convex-style upload at the runtime: the
// browser POSTs the file bytes to a signed upload URL (issued by
// GenerateUploadURL when a public base URL is configured) and the runtime writes
// them to the (private) S3 endpoint, then returns { storageId } — the id used to
// reference the object later via getUrl/getFileUrls. Mirrors handleStorageProxy
// (download) so the browser never reaches the internal S3 host directly.
func (s *Server) handleStorageUpload(w http.ResponseWriter, r *http.Request) {
	if s.storage == nil {
		http.Error(w, "storage not configured", http.StatusNotFound)
		return
	}
	objectKey := r.PathValue("key")
	if objectKey == "" {
		http.NotFound(w, r)
		return
	}
	exp, _ := strconv.ParseInt(r.URL.Query().Get("exp"), 10, 64)
	if !s.storage.VerifyProxyPut(objectKey, exp, r.URL.Query().Get("sig")) {
		http.Error(w, "invalid or expired storage signature", http.StatusForbidden)
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxUploadBytes))
	if err != nil {
		http.Error(w, "upload too large or read failed", http.StatusRequestEntityTooLarge)
		return
	}

	if err := s.storage.UploadObject(r.Context(), objectKey, body, r.Header.Get("Content-Type")); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}

	// objectKey is <project>/<tenant>/<fileId>; the trailing segment is the
	// storage id the Convex client stores and resolves with getFileUrls.
	writeJSON(w, http.StatusOK, map[string]any{"storageId": path.Base(objectKey)})
}
