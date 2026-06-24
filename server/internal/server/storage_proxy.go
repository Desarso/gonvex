package server

import (
	"io"
	"net/http"
	"strconv"
)

// handleStorageProxy streams a stored object to the browser. GetURL hands out
// signed URLs like /storage/<project>/<tenant>/<fileId>?exp=&sig= so the file is
// reachable over the runtime's public hostname while MinIO stays private. The
// signature (HMAC over object key + expiry) is the authorization, mirroring how
// the presigned S3 URLs worked — just terminated at the runtime instead.
func (s *Server) handleStorageProxy(w http.ResponseWriter, r *http.Request) {
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
	if !s.storage.VerifyProxyGet(objectKey, exp, r.URL.Query().Get("sig")) {
		http.Error(w, "invalid or expired storage signature", http.StatusForbidden)
		return
	}

	resp, err := s.storage.FetchObject(r.Context(), objectKey)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer drainClose(resp.Body)
	if resp.StatusCode != http.StatusOK {
		http.Error(w, "object not found", http.StatusNotFound)
		return
	}

	if ct := resp.Header.Get("Content-Type"); ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	if cl := resp.Header.Get("Content-Length"); cl != "" {
		w.Header().Set("Content-Length", cl)
	}
	if et := resp.Header.Get("ETag"); et != "" {
		w.Header().Set("ETag", et)
	}
	w.Header().Set("Cache-Control", "private, max-age=300")
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, resp.Body)
}

func drainClose(body io.ReadCloser) {
	if body == nil {
		return
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(body, 1<<16))
	_ = body.Close()
}
