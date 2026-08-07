package server

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/gonvex/gonvex/pkg/storage"
)

func TestStorageProxyPreservesByteRangeResponses(t *testing.T) {
	const (
		objectKey = "whagons/tenant/video.mp4"
		byteRange = "bytes=8-15"
	)
	payload := []byte("0123456789abcdefghijklmnopqrstuvwxyz")

	objectStore := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Range"); got != byteRange {
			t.Errorf("upstream Range = %q, want %q", got, byteRange)
		}
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Type", "video/mp4")
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 8-15/%d", len(payload)))
		w.Header().Set("Content-Length", "8")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(payload[8:16])
	}))
	defer objectStore.Close()

	factory := storage.NewFactory(storage.Config{
		Endpoint:        objectStore.URL,
		Region:          "us-east-1",
		Bucket:          "gonvex-test",
		AccessKeyID:     "test-key",
		SecretAccessKey: "test-secret",
		ForcePathStyle:  true,
		PublicBaseURL:   "https://runtime.example.com",
	})
	if factory == nil {
		t.Fatal("expected configured storage factory")
	}

	downloadURL, err := factory.DownloadURLForKey(objectKey)
	if err != nil {
		t.Fatalf("DownloadURLForKey: %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, downloadURL, nil)
	req.Header.Set("Range", byteRange)
	req.SetPathValue("key", objectKey)
	recorder := httptest.NewRecorder()

	server := &Server{storage: factory}
	server.handleStorageProxy(recorder, req)
	response := recorder.Result()
	defer response.Body.Close()

	if response.StatusCode != http.StatusPartialContent {
		t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusPartialContent)
	}
	if got := response.Header.Get("Accept-Ranges"); got != "bytes" {
		t.Errorf("Accept-Ranges = %q, want bytes", got)
	}
	wantContentRange := fmt.Sprintf("bytes 8-15/%d", len(payload))
	if got := response.Header.Get("Content-Range"); got != wantContentRange {
		t.Errorf("Content-Range = %q, want %q", got, wantContentRange)
	}
	if got := response.Header.Get("Content-Length"); got != strconv.Itoa(len(payload[8:16])) {
		t.Errorf("Content-Length = %q, want %d", got, len(payload[8:16]))
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read proxy response: %v", err)
	}
	if got, want := string(body), string(payload[8:16]); got != want {
		t.Errorf("body = %q, want %q", got, want)
	}
}
