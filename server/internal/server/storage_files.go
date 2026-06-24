package server

import (
	"net/http"
	"path"
	"time"
)

type storageFileEntry struct {
	ID          string `json:"id"`
	Key         string `json:"key"`
	Size        int64  `json:"size"`
	ContentType string `json:"contentType,omitempty"`
	UploadedAt  string `json:"uploadedAt"`
	URL         string `json:"url,omitempty"`
}

// handleStorageFiles lists a project's objects straight from object storage so
// the dashboard Files tab reflects what's actually in the bucket, independent
// of the _gonvex_files metadata table. It lists at the project prefix so files
// across all of the project's tenants are included.
func (s *Server) handleStorageFiles(w http.ResponseWriter, r *http.Request) {
	if s.storage == nil {
		writeJSON(w, http.StatusOK, map[string]any{"configured": false, "files": []storageFileEntry{}})
		return
	}

	objects, err := s.storage.ListProjectFiles(r.Context(), projectID(r), "", 1000)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}

	files := make([]storageFileEntry, 0, len(objects))
	for _, object := range objects {
		downloadURL, _ := s.storage.DownloadURLForKey(object.Key)
		files = append(files, storageFileEntry{
			ID:         path.Base(object.Key),
			Key:        object.Key,
			Size:       object.Size,
			UploadedAt: object.LastModified.UTC().Format(time.RFC3339),
			URL:        downloadURL,
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"configured": true,
		"bucket":     s.storage.Bucket(),
		"files":      files,
	})
}
