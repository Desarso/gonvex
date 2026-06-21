package gonvextest

import (
	"time"

	"github.com/gonvex/gonvex/pkg/gonvex"
)

// File storage handlers. These mirror the Convex storage flow: the client asks
// for an upload URL, PUTs the bytes directly to object storage, then references
// the returned file id from app rows.

type CreateUploadURLArgs struct {
	ContentType string `json:"contentType"`
	Size        int64  `json:"size,omitempty"`
}

type CreateUploadURLResult struct {
	FileID  string            `json:"fileId"`
	URL     string            `json:"url"`
	Method  string            `json:"method"`
	Headers map[string]string `json:"headers,omitempty"`
}

type FileIDArgs struct {
	FileID string `json:"fileId"`
}

type GetFileURLResult struct {
	URL string `json:"url"`
}

type DeleteFileResult struct {
	Deleted bool `json:"deleted"`
}

func RegisterFiles(app *gonvex.App) {
	app.Mutation("files.createUploadUrl", CreateUploadURL)
	app.Query("files.getUrl", GetFileURL)
	app.Query("files.getMetadata", GetFileMetadata)
	app.Mutation("files.delete", DeleteFile)
}

// CreateUploadURL records a pending file and returns a short-lived presigned
// PUT URL the client uploads the bytes to.
func CreateUploadURL(ctx *gonvex.MutationCtx, args CreateUploadURLArgs) (CreateUploadURLResult, error) {
	target, err := ctx.Storage.GenerateUploadURL(gonvex.UploadOptions{
		ContentType: args.ContentType,
		Size:        args.Size,
	})
	if err != nil {
		return CreateUploadURLResult{}, err
	}
	return CreateUploadURLResult{
		FileID:  target.FileID,
		URL:     target.URL,
		Method:  target.Method,
		Headers: target.Headers,
	}, nil
}

// GetFileURL returns a URL the caller can use to download the file. Private and
// tenant files get a short-lived signed URL; public files get a stable URL.
func GetFileURL(ctx *gonvex.QueryCtx, args FileIDArgs) (GetFileURLResult, error) {
	url, err := ctx.Storage.GenerateDownloadURL(args.FileID, 10*time.Minute)
	if err != nil {
		return GetFileURLResult{}, err
	}
	return GetFileURLResult{URL: url}, nil
}

// GetFileMetadata returns the stored metadata record, finalizing the upload if
// the bytes have arrived since the URL was issued.
func GetFileMetadata(ctx *gonvex.QueryCtx, args FileIDArgs) (gonvex.FileMetadata, error) {
	return ctx.Storage.GetMetadata(args.FileID)
}

// DeleteFile removes the object and its metadata record.
func DeleteFile(ctx *gonvex.MutationCtx, args FileIDArgs) (DeleteFileResult, error) {
	if err := ctx.Storage.Delete(args.FileID); err != nil {
		return DeleteFileResult{}, err
	}
	return DeleteFileResult{Deleted: true}, nil
}
