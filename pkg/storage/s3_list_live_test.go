package storage

import (
	"context"
	"os"
	"testing"
	"time"
)

// TestListObjectsLive exercises ListObjects against a real S3-compatible
// backend. It is skipped unless MINIO_TEST_ENDPOINT is set, so it never runs in
// normal CI. Configure via MINIO_TEST_ENDPOINT, MINIO_TEST_BUCKET,
// MINIO_TEST_PREFIX, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY.
func TestListObjectsLive(t *testing.T) {
	endpoint := os.Getenv("MINIO_TEST_ENDPOINT")
	if endpoint == "" {
		t.Skip("set MINIO_TEST_ENDPOINT to run the live MinIO list test")
	}
	client := NewClient(Config{
		Endpoint:        endpoint,
		Region:          "us-east-1",
		Bucket:          os.Getenv("MINIO_TEST_BUCKET"),
		AccessKeyID:     os.Getenv("S3_ACCESS_KEY_ID"),
		SecretAccessKey: os.Getenv("S3_SECRET_ACCESS_KEY"),
		ForcePathStyle:  true,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	objects, err := client.ListObjects(ctx, os.Getenv("MINIO_TEST_PREFIX"), 5)
	if err != nil {
		t.Fatalf("list objects: %v", err)
	}
	t.Logf("listed %d objects", len(objects))
	for _, object := range objects {
		t.Logf("  %s  %d bytes  %s", object.Key, object.Size, object.LastModified.Format(time.RFC3339))
	}
	if len(objects) == 0 {
		t.Fatal("expected at least one object under the prefix")
	}
}
