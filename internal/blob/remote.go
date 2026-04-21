package blob

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// RemoteBackend is implemented by object stores (S3, GCS, R2, MinIO).
// blob.Manager calls it when a WithCoord deployment needs cross-node file access.
//
// The path returned by Put is stored in Ref.Path and must be a full URL
// (e.g. "s3://bucket/pepper-blobs/blob_xyz"). Workers on other nodes
// call Get with that URL to materialise the blob locally.
type RemoteBackend interface {
	// Put uploads r to the object store under id.
	// Returns the full URL that becomes Ref.Path.
	Put(ctx context.Context, id string, r io.Reader, size int64) (path string, err error)

	// Get returns a reader for the object at path.
	// The caller is responsible for closing the reader.
	Get(ctx context.Context, path string) (io.ReadCloser, error)

	// Delete removes the object at path. Best-effort; errors are logged
	// but do not block caller.
	Delete(ctx context.Context, path string) error
}

// S3Backend implements RemoteBackend using pre-signed HTTP PUTs and GETs.
// It works with AWS S3, MinIO, Cloudflare R2, or any S3-compatible service
// without importing the AWS SDK — presigned URLs are generated externally
// (e.g. by your infrastructure layer or a small signing sidecar).
//
// For a full production deployment, replace PresignFunc with your own
// signing logic or swap in the AWS SDK implementation.
type S3Backend struct {
	// Bucket is the S3 bucket name.
	Bucket string
	// BaseURL is the base URL of the object store (e.g. "https://s3.amazonaws.com").
	// Objects are addressed as BaseURL/Bucket/Key.
	BaseURL string
	// KeyPrefix is prepended to every blob ID (default: "pepper-blobs/").
	KeyPrefix string
	// PresignFunc, if set, is called to generate a presigned PUT URL.
	// If nil, unsigned PUT is attempted (only works with public buckets or
	// services that accept anonymous writes, e.g. local MinIO in dev).
	PresignFunc func(ctx context.Context, bucket, key, method string) (url string, headers map[string]string, err error)
	// Client is the HTTP client used for all requests. Defaults to http.DefaultClient.
	Client *http.Client
}

func (s *S3Backend) prefix() string {
	if s.KeyPrefix != "" {
		return s.KeyPrefix
	}
	return "pepper-blobs/"
}

func (s *S3Backend) client() *http.Client {
	if s.Client != nil {
		return s.Client
	}
	return http.DefaultClient
}

func (s *S3Backend) objectURL(id string) string {
	key := s.prefix() + id
	return strings.TrimRight(s.BaseURL, "/") + "/" + s.Bucket + "/" + key
}

// Put uploads r to S3 and returns "s3://bucket/prefix/id" as the Ref.Path.
func (s *S3Backend) Put(ctx context.Context, id string, r io.Reader, size int64) (string, error) {
	key := s.prefix() + id
	var putURL string
	var headers map[string]string

	if s.PresignFunc != nil {
		var err error
		putURL, headers, err = s.PresignFunc(ctx, s.Bucket, key, "PUT")
		if err != nil {
			return "", fmt.Errorf("blob/s3: presign PUT: %w", err)
		}
	} else {
		putURL = s.objectURL(id)
	}

	req, err := http.NewRequestWithContext(ctx, "PUT", putURL, r)
	if err != nil {
		return "", fmt.Errorf("blob/s3: PUT request: %w", err)
	}
	if size > 0 {
		req.ContentLength = size
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := s.client().Do(req)
	if err != nil {
		return "", fmt.Errorf("blob/s3: PUT: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return "", fmt.Errorf("blob/s3: PUT %d: %s", resp.StatusCode, body)
	}

	// Canonical Ref.Path: s3://bucket/key
	return "s3://" + s.Bucket + "/" + key, nil
}

// Get returns a reader for the object at path ("s3://bucket/key").
func (s *S3Backend) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	// path is "s3://bucket/key" — map back to HTTP URL.
	withoutScheme := strings.TrimPrefix(path, "s3://")
	slash := strings.Index(withoutScheme, "/")
	if slash < 0 {
		return nil, fmt.Errorf("blob/s3: Get: malformed path %q", path)
	}
	key := withoutScheme[slash+1:]

	var getURL string
	if s.PresignFunc != nil {
		bucket := withoutScheme[:slash]
		var err error
		getURL, _, err = s.PresignFunc(ctx, bucket, key, "GET")
		if err != nil {
			return nil, fmt.Errorf("blob/s3: presign GET: %w", err)
		}
	} else {
		getURL = strings.TrimRight(s.BaseURL, "/") + "/" + withoutScheme
	}

	req, err := http.NewRequestWithContext(ctx, "GET", getURL, nil)
	if err != nil {
		return nil, fmt.Errorf("blob/s3: GET request: %w", err)
	}
	resp, err := s.client().Do(req)
	if err != nil {
		return nil, fmt.Errorf("blob/s3: GET: %w", err)
	}
	if resp.StatusCode >= 300 {
		resp.Body.Close()
		return nil, fmt.Errorf("blob/s3: GET %d for %s", resp.StatusCode, path)
	}
	return resp.Body, nil
}

// Delete removes the object at path. Errors are returned but callers
// should treat them as best-effort.
func (s *S3Backend) Delete(ctx context.Context, path string) error {
	withoutScheme := strings.TrimPrefix(path, "s3://")
	slash := strings.Index(withoutScheme, "/")
	if slash < 0 {
		return fmt.Errorf("blob/s3: Delete: malformed path %q", path)
	}

	var delURL string
	if s.PresignFunc != nil {
		bucket := withoutScheme[:slash]
		key := withoutScheme[slash+1:]
		var err error
		delURL, _, err = s.PresignFunc(ctx, bucket, key, "DELETE")
		if err != nil {
			return fmt.Errorf("blob/s3: presign DELETE: %w", err)
		}
	} else {
		delURL = strings.TrimRight(s.BaseURL, "/") + "/" + withoutScheme
	}

	req, err := http.NewRequestWithContext(ctx, "DELETE", delURL, nil)
	if err != nil {
		return err
	}
	resp, err := s.client().Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode >= 300 && resp.StatusCode != 404 {
		return fmt.Errorf("blob/s3: DELETE %d for %s", resp.StatusCode, path)
	}
	return nil
}

// Fetch downloads a remote blob to the local Manager's blob directory.
// Called by workers receiving a Ref where IsRemote() is true.
// Returns a new Ref with Path set to the local file path.
func Fetch(ctx context.Context, remote RemoteBackend, ref Ref, m *Manager) (Ref, error) {
	if ref.IsLocal() {
		return ref, nil
	}
	r, err := remote.Get(ctx, ref.Path)
	if err != nil {
		return Ref{}, fmt.Errorf("blob.Fetch %s: %w", ref.ID, err)
	}
	defer r.Close()

	local, err := m.WriteFromReader(r, 30*time.Minute,
		WithDType(ref.DType),
		WithFormat(ref.Format),
	)
	if err != nil {
		return Ref{}, fmt.Errorf("blob.Fetch %s: local write: %w", ref.ID, err)
	}
	local.ref.ID = ref.ID // preserve original ID
	return local.Ref(), nil
}
