/*
Copyright 2025 The Crossplane Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package s3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// fakeAPI records what Client sends to S3.
type fakeAPI struct {
	puts      map[string][]byte
	parts     [][]byte
	created   int
	completed int
	aborted   int
}

func newFakeAPI() *fakeAPI { return &fakeAPI{puts: map[string][]byte{}} }

func (f *fakeAPI) PutObject(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	b, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}
	f.puts[aws.ToString(in.Key)] = b
	return &s3.PutObjectOutput{}, nil
}

func (f *fakeAPI) CreateMultipartUpload(_ context.Context, _ *s3.CreateMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	f.created++
	return &s3.CreateMultipartUploadOutput{UploadId: aws.String("upload")}, nil
}

func (f *fakeAPI) UploadPart(_ context.Context, in *s3.UploadPartInput, _ ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	b, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}
	if int(aws.ToInt32(in.PartNumber)) != len(f.parts)+1 {
		return nil, errors.New("parts uploaded out of order")
	}
	f.parts = append(f.parts, b)
	return &s3.UploadPartOutput{ETag: aws.String("etag")}, nil
}

func (f *fakeAPI) CompleteMultipartUpload(_ context.Context, in *s3.CompleteMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	if len(in.MultipartUpload.Parts) != len(f.parts) {
		return nil, errors.New("completed part list does not match uploaded parts")
	}
	f.completed++
	return &s3.CompleteMultipartUploadOutput{}, nil
}

func (f *fakeAPI) AbortMultipartUpload(_ context.Context, _ *s3.AbortMultipartUploadInput, _ ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	f.aborted++
	return &s3.AbortMultipartUploadOutput{}, nil
}

func (f *fakeAPI) ListObjectsV2(_ context.Context, _ *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	return &s3.ListObjectsV2Output{}, nil
}

func (f *fakeAPI) DeleteObjects(_ context.Context, _ *s3.DeleteObjectsInput, _ ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	return &s3.DeleteObjectsOutput{}, nil
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, errors.New("boom") }

func TestUpload(t *testing.T) {
	cases := map[string]struct {
		size      int
		wantParts []int // nil means a single PutObject
	}{
		"empty":             {size: 0},
		"smaller than part": {size: 10},
		"exactly one part":  {size: partSize, wantParts: []int{partSize}},
		"several parts":     {size: 2*partSize + 7, wantParts: []int{partSize, partSize, 7}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			f := newFakeAPI()
			c := &Client{s3: f, bucket: "b"}
			body := bytes.Repeat([]byte{'x'}, tc.size)

			n, err := c.Upload(context.Background(), "k", bytes.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			if n != int64(tc.size) {
				t.Fatalf("size = %d, want %d", n, tc.size)
			}
			if tc.wantParts == nil {
				if len(f.puts["k"]) != tc.size || f.created != 0 {
					t.Fatalf("expected a single PutObject of %d bytes, got puts=%d created=%d", tc.size, len(f.puts["k"]), f.created)
				}
				return
			}
			if f.completed != 1 || len(f.parts) != len(tc.wantParts) {
				t.Fatalf("expected %d completed parts, got completed=%d parts=%d", len(tc.wantParts), f.completed, len(f.parts))
			}
			for i, want := range tc.wantParts {
				if len(f.parts[i]) != want {
					t.Fatalf("part %d has %d bytes, want %d", i+1, len(f.parts[i]), want)
				}
			}
		})
	}
}

func TestUpload_ReadErrorAbortsMultipartUpload(t *testing.T) {
	f := newFakeAPI()
	c := &Client{s3: f, bucket: "b"}
	r := io.MultiReader(bytes.NewReader(bytes.Repeat([]byte{'x'}, partSize+1)), failingReader{})

	if _, err := c.Upload(context.Background(), "k", r); err == nil {
		t.Fatal("expected read error to propagate")
	}
	if f.aborted != 1 || f.completed != 0 {
		t.Fatalf("expected the upload to be aborted, got aborted=%d completed=%d", f.aborted, f.completed)
	}
}

func TestUpload_ReadErrorBeforeFirstPartUploadsNothing(t *testing.T) {
	f := newFakeAPI()
	c := &Client{s3: f, bucket: "b"}

	if _, err := c.Upload(context.Background(), "k", io.MultiReader(bytes.NewReader([]byte("partial")), failingReader{})); err == nil {
		t.Fatal("expected read error to propagate")
	}
	if len(f.puts) != 0 || f.created != 0 {
		t.Fatalf("expected nothing to be uploaded, got puts=%d created=%d", len(f.puts), f.created)
	}
}

// GCS rejects UNSIGNED-PAYLOAD, which the SDK uses for uploads over HTTPS by
// default, so the client must sign the body's SHA-256.
func TestNew_SignsPayloadOverHTTPS(t *testing.T) {
	body := []byte("backup")
	want := sha256.Sum256(body)

	var got, auth string
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("X-Amz-Content-Sha256")
		auth = r.Header.Get("Authorization")
	}))
	defer srv.Close()

	c, err := New(context.Background(), Config{
		Bucket:         "bucket",
		Endpoint:       srv.URL,
		ForcePathStyle: true,
		Credentials:    &Credentials{AccessKeyID: "id", SecretAccessKey: "secret"},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := c.s3.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String("bucket"),
		Key:    aws.String("key"),
		Body:   bytes.NewReader(body),
	}, func(o *s3.Options) { o.HTTPClient = srv.Client() }); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if got != hex.EncodeToString(want[:]) {
		t.Errorf("X-Amz-Content-Sha256 = %q, want the body's SHA-256", got)
	}
	// GCS rewrites Accept-Encoding in transit, so it must not be signed.
	if strings.Contains(strings.ToLower(auth), "accept-encoding") {
		t.Errorf("Authorization signs Accept-Encoding: %q", auth)
	}
}
