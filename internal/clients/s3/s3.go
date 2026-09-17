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

// Package s3 is a thin client over the S3 API used to write Backup managed
// resources to object storage. It targets the S3 API specifically (rather
// than a native client per backend) so that AWS, MinIO, Cloudflare R2 and
// GCS (via its S3-compatible XML API) are all covered through Endpoint and
// ForcePathStyle.
package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go/middleware"
	"github.com/pkg/errors"
)

const (
	defaultRegion = "us-east-1"

	// partSize is the size of each part of a multipart upload, and so the
	// most an upload buffers in memory. S3 requires every part but the last
	// to be at least 5 MiB.
	partSize = 5 * 1024 * 1024

	// maxParts is the most parts S3 accepts in one multipart upload.
	maxParts = 10000
)

// Credentials is the JSON payload expected in a destination bucket
// credentials secret.
type Credentials struct {
	AccessKeyID     string `json:"accessKeyId"`
	SecretAccessKey string `json:"secretAccessKey"`
	SessionToken    string `json:"sessionToken,omitempty"`
}

// ParseCredentials decodes JSON-shaped static bucket credentials.
func ParseCredentials(data []byte) (Credentials, error) {
	var c Credentials
	if err := json.Unmarshal(data, &c); err != nil {
		return c, errors.Wrap(err, "bucket credentials secret is not valid JSON")
	}
	if c.AccessKeyID == "" || c.SecretAccessKey == "" {
		return c, errors.New(`bucket credentials secret must contain non-empty "accessKeyId" and "secretAccessKey"`)
	}
	return c, nil
}

// Config is everything needed to construct a Client.
type Config struct {
	Bucket         string
	Region         string
	Endpoint       string
	ForcePathStyle bool

	// Credentials is nil to use the AWS SDK's ambient credential chain
	// (e.g. IRSA), or non-nil for static credentials read from a Secret.
	Credentials *Credentials
}

// api is the subset of the S3 API used by Client, so tests can substitute a
// fake.
type api interface {
	PutObject(ctx context.Context, in *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	CreateMultipartUpload(ctx context.Context, in *s3.CreateMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	UploadPart(ctx context.Context, in *s3.UploadPartInput, optFns ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	CompleteMultipartUpload(ctx context.Context, in *s3.CompleteMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(ctx context.Context, in *s3.AbortMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
	ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	DeleteObjects(ctx context.Context, in *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
}

// Client is a minimal S3 client scoped to a single bucket.
type Client struct {
	s3     api
	bucket string
}

// New constructs a Client for cfg.Bucket.
func New(ctx context.Context, cfg Config) (*Client, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("bucket is required")
	}

	region := cfg.Region
	if region == "" {
		region = defaultRegion
	}

	optFns := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(region),
		// GCS's S3-compatible XML API doesn't accept the SDK's default
		// flexible checksum headers (x-amz-sdk-checksum-algorithm,
		// x-amz-checksum-crc32) and rejects requests that include them as
		// SignatureDoesNotMatch. Only send/require them when a caller (or
		// operation) explicitly opts in.
		awsconfig.WithRequestChecksumCalculation(aws.RequestChecksumCalculationWhenRequired),
		awsconfig.WithResponseChecksumValidation(aws.ResponseChecksumValidationWhenRequired),
	}
	if cfg.Credentials != nil {
		optFns = append(optFns, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg.Credentials.AccessKeyID, cfg.Credentials.SecretAccessKey, cfg.Credentials.SessionToken,
		)))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, optFns...)
	if err != nil {
		return nil, errors.Wrap(err, "load AWS config")
	}

	cl := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		o.UsePathStyle = cfg.ForcePathStyle
		// Over HTTPS the SDK sends PutObject/UploadPart bodies as
		// UNSIGNED-PAYLOAD, which GCS rejects as SignatureDoesNotMatch.
		// Swap in the middleware that hashes and signs the payload. Swap
		// (not Insert) because every operation already registers one.
		o.APIOptions = append(o.APIOptions, signPayload)
	})

	return &Client{s3: cl, bucket: cfg.Bucket}, nil
}

// signPayload replaces the operation's payload-hash middleware with one that
// always computes the body's SHA-256.
func signPayload(stack *middleware.Stack) error {
	_, err := stack.Finalize.Swap((*v4.ComputePayloadSHA256)(nil).ID(), &v4.ComputePayloadSHA256{})
	return err
}

// Upload streams r to key and returns the number of bytes written. The body
// is read one part at a time, so memory use is bounded by partSize whatever
// the object's size, and nothing is staged on disk. A body that fits in one
// part is sent with PutObject; a larger one uses a multipart upload, which
// is aborted if reading r or uploading fails.
func (c *Client) Upload(ctx context.Context, key string, r io.Reader) (int64, error) {
	buf := make([]byte, partSize)
	n, err := io.ReadFull(r, buf)
	switch {
	case errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF):
		if _, err := c.s3.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(c.bucket),
			Key:           aws.String(key),
			Body:          bytes.NewReader(buf[:n]),
			ContentLength: aws.Int64(int64(n)),
		}); err != nil {
			return 0, errors.Wrapf(err, "put object %q", key)
		}
		return int64(n), nil
	case err != nil:
		return 0, errors.Wrapf(err, "read %q", key)
	}
	return c.multipartUpload(ctx, key, r, buf)
}

func (c *Client) multipartUpload(ctx context.Context, key string, r io.Reader, first []byte) (int64, error) {
	created, err := c.s3.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return 0, errors.Wrapf(err, "create multipart upload %q", key)
	}

	parts, size, err := c.uploadParts(ctx, key, created.UploadId, r, first)
	if err != nil {
		// Best effort: an unfinished multipart upload otherwise lingers (and
		// is billed) until a bucket lifecycle rule removes it.
		_, _ = c.s3.AbortMultipartUpload(context.WithoutCancel(ctx), &s3.AbortMultipartUploadInput{ //nolint:errcheck // best-effort cleanup
			Bucket:   aws.String(c.bucket),
			Key:      aws.String(key),
			UploadId: created.UploadId,
		})
		return 0, err
	}

	if _, err := c.s3.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(c.bucket),
		Key:             aws.String(key),
		UploadId:        created.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	}); err != nil {
		return 0, errors.Wrapf(err, "complete multipart upload %q", key)
	}
	return size, nil
}

// uploadParts uploads buf, which holds the first full part, followed by the
// rest of r. buf is reused for every part.
func (c *Client) uploadParts(ctx context.Context, key string, uploadID *string, r io.Reader, buf []byte) ([]types.CompletedPart, int64, error) {
	var (
		parts []types.CompletedPart
		size  int64
	)
	chunk := buf
	for num := int32(1); len(chunk) > 0; num++ {
		if num > maxParts {
			return nil, 0, errors.Errorf("object %q exceeds %d parts", key, maxParts)
		}
		out, err := c.s3.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:        aws.String(c.bucket),
			Key:           aws.String(key),
			UploadId:      uploadID,
			PartNumber:    aws.Int32(num),
			Body:          bytes.NewReader(chunk),
			ContentLength: aws.Int64(int64(len(chunk))),
		})
		if err != nil {
			return nil, 0, errors.Wrapf(err, "upload part %d of %q", num, key)
		}
		parts = append(parts, types.CompletedPart{ETag: out.ETag, PartNumber: aws.Int32(num)})
		size += int64(len(chunk))

		if len(chunk) < partSize {
			break // a short part means r is drained
		}
		n, err := io.ReadFull(r, buf)
		if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, 0, errors.Wrapf(err, "read %q", key)
		}
		chunk = buf[:n]
	}
	return parts, size, nil
}

// Object describes an object found by ListObjects.
type Object struct {
	Key          string
	LastModified time.Time
}

// ListObjects lists every object under prefix, paging as needed.
func (c *Client) ListObjects(ctx context.Context, prefix string) ([]Object, error) {
	var out []Object
	paginator := s3.NewListObjectsV2Paginator(c.s3, &s3.ListObjectsV2Input{
		Bucket: aws.String(c.bucket),
		Prefix: aws.String(prefix),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, errors.Wrapf(err, "list objects under %q", prefix)
		}
		for _, obj := range page.Contents {
			o := Object{Key: aws.ToString(obj.Key)}
			if obj.LastModified != nil {
				o.LastModified = *obj.LastModified
			}
			out = append(out, o)
		}
	}
	return out, nil
}

// deleteBatchLimit is the maximum number of keys the S3 DeleteObjects API
// accepts in a single request.
const deleteBatchLimit = 1000

// DeleteObjects deletes every key in keys, batching as needed.
func (c *Client) DeleteObjects(ctx context.Context, keys []string) error {
	for start := 0; start < len(keys); start += deleteBatchLimit {
		end := min(start+deleteBatchLimit, len(keys))
		ids := make([]types.ObjectIdentifier, 0, end-start)
		for _, k := range keys[start:end] {
			ids = append(ids, types.ObjectIdentifier{Key: aws.String(k)})
		}
		if _, err := c.s3.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(c.bucket),
			Delete: &types.Delete{Objects: ids},
		}); err != nil {
			return errors.Wrap(err, "delete objects")
		}
	}
	return nil
}
