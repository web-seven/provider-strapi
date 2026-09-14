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
	"context"
	"encoding/json"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/pkg/errors"
)

const defaultRegion = "us-east-1"

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

// Client is a minimal S3 client scoped to a single bucket.
type Client struct {
	s3     *s3.Client
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

	optFns := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(region)}
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
	})

	return &Client{s3: cl, bucket: cfg.Bucket}, nil
}

// PutFile uploads the local file at path under key and returns its size in
// bytes. The file is read via a seekable *os.File so the SDK can sign the
// request without buffering the whole payload in memory.
func (c *Client) PutFile(ctx context.Context, key, path string) (int64, error) {
	f, err := os.Open(path) //nolint:gosec // path is a provider-managed temp file, not user input
	if err != nil {
		return 0, errors.Wrap(err, "open backup file")
	}
	defer f.Close() //nolint:errcheck

	stat, err := f.Stat()
	if err != nil {
		return 0, errors.Wrap(err, "stat backup file")
	}

	if _, err := c.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(c.bucket),
		Key:           aws.String(key),
		Body:          f,
		ContentLength: aws.Int64(stat.Size()),
	}); err != nil {
		return 0, errors.Wrapf(err, "put object %q", key)
	}
	return stat.Size(), nil
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
