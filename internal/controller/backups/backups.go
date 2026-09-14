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

// Package backups reconciles the Backup managed resource: on a cron
// schedule it pulls a Strapi project's content from Strapi's remote
// data-transfer endpoint and writes it to an S3-compatible bucket.
package backups

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"

	xpv1 "github.com/crossplane/crossplane-runtime/v2/apis/common/v1"
	"github.com/crossplane/crossplane-runtime/v2/pkg/controller"
	"github.com/crossplane/crossplane-runtime/v2/pkg/event"
	"github.com/crossplane/crossplane-runtime/v2/pkg/feature"
	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	"github.com/crossplane/crossplane-runtime/v2/pkg/ratelimiter"
	"github.com/crossplane/crossplane-runtime/v2/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	"github.com/crossplane/crossplane-runtime/v2/pkg/statemetrics"
	"github.com/pkg/errors"
	"github.com/robfig/cron/v3"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	backupsv2alpha1 "github.com/web-seven/provider-strapi/apis/backups/v2alpha1"
	apisv2alpha1 "github.com/web-seven/provider-strapi/apis/v2alpha1"
	s3client "github.com/web-seven/provider-strapi/internal/clients/s3"
	strapiclient "github.com/web-seven/provider-strapi/internal/clients/strapi"
)

const (
	errTrackPCUsage      = "cannot track ProviderConfig usage"
	errGetPC             = "cannot get ProviderConfig"
	errGetCPC            = "cannot get ClusterProviderConfig"
	errGetTransferToken  = "cannot get transfer token"
	errGetBucketCreds    = "cannot get bucket credentials"
	errParseBucketCreds  = "cannot parse bucket credentials"
	errNewTransferClient = "cannot create Strapi transfer client"
	errNewBucketClient   = "cannot create bucket client"
	errParseSchedule     = "cannot parse schedule"
	errMkTempDir         = "cannot create local staging directory"
	errDump              = "cannot pull project dump from Strapi"
	errUpload            = "cannot upload backup to bucket"
	errRetention         = "cannot prune old backups"
	errNotBucketSecret   = "credentials.secretRef is required when source is Secret"
)

// SetupGated registers the controller. The "Gated" name is preserved for
// continuity with the upstream provider-template's call sites; the safe-start
// CRD gate has been removed because the provider's auto-generated ClusterRole
// did not include `apiextensions.k8s.io/customresourcedefinitions` watch
// permissions, causing the gate's CRD informer to time out and the manager
// to refuse to start. CRDs in this package are installed by Crossplane
// before the provider container starts, so gating is unnecessary.
func SetupGated(mgr ctrl.Manager, o controller.Options) error {
	return Setup(mgr, o)
}

// Setup adds a controller that reconciles Backup managed resources.
func Setup(mgr ctrl.Manager, o controller.Options) error {
	name := managed.ControllerName(backupsv2alpha1.BackupGroupKind)

	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*backupsv2alpha1.Backup](&connector{
			kube:  mgr.GetClient(),
			usage: resource.NewProviderConfigUsageTracker(mgr.GetClient(), &apisv2alpha1.ProviderConfigUsage{}),
			newTransferFn: func(cfg strapiclient.TransferConfig) (transferClient, error) {
				return strapiclient.NewTransferClient(cfg)
			},
			newBucketFn: func(ctx context.Context, cfg s3client.Config) (bucketClient, error) {
				return s3client.New(ctx, cfg)
			},
		}),
		managed.WithLogger(o.Logger.WithValues("controller", name)),
		managed.WithPollInterval(o.PollInterval),
		managed.WithRecorder(event.NewAPIRecorder(mgr.GetEventRecorderFor(name))),
	}

	if o.Features.Enabled(feature.EnableBetaManagementPolicies) {
		opts = append(opts, managed.WithManagementPolicies())
	}
	if o.Features.Enabled(feature.EnableAlphaChangeLogs) {
		opts = append(opts, managed.WithChangeLogger(o.ChangeLogOptions.ChangeLogger))
	}
	if o.MetricOptions != nil {
		opts = append(opts, managed.WithMetricRecorder(o.MetricOptions.MRMetrics))
	}
	if o.MetricOptions != nil && o.MetricOptions.MRStateMetrics != nil {
		stateMetricsRecorder := statemetrics.NewMRStateRecorder(
			mgr.GetClient(), o.Logger, o.MetricOptions.MRStateMetrics,
			&backupsv2alpha1.BackupList{}, o.MetricOptions.PollStateMetricInterval,
		)
		if err := mgr.Add(stateMetricsRecorder); err != nil {
			return errors.Wrap(err, "cannot register MR state metrics recorder")
		}
	}

	r := managed.NewReconciler(mgr, resource.ManagedKind(backupsv2alpha1.BackupGroupVersionKind), opts...)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&backupsv2alpha1.Backup{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}

// transferClient is the subset of strapiclient.TransferClient this
// controller depends on. Defined as an interface so tests can substitute a
// fake without dialing a real WebSocket.
type transferClient interface {
	Dump(ctx context.Context, dir string, includeAssets bool) (strapiclient.DumpManifest, error)
}

// bucketClient is the subset of s3client.Client this controller depends on.
type bucketClient interface {
	PutFile(ctx context.Context, key, path string) (int64, error)
	ListObjects(ctx context.Context, prefix string) ([]s3client.Object, error)
	DeleteObjects(ctx context.Context, keys []string) error
}

type connector struct {
	kube          client.Client
	usage         *resource.ProviderConfigUsageTracker
	newTransferFn func(cfg strapiclient.TransferConfig) (transferClient, error)
	newBucketFn   func(ctx context.Context, cfg s3client.Config) (bucketClient, error)
}

// providerConfigRef returns the name and kind of the ProviderConfig to use.
// If the CR has no providerConfigRef, it defaults to the ClusterProviderConfig
// named "default" (the Crossplane v2.2 platform default).
func providerConfigRef(cr *backupsv2alpha1.Backup) (name, kind string) {
	name, kind = "default", "ClusterProviderConfig"
	ref := cr.GetProviderConfigReference()
	if ref == nil {
		return
	}
	if ref.Name != "" {
		name = ref.Name
	}
	if ref.Kind != "" {
		kind = ref.Kind
	}
	return
}

func (c *connector) resolveProviderConfig(ctx context.Context, cr *backupsv2alpha1.Backup) (endpoint string, insecure bool, err error) {
	name, kind := providerConfigRef(cr)
	switch kind {
	case "ProviderConfig":
		pc := &apisv2alpha1.ProviderConfig{}
		if err := c.kube.Get(ctx, types.NamespacedName{Name: name, Namespace: cr.GetNamespace()}, pc); err != nil {
			return "", false, errors.Wrap(err, errGetPC)
		}
		return pc.Spec.Endpoint, pc.Spec.InsecureSkipTLSVerify, nil
	case "ClusterProviderConfig":
		cpc := &apisv2alpha1.ClusterProviderConfig{}
		if err := c.kube.Get(ctx, types.NamespacedName{Name: name}, cpc); err != nil {
			return "", false, errors.Wrap(err, errGetCPC)
		}
		return cpc.Spec.Endpoint, cpc.Spec.InsecureSkipTLSVerify, nil
	default:
		return "", false, errors.Errorf("unsupported provider config kind: %s", kind)
	}
}

func (c *connector) getSecretKey(ctx context.Context, ref xpv1.SecretKeySelector) ([]byte, error) {
	s := &corev1.Secret{}
	if err := c.kube.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: ref.Namespace}, s); err != nil {
		return nil, err
	}
	v, ok := s.Data[ref.Key]
	if !ok {
		return nil, errors.Errorf("secret %s/%s has no key %q", ref.Namespace, ref.Name, ref.Key)
	}
	return v, nil
}

func (c *connector) resolveBucketCredentials(ctx context.Context, bc backupsv2alpha1.BucketCredentials) (*s3client.Credentials, error) {
	if bc.Source != xpv1.CredentialsSourceSecret {
		return nil, nil
	}
	if bc.SecretRef == nil {
		return nil, errors.New(errNotBucketSecret)
	}
	data, err := c.getSecretKey(ctx, *bc.SecretRef)
	if err != nil {
		return nil, errors.Wrap(err, errGetBucketCreds)
	}
	creds, err := s3client.ParseCredentials(data)
	if err != nil {
		return nil, errors.Wrap(err, errParseBucketCreds)
	}
	return &creds, nil
}

func (c *connector) Connect(ctx context.Context, cr *backupsv2alpha1.Backup) (managed.TypedExternalClient[*backupsv2alpha1.Backup], error) {
	if err := c.usage.Track(ctx, cr); err != nil {
		return nil, errors.Wrap(err, errTrackPCUsage)
	}

	endpoint, insecure, err := c.resolveProviderConfig(ctx, cr)
	if err != nil {
		return nil, err
	}

	tokenBytes, err := c.getSecretKey(ctx, cr.Spec.ForProvider.TransferTokenSecretRef)
	if err != nil {
		return nil, errors.Wrap(err, errGetTransferToken)
	}

	tc, err := c.newTransferFn(strapiclient.TransferConfig{
		Endpoint:              endpoint,
		Token:                 strings.TrimSpace(string(tokenBytes)),
		InsecureSkipTLSVerify: insecure,
	})
	if err != nil {
		return nil, errors.Wrap(err, errNewTransferClient)
	}

	dest := cr.Spec.ForProvider.Destination
	bucketCreds, err := c.resolveBucketCredentials(ctx, dest.Credentials)
	if err != nil {
		return nil, err
	}

	bc, err := c.newBucketFn(ctx, s3client.Config{
		Bucket:         dest.Bucket,
		Region:         dest.Region,
		Endpoint:       dest.Endpoint,
		ForcePathStyle: dest.ForcePathStyle,
		Credentials:    bucketCreds,
	})
	if err != nil {
		return nil, errors.Wrap(err, errNewBucketClient)
	}

	return &external{transfer: tc, bucket: bc}, nil
}

type external struct {
	transfer transferClient
	bucket   bucketClient
}

// nextBackupTime computes when the next backup is due, counting forward
// from the last successful backup (or from resource creation if there has
// never been one). Anchoring on the actual last-run time — rather than on
// schedule ticks that may have been missed while the provider was down —
// means a delayed reconcile triggers exactly one backup, not a catch-up
// storm.
func nextBackupTime(cr *backupsv2alpha1.Backup) (time.Time, error) {
	sched, err := cron.ParseStandard(cr.Spec.ForProvider.Schedule)
	if err != nil {
		return time.Time{}, errors.Wrap(err, errParseSchedule)
	}
	ref := cr.CreationTimestamp.Time
	if lb := cr.Status.AtProvider.LastBackupTime; lb != nil {
		ref = lb.Time
	}
	return sched.Next(ref), nil
}

func (e *external) Observe(_ context.Context, cr *backupsv2alpha1.Backup) (managed.ExternalObservation, error) {
	if meta.GetExternalName(cr) == "" {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}

	next, err := nextBackupTime(cr)
	if err != nil {
		return managed.ExternalObservation{}, err
	}
	cr.Status.AtProvider.NextBackupTime = &metav1.Time{Time: next}
	cr.Status.SetConditions(xpv1.Available())

	due := !next.After(time.Now())
	return managed.ExternalObservation{ResourceExists: true, ResourceUpToDate: !due}, nil
}

func (e *external) Create(_ context.Context, cr *backupsv2alpha1.Backup) (managed.ExternalCreation, error) {
	cr.Status.SetConditions(xpv1.Creating())
	// There's no external object to create yet — the first backup happens
	// once the schedule comes due, the same as any subsequent one. Setting
	// the external name just marks that Create has run, so the next Observe
	// reports the resource as existing.
	meta.SetExternalName(cr, cr.GetName())
	return managed.ExternalCreation{}, nil
}

// Update performs a backup. It's invoked by the managed-resource reconciler
// whenever Observe reports the resource isn't up to date, which for a
// Backup means "a run is due" rather than "the spec drifted".
func (e *external) Update(ctx context.Context, cr *backupsv2alpha1.Backup) (managed.ExternalUpdate, error) {
	dir, err := os.MkdirTemp("", "strapi-backup-*")
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errMkTempDir)
	}
	defer os.RemoveAll(dir) //nolint:errcheck

	manifest, err := e.transfer.Dump(ctx, dir, cr.Spec.ForProvider.IncludeAssets)
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errDump)
	}

	now := time.Now().UTC()
	prefix := backupPrefix(cr.Spec.ForProvider.Destination.Path, now)
	size, err := e.upload(ctx, prefix, dir, manifest)
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errUpload)
	}

	cr.Status.AtProvider.LastBackupTime = &metav1.Time{Time: now}
	cr.Status.AtProvider.LastBackupKey = prefix
	cr.Status.AtProvider.LastBackupSizeBytes = size
	cr.Status.AtProvider.LastBackupEntities = manifest.Entities.Count
	cr.Status.AtProvider.LastBackupAssets = int64(len(manifest.Assets))

	if days := cr.Spec.ForProvider.RetentionDays; days > 0 {
		if err := e.applyRetention(ctx, cr.Spec.ForProvider.Destination.Path, days); err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, errRetention)
		}
	}
	return managed.ExternalUpdate{}, nil
}

// Delete does not remove any backups already written to the bucket — a
// Backup resource schedules future backups, it doesn't own the lifecycle of
// past ones. Retention (if configured) and bucket lifecycle rules are the
// intended way to age data out.
func (e *external) Delete(_ context.Context, cr *backupsv2alpha1.Backup) (managed.ExternalDelete, error) {
	cr.Status.SetConditions(xpv1.Deleting())
	return managed.ExternalDelete{}, nil
}

func (e *external) Disconnect(_ context.Context) error { return nil }

// backupManifest is written alongside a backup's files so retention pruning
// can identify a complete backup by its key prefix.
type backupManifest struct {
	CreatedAt     time.Time `json:"createdAt"`
	Entities      int64     `json:"entities"`
	Links         int64     `json:"links"`
	Configuration int64     `json:"configuration"`
	Assets        int       `json:"assets"`
}

func backupPrefix(destPath string, at time.Time) string {
	p := strings.Trim(destPath, "/")
	stamp := at.Format("20060102T150405Z")
	if p == "" {
		return stamp
	}
	return p + "/" + stamp
}

func (e *external) upload(ctx context.Context, prefix, dir string, manifest strapiclient.DumpManifest) (int64, error) {
	var total int64
	files := append([]strapiclient.DumpFile{manifest.Entities, manifest.Links, manifest.Configuration}, manifest.Assets...)
	for _, f := range files {
		if f.Path == "" {
			continue
		}
		size, err := e.bucket.PutFile(ctx, prefix+"/"+f.Key, f.Path)
		if err != nil {
			return total, errors.Wrapf(err, "upload %s", f.Key)
		}
		total += size
	}

	size, err := e.uploadManifest(ctx, prefix, dir, manifest)
	if err != nil {
		return total, err
	}
	return total + size, nil
}

func (e *external) uploadManifest(ctx context.Context, prefix, dir string, manifest strapiclient.DumpManifest) (int64, error) {
	b, err := json.Marshal(backupManifest{
		CreatedAt:     time.Now().UTC(),
		Entities:      manifest.Entities.Count,
		Links:         manifest.Links.Count,
		Configuration: manifest.Configuration.Count,
		Assets:        len(manifest.Assets),
	})
	if err != nil {
		return 0, errors.Wrap(err, "encode manifest")
	}
	path := filepath.Join(dir, "manifest.json")
	if err := os.WriteFile(path, b, 0o600); err != nil {
		return 0, errors.Wrap(err, "write manifest")
	}
	size, err := e.bucket.PutFile(ctx, prefix+"/manifest.json", path)
	return size, errors.Wrap(err, "upload manifest")
}

// applyRetention deletes every backup (identified by its manifest.json)
// whose object was last modified more than retentionDays ago.
func (e *external) applyRetention(ctx context.Context, destPath string, retentionDays int32) error {
	listPrefix := strings.Trim(destPath, "/")
	if listPrefix != "" {
		listPrefix += "/"
	}
	objs, err := e.bucket.ListObjects(ctx, listPrefix)
	if err != nil {
		return errors.Wrap(err, "list backups")
	}

	cutoff := time.Now().Add(-time.Duration(retentionDays) * 24 * time.Hour)
	var stale []string
	for _, o := range objs {
		if !strings.HasSuffix(o.Key, "/manifest.json") || o.LastModified.After(cutoff) {
			continue
		}
		prefix := strings.TrimSuffix(o.Key, "manifest.json")
		keys, err := e.bucket.ListObjects(ctx, prefix)
		if err != nil {
			return errors.Wrapf(err, "list stale backup %q", prefix)
		}
		for _, k := range keys {
			stale = append(stale, k.Key)
		}
	}
	if len(stale) == 0 {
		return nil
	}
	return errors.Wrap(e.bucket.DeleteObjects(ctx, stale), "delete stale backups")
}
