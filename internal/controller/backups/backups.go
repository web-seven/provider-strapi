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
// data-transfer endpoint and streams it to an S3-compatible bucket.
package backups

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"sync"
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
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	backupsv2alpha1 "github.com/web-seven/provider-strapi/apis/backups/v2alpha1"
	apisv2alpha1 "github.com/web-seven/provider-strapi/apis/v2alpha1"
	s3client "github.com/web-seven/provider-strapi/internal/clients/s3"
	strapiclient "github.com/web-seven/provider-strapi/internal/clients/strapi"
)

const (
	errTrackPCUsage      = "cannot track ProviderConfig usage"
	errGetPC             = "cannot get ProviderConfig"
	errGetCPC            = "cannot get ClusterProviderConfig"
	errGetCreds          = "cannot get Strapi admin credentials"
	errParseCreds        = "cannot parse Strapi admin credentials"
	errNewAdminClient    = "cannot create Strapi admin client"
	errGetTransferToken  = "cannot get transfer token"
	errGetTokenSecret    = "cannot get managed transfer token secret"
	errWriteTokenSecret  = "cannot write managed transfer token secret"
	errDeleteTokenSecret = "cannot delete managed transfer token secret"
	errProvisionToken    = "cannot provision transfer token"
	errObserveToken      = "cannot observe transfer token"
	errDeleteToken       = "cannot delete transfer token"
	errGetBucketCreds    = "cannot get bucket credentials"
	errParseBucketCreds  = "cannot parse bucket credentials"
	errNewTransferClient = "cannot create Strapi transfer client"
	errNewBucketClient   = "cannot create bucket client"
	errParseSchedule     = "cannot parse schedule"
	errDump              = "cannot stream project dump from Strapi to bucket"
	errUpload            = "cannot upload backup manifest to bucket"
	errRetention         = "cannot prune old backups"
	errNotBucketSecret   = "credentials.secretRef is required when source is Secret"
)

const (
	// transferTokenSecretKey is the key a provisioned transfer token is
	// stored under in its managed Secret.
	transferTokenSecretKey   = "token"
	transferTokenDescription = "Managed by Crossplane provider-strapi."

	// backupTimeout bounds a single reconcile, which includes streaming a
	// whole backup from Strapi to the bucket. The managed reconciler's
	// one-minute default is too short for all but the smallest projects.
	backupTimeout = time.Hour
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
			newAdminFn: func(cfg strapiclient.Config) (tokenAdmin, error) {
				return strapiclient.New(cfg)
			},
			newTransferFn: func(cfg strapiclient.TransferConfig) (transferClient, error) {
				return strapiclient.NewTransferClient(cfg)
			},
			newBucketFn: func(ctx context.Context, cfg s3client.Config) (bucketClient, error) {
				return s3client.New(ctx, cfg)
			},
		}),
		managed.WithLogger(o.Logger.WithValues("controller", name)),
		managed.WithTimeout(backupTimeout),
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
	Dump(ctx context.Context, sink strapiclient.DumpSink, includeAssets bool) (strapiclient.DumpManifest, error)
}

// tokenAdmin is the subset of the Strapi admin client used to provision
// transfer tokens when a Backup doesn't reference one.
type tokenAdmin interface {
	ListTransferTokens(ctx context.Context) ([]strapiclient.TransferToken, error)
	CreateTransferToken(ctx context.Context, name, description string, permissions []string) (strapiclient.TransferToken, error)
	RegenerateTransferToken(ctx context.Context, id int) (string, error)
	DeleteTransferToken(ctx context.Context, id int) error
}

// bucketClient is the subset of s3client.Client this controller depends on.
type bucketClient interface {
	Upload(ctx context.Context, key string, r io.Reader) (int64, error)
	ListObjects(ctx context.Context, prefix string) ([]s3client.Object, error)
	DeleteObjects(ctx context.Context, keys []string) error
}

type connector struct {
	kube          client.Client
	usage         *resource.ProviderConfigUsageTracker
	newAdminFn    func(cfg strapiclient.Config) (tokenAdmin, error)
	newTransferFn func(cfg strapiclient.TransferConfig) (transferClient, error)
	newBucketFn   func(ctx context.Context, cfg s3client.Config) (bucketClient, error)
}

// providerConfig is the part of a (Cluster)ProviderConfig spec this
// controller needs.
type providerConfig struct {
	endpoint    string
	insecure    bool
	credentials apisv2alpha1.ProviderCredentials
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

func (c *connector) resolveProviderConfig(ctx context.Context, cr *backupsv2alpha1.Backup) (providerConfig, error) {
	name, kind := providerConfigRef(cr)
	switch kind {
	case "ProviderConfig":
		pc := &apisv2alpha1.ProviderConfig{}
		if err := c.kube.Get(ctx, types.NamespacedName{Name: name, Namespace: cr.GetNamespace()}, pc); err != nil {
			return providerConfig{}, errors.Wrap(err, errGetPC)
		}
		return providerConfig{endpoint: pc.Spec.Endpoint, insecure: pc.Spec.InsecureSkipTLSVerify, credentials: pc.Spec.Credentials}, nil
	case "ClusterProviderConfig":
		cpc := &apisv2alpha1.ClusterProviderConfig{}
		if err := c.kube.Get(ctx, types.NamespacedName{Name: name}, cpc); err != nil {
			return providerConfig{}, errors.Wrap(err, errGetCPC)
		}
		return providerConfig{endpoint: cpc.Spec.Endpoint, insecure: cpc.Spec.InsecureSkipTLSVerify, credentials: cpc.Spec.Credentials}, nil
	default:
		return providerConfig{}, errors.Errorf("unsupported provider config kind: %s", kind)
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

func (c *connector) newAdmin(ctx context.Context, pc providerConfig) (tokenAdmin, error) {
	data, err := resource.CommonCredentialExtractor(ctx, pc.credentials.Source, c.kube, pc.credentials.CommonCredentialSelectors)
	if err != nil {
		return nil, errors.Wrap(err, errGetCreds)
	}
	creds, err := strapiclient.ParseCredentials(data)
	if err != nil {
		return nil, errors.Wrap(err, errParseCreds)
	}
	admin, err := c.newAdminFn(strapiclient.Config{
		Endpoint:              pc.endpoint,
		Credentials:           creds,
		InsecureSkipTLSVerify: pc.insecure,
	})
	if err != nil {
		return nil, errors.Wrap(err, errNewAdminClient)
	}
	return admin, nil
}

func (c *connector) newBucket(ctx context.Context, dest backupsv2alpha1.BackupDestination) (bucketClient, error) {
	creds, err := c.resolveBucketCredentials(ctx, dest.Credentials)
	if err != nil {
		return nil, err
	}
	bc, err := c.newBucketFn(ctx, s3client.Config{
		Bucket:         dest.Bucket,
		Region:         dest.Region,
		Endpoint:       dest.Endpoint,
		ForcePathStyle: dest.ForcePathStyle,
		Credentials:    creds,
	})
	if err != nil {
		return nil, errors.Wrap(err, errNewBucketClient)
	}
	return bc, nil
}

// transferToken returns the token data transfers authenticate with: read
// from the referenced Secret, or provisioned through the admin API when the
// Backup doesn't reference one.
func (c *connector) transferToken(ctx context.Context, cr *backupsv2alpha1.Backup, admin tokenAdmin) (string, error) {
	if ref := cr.Spec.ForProvider.TransferTokenSecretRef; ref != nil {
		data, err := c.getSecretKey(ctx, *ref)
		if err != nil {
			return "", errors.Wrap(err, errGetTransferToken)
		}
		return strings.TrimSpace(string(data)), nil
	}
	return ensureTransferToken(ctx, c.kube, admin, cr)
}

func (c *connector) Connect(ctx context.Context, cr *backupsv2alpha1.Backup) (managed.TypedExternalClient[*backupsv2alpha1.Backup], error) {
	if err := c.usage.Track(ctx, cr); err != nil {
		return nil, errors.Wrap(err, errTrackPCUsage)
	}

	pc, err := c.resolveProviderConfig(ctx, cr)
	if err != nil {
		return nil, err
	}

	ext := &external{kube: c.kube}
	if cr.Spec.ForProvider.TransferTokenSecretRef == nil {
		if ext.admin, err = c.newAdmin(ctx, pc); err != nil {
			return nil, err
		}
	}

	// Deleting only needs the admin client to clean up a provisioned token.
	// Skip provisioning, which would otherwise mint a token just to delete it.
	if meta.WasDeleted(cr) {
		return ext, nil
	}

	token, err := c.transferToken(ctx, cr, ext.admin)
	if err != nil {
		return nil, err
	}
	if ext.transfer, err = c.newTransferFn(strapiclient.TransferConfig{
		Endpoint:              pc.endpoint,
		Token:                 token,
		InsecureSkipTLSVerify: pc.insecure,
	}); err != nil {
		return nil, errors.Wrap(err, errNewTransferClient)
	}

	if ext.bucket, err = c.newBucket(ctx, cr.Spec.ForProvider.Destination); err != nil {
		return nil, err
	}
	return ext, nil
}

// transferTokenName is the name of the transfer token provisioned in Strapi
// for a Backup. Strapi token names are unique, so it's namespaced.
func transferTokenName(cr *backupsv2alpha1.Backup) string {
	return "crossplane-backup-" + cr.GetNamespace() + "-" + cr.GetName()
}

// transferTokenSecretName is the name of the Secret, in the Backup's
// namespace, that stores a provisioned transfer token.
func transferTokenSecretName(cr *backupsv2alpha1.Backup) string {
	return cr.GetName() + "-transfer-token"
}

// ensureTransferToken returns the provisioned transfer token stored in the
// Backup's managed Secret, provisioning one through the admin API and
// storing it if the Secret is absent.
func ensureTransferToken(ctx context.Context, kube client.Client, admin tokenAdmin, cr *backupsv2alpha1.Backup) (string, error) {
	s := &corev1.Secret{}
	err := kube.Get(ctx, types.NamespacedName{Namespace: cr.GetNamespace(), Name: transferTokenSecretName(cr)}, s)
	if err != nil && !kerrors.IsNotFound(err) {
		return "", errors.Wrap(err, errGetTokenSecret)
	}
	if token := s.Data[transferTokenSecretKey]; err == nil && len(token) > 0 {
		return string(token), nil
	}

	token, err := provisionTransferToken(ctx, admin, transferTokenName(cr))
	if err != nil {
		return "", errors.Wrap(err, errProvisionToken)
	}
	if err := writeTransferTokenSecret(ctx, kube, cr, token); err != nil {
		return "", err
	}
	return token, nil
}

// provisionTransferToken returns a fresh access key for the named pull
// token, creating the token if needed. Strapi only reveals an access key
// once, so an existing token (e.g. left behind when storing its Secret
// failed) is regenerated rather than reused.
func provisionTransferToken(ctx context.Context, admin tokenAdmin, name string) (string, error) {
	tok, found, err := findTransferToken(ctx, admin, name)
	if err != nil {
		return "", err
	}
	if found {
		return admin.RegenerateTransferToken(ctx, tok.ID)
	}
	created, err := admin.CreateTransferToken(ctx, name, transferTokenDescription, []string{strapiclient.TransferTokenPermissionPull})
	if err != nil {
		return "", err
	}
	return created.AccessKey, nil
}

func findTransferToken(ctx context.Context, admin tokenAdmin, name string) (strapiclient.TransferToken, bool, error) {
	tokens, err := admin.ListTransferTokens(ctx)
	if err != nil {
		return strapiclient.TransferToken{}, false, err
	}
	for _, t := range tokens {
		if t.Name == name {
			return t, true, nil
		}
	}
	return strapiclient.TransferToken{}, false, nil
}

// writeTransferTokenSecret stores a provisioned token in a Secret controlled
// by the Backup, so it is garbage collected along with it.
func writeTransferTokenSecret(ctx context.Context, kube client.Client, cr *backupsv2alpha1.Backup, token string) error {
	s := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: transferTokenSecretName(cr), Namespace: cr.GetNamespace()}}
	_, err := controllerutil.CreateOrUpdate(ctx, kube, s, func() error {
		meta.AddOwnerReference(s, meta.AsController(meta.TypedReferenceTo(cr, backupsv2alpha1.BackupGroupVersionKind)))
		s.Data = map[string][]byte{transferTokenSecretKey: []byte(token)}
		return nil
	})
	return errors.Wrap(err, errWriteTokenSecret)
}

type external struct {
	kube     client.Client
	admin    tokenAdmin
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

func (e *external) Observe(ctx context.Context, cr *backupsv2alpha1.Backup) (managed.ExternalObservation, error) {
	if meta.WasDeleted(cr) {
		return e.observeDeletion(ctx, cr)
	}
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

// observeDeletion reports whether a deleted Backup still has external state
// to clean up. The only such state is a provisioned transfer token; backups
// already written to the bucket are never removed.
func (e *external) observeDeletion(ctx context.Context, cr *backupsv2alpha1.Backup) (managed.ExternalObservation, error) {
	if e.admin == nil {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}
	_, found, err := findTransferToken(ctx, e.admin, transferTokenName(cr))
	if err != nil {
		return managed.ExternalObservation{}, errors.Wrap(err, errObserveToken)
	}
	return managed.ExternalObservation{ResourceExists: found}, nil
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
//
// The dump is streamed from Strapi straight into the bucket, so a backup
// needs no local disk — the provider container has no writable filesystem
// by default. manifest.json is written last, so a backup whose prefix lacks
// it is incomplete.
func (e *external) Update(ctx context.Context, cr *backupsv2alpha1.Backup) (managed.ExternalUpdate, error) {
	prefix := backupPrefix(cr.Spec.ForProvider.Destination.Path, time.Now().UTC())

	sink := newBucketSink(e.bucket, prefix)
	defer sink.abort()
	manifest, err := e.dump(ctx, cr, sink)
	if err != nil {
		return managed.ExternalUpdate{}, err
	}

	manifestSize, err := e.uploadManifest(ctx, prefix, manifest)
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errUpload)
	}

	cr.Status.AtProvider.LastBackupTime = &metav1.Time{Time: time.Now().UTC()}
	cr.Status.AtProvider.LastBackupKey = prefix
	cr.Status.AtProvider.LastBackupSizeBytes = dumpSize(manifest) + manifestSize
	cr.Status.AtProvider.LastBackupEntities = manifest.Entities.Count
	cr.Status.AtProvider.LastBackupAssets = int64(len(manifest.Assets))

	if days := cr.Spec.ForProvider.RetentionDays; days > 0 {
		if err := e.applyRetention(ctx, cr.Spec.ForProvider.Destination.Path, days); err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, errRetention)
		}
	}
	return managed.ExternalUpdate{}, nil
}

// dump streams the project to sink. If Strapi rejects a provisioned
// transfer token (revoked or regenerated outside the provider), its Secret
// is removed so the next reconcile provisions a new one.
func (e *external) dump(ctx context.Context, cr *backupsv2alpha1.Backup, sink *bucketSink) (strapiclient.DumpManifest, error) {
	manifest, err := e.transfer.Dump(ctx, sink, cr.Spec.ForProvider.IncludeAssets)
	if err == nil {
		return manifest, nil
	}
	if e.admin != nil && strapiclient.IsTransferUnauthorized(err) {
		s := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: transferTokenSecretName(cr), Namespace: cr.GetNamespace()}}
		if derr := client.IgnoreNotFound(e.kube.Delete(ctx, s)); derr != nil {
			return manifest, errors.Wrap(derr, errDeleteTokenSecret)
		}
	}
	return manifest, errors.Wrap(err, errDump)
}

// Delete does not remove any backups already written to the bucket — a
// Backup resource schedules future backups, it doesn't own the lifecycle of
// past ones. Retention (if configured) and bucket lifecycle rules are the
// intended way to age data out. A provisioned transfer token is deleted
// from Strapi; its Secret is garbage collected with the Backup.
func (e *external) Delete(ctx context.Context, cr *backupsv2alpha1.Backup) (managed.ExternalDelete, error) {
	cr.Status.SetConditions(xpv1.Deleting())
	if e.admin == nil {
		return managed.ExternalDelete{}, nil
	}
	tok, found, err := findTransferToken(ctx, e.admin, transferTokenName(cr))
	if err != nil {
		return managed.ExternalDelete{}, errors.Wrap(err, errDeleteToken)
	}
	if found {
		if err := e.admin.DeleteTransferToken(ctx, tok.ID); err != nil {
			return managed.ExternalDelete{}, errors.Wrap(err, errDeleteToken)
		}
	}
	return managed.ExternalDelete{}, nil
}

func (e *external) Disconnect(_ context.Context) error { return nil }

// bucketSink is a strapiclient.DumpSink that uploads each object of a dump
// to the bucket under prefix as it is written.
type bucketSink struct {
	bucket bucketClient
	prefix string

	mu   sync.Mutex
	open map[*bucketObject]struct{}
}

func newBucketSink(bucket bucketClient, prefix string) *bucketSink {
	return &bucketSink{bucket: bucket, prefix: prefix, open: map[*bucketObject]struct{}{}}
}

// Create starts uploading the object under key. Its content is whatever is
// written to the returned writer; closing the writer completes the upload
// and reports its result.
func (s *bucketSink) Create(ctx context.Context, key string) (io.WriteCloser, error) {
	pr, pw := io.Pipe()
	obj := &bucketObject{sink: s, pw: pw, done: make(chan error, 1)}
	s.mu.Lock()
	s.open[obj] = struct{}{}
	s.mu.Unlock()

	go func() {
		_, err := s.bucket.Upload(ctx, s.prefix+"/"+key, pr)
		// Fail further writes if the upload stopped reading early.
		pr.CloseWithError(err) //nolint:errcheck,gosec // PipeReader.CloseWithError always returns nil
		obj.done <- errors.Wrapf(err, "upload %s", key)
	}()
	return obj, nil
}

// abort fails every object that was created but never closed, e.g. because
// the dump failed midway, so its upload is abandoned rather than completed
// with partial content.
func (s *bucketSink) abort() {
	s.mu.Lock()
	objs := make([]*bucketObject, 0, len(s.open))
	for o := range s.open {
		objs = append(objs, o)
	}
	s.open = map[*bucketObject]struct{}{}
	s.mu.Unlock()

	// A plain error with no cause chain, so an upload can't mistake it for
	// the end of its input.
	aborted := errors.New("backup aborted before the object was complete")
	for _, o := range objs {
		o.pw.CloseWithError(aborted) //nolint:errcheck,gosec // PipeWriter.CloseWithError always returns nil
		<-o.done
	}
}

// bucketObject is the writer for one object being uploaded by a bucketSink.
type bucketObject struct {
	sink *bucketSink
	pw   *io.PipeWriter
	done chan error
}

func (o *bucketObject) Write(p []byte) (int, error) {
	return o.pw.Write(p)
}

func (o *bucketObject) Close() error {
	o.sink.mu.Lock()
	delete(o.sink.open, o)
	o.sink.mu.Unlock()
	o.pw.Close() //nolint:errcheck,gosec // PipeWriter.Close always returns nil
	return <-o.done
}

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

func dumpSize(manifest strapiclient.DumpManifest) int64 {
	total := manifest.Entities.Size + manifest.Links.Size + manifest.Configuration.Size
	for _, a := range manifest.Assets {
		total += a.Size
	}
	return total
}

func (e *external) uploadManifest(ctx context.Context, prefix string, manifest strapiclient.DumpManifest) (int64, error) {
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
	size, err := e.bucket.Upload(ctx, prefix+"/manifest.json", bytes.NewReader(b))
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
