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

package rolepermissions

import (
	"context"
	"reflect"
	"sort"
	"strconv"

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
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	permv1alpha1 "github.com/web-seven/provider-strapi/apis/permissions/v1alpha1"
	apisv1alpha1 "github.com/web-seven/provider-strapi/apis/v1alpha1"
	strapiclient "github.com/web-seven/provider-strapi/internal/clients/strapi"
)

const (
	errTrackPCUsage = "cannot track ProviderConfig usage"
	errGetPC        = "cannot get ProviderConfig"
	errGetCPC       = "cannot get ClusterProviderConfig"
	errGetCreds     = "cannot get credentials"
	errParseCreds   = "cannot parse credentials"
	errNewClient    = "cannot create Strapi client"
	errListRoles    = "cannot list users-permissions roles"
	errRoleNotFound = "role not found in Strapi"
	errUpdateRole   = "cannot update role permissions"
)

// SetupGated registers the controller with safe-start support.
func SetupGated(mgr ctrl.Manager, o controller.Options) error {
	o.Gate.Register(func() {
		if err := Setup(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup RolePermissions controller"))
		}
	}, permv1alpha1.RolePermissionsGroupVersionKind)
	return nil
}

// Setup adds a controller that reconciles RolePermissions managed resources.
func Setup(mgr ctrl.Manager, o controller.Options) error {
	name := managed.ControllerName(permv1alpha1.RolePermissionsGroupKind)

	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*permv1alpha1.RolePermissions](&connector{
			kube:  mgr.GetClient(),
			usage: resource.NewProviderConfigUsageTracker(mgr.GetClient(), &apisv1alpha1.ProviderConfigUsage{}),
			newClientFn: func(cfg strapiclient.Config) (strapiClient, error) {
				return strapiclient.New(cfg)
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
			&permv1alpha1.RolePermissionsList{}, o.MetricOptions.PollStateMetricInterval,
		)
		if err := mgr.Add(stateMetricsRecorder); err != nil {
			return errors.Wrap(err, "cannot register MR state metrics recorder")
		}
	}

	r := managed.NewReconciler(mgr, resource.ManagedKind(permv1alpha1.RolePermissionsGroupVersionKind), opts...)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&permv1alpha1.RolePermissions{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}

// strapiClient is the subset of the Strapi client this controller depends on.
// Defined as an interface so tests can substitute a fake without spinning up
// an HTTP server.
type strapiClient interface {
	ListRoles(ctx context.Context) ([]strapiclient.Role, error)
	UpdateRole(ctx context.Context, id int, role strapiclient.Role) error
}

type connector struct {
	kube        client.Client
	usage       *resource.ProviderConfigUsageTracker
	newClientFn func(cfg strapiclient.Config) (strapiClient, error)
}

func (c *connector) Connect(ctx context.Context, cr *permv1alpha1.RolePermissions) (managed.TypedExternalClient[*permv1alpha1.RolePermissions], error) {
	if err := c.usage.Track(ctx, cr); err != nil {
		return nil, errors.Wrap(err, errTrackPCUsage)
	}

	var (
		endpoint    string
		insecure    bool
		credsSelect apisv1alpha1.ProviderCredentials
	)

	ref := cr.GetProviderConfigReference()
	switch ref.Kind {
	case "ProviderConfig":
		pc := &apisv1alpha1.ProviderConfig{}
		if err := c.kube.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: cr.GetNamespace()}, pc); err != nil {
			return nil, errors.Wrap(err, errGetPC)
		}
		endpoint, insecure, credsSelect = pc.Spec.Endpoint, pc.Spec.InsecureSkipTLSVerify, pc.Spec.Credentials
	case "ClusterProviderConfig":
		cpc := &apisv1alpha1.ClusterProviderConfig{}
		if err := c.kube.Get(ctx, types.NamespacedName{Name: ref.Name}, cpc); err != nil {
			return nil, errors.Wrap(err, errGetCPC)
		}
		endpoint, insecure, credsSelect = cpc.Spec.Endpoint, cpc.Spec.InsecureSkipTLSVerify, cpc.Spec.Credentials
	default:
		return nil, errors.Errorf("unsupported provider config kind: %s", ref.Kind)
	}

	credBytes, err := resource.CommonCredentialExtractor(ctx, credsSelect.Source, c.kube, credsSelect.CommonCredentialSelectors)
	if err != nil {
		return nil, errors.Wrap(err, errGetCreds)
	}
	creds, err := strapiclient.ParseCredentials(credBytes)
	if err != nil {
		return nil, errors.Wrap(err, errParseCreds)
	}

	sc, err := c.newClientFn(strapiclient.Config{
		Endpoint:              endpoint,
		Credentials:           creds,
		InsecureSkipTLSVerify: insecure,
	})
	if err != nil {
		return nil, errors.Wrap(err, errNewClient)
	}
	return &external{client: sc}, nil
}

type external struct {
	client strapiClient
}

func (e *external) Observe(ctx context.Context, cr *permv1alpha1.RolePermissions) (managed.ExternalObservation, error) {
	if meta.WasDeleted(cr) {
		// Built-in roles can't be deleted; we treat Delete as a no-op below.
		// Returning ResourceExists=false short-circuits the reconcile loop
		// and lets the finalizer be removed.
		return managed.ExternalObservation{ResourceExists: false}, nil
	}

	role, err := e.resolveRole(ctx, cr)
	if err != nil {
		return managed.ExternalObservation{}, err
	}

	meta.SetExternalName(cr, strconv.Itoa(role.ID))

	current := strapiclient.FlattenPermissions(role.Permissions)
	desired := append([]string(nil), cr.Spec.ForProvider.Permissions...)
	sort.Strings(desired)

	cr.Status.AtProvider = permv1alpha1.RolePermissionsObservation{
		RoleID:      role.ID,
		Type:        role.Type,
		Permissions: current,
	}
	cr.Status.SetConditions(xpv1.Available())

	return managed.ExternalObservation{
		ResourceExists:   true,
		ResourceUpToDate: reflect.DeepEqual(current, desired),
	}, nil
}

func (e *external) Create(ctx context.Context, cr *permv1alpha1.RolePermissions) (managed.ExternalCreation, error) {
	// The role itself already exists in Strapi (we never create roles in
	// this MR). "Create" here means "first-time set the permissions" — the
	// same operation as Update.
	cr.Status.SetConditions(xpv1.Creating())
	if err := e.applyPermissions(ctx, cr); err != nil {
		return managed.ExternalCreation{}, err
	}
	return managed.ExternalCreation{}, nil
}

func (e *external) Update(ctx context.Context, cr *permv1alpha1.RolePermissions) (managed.ExternalUpdate, error) {
	if err := e.applyPermissions(ctx, cr); err != nil {
		return managed.ExternalUpdate{}, err
	}
	return managed.ExternalUpdate{}, nil
}

func (e *external) Delete(_ context.Context, cr *permv1alpha1.RolePermissions) (managed.ExternalDelete, error) {
	cr.Status.SetConditions(xpv1.Deleting())
	// Built-in roles (Public, Authenticated) can't be removed from Strapi,
	// and we don't manage role lifecycle in this MR. Leave the role's
	// permissions as they are and let the finalizer be removed.
	return managed.ExternalDelete{}, nil
}

func (e *external) Disconnect(_ context.Context) error { return nil }

// resolveRole locates the target role: it prefers the cached external-name
// annotation (numeric role ID) for stability, falling back to selector
// resolution by type or name. A non-numeric external-name (which is what
// crossplane-runtime's default NameAsExternalName initializer writes — the
// CR's metadata.name — on first reconcile, before our Observe overwrites
// it) is treated as "not set" rather than an error.
func (e *external) resolveRole(ctx context.Context, cr *permv1alpha1.RolePermissions) (strapiclient.Role, error) {
	roles, err := e.client.ListRoles(ctx)
	if err != nil {
		return strapiclient.Role{}, errors.Wrap(err, errListRoles)
	}

	if extName := meta.GetExternalName(cr); extName != "" {
		if id, err := strconv.Atoi(extName); err == nil {
			for _, r := range roles {
				if r.ID == id {
					return r, nil
				}
			}
			// Numeric external-name set but role not found — fall through to
			// selector resolution so a renumbered or recreated role can be
			// picked up.
		}
		// Non-numeric external-name: ignore and fall through to selector
		// resolution. Observe will overwrite the annotation with the resolved
		// role's ID.
	}

	role, ok := strapiclient.FindRole(roles, cr.Spec.ForProvider.Role)
	if !ok {
		return strapiclient.Role{}, errors.Errorf("%s: %q", errRoleNotFound, cr.Spec.ForProvider.Role)
	}
	return role, nil
}

func (e *external) applyPermissions(ctx context.Context, cr *permv1alpha1.RolePermissions) error {
	role, err := e.resolveRole(ctx, cr)
	if err != nil {
		return err
	}
	role.Permissions = strapiclient.ExpandPermissions(cr.Spec.ForProvider.Permissions)
	if err := e.client.UpdateRole(ctx, role.ID, role); err != nil {
		return errors.Wrap(err, errUpdateRole)
	}
	return nil
}
