package controller

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/nunocgoncalves/control-plane/api/v1alpha1"
	"github.com/nunocgoncalves/control-plane/internal/egress"
)

// egressRouteFinalizer ensures the Postgres cleanup runs before a CR is removed.
const egressRouteFinalizer = "platform.iterabase.com/egressroute-finalizer"

// EgressRouteReconciler materializes EgressRoute CRs into the Postgres egress
// store (Git -> DB bridge). On add/update it upserts the route (reviving a
// soft-deleted row on recreate); on delete it soft-deletes the route (it drops
// out of effective_routes -> access revoked, row retained for history). The
// credential value never enters Postgres — only the K8s Secret reference.
type EgressRouteReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Store  *egress.Store
}

// +kubebuilder:rbac:groups=platform.iterabase.com,resources=egressroutes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=platform.iterabase.com,resources=egressroutes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=platform.iterabase.com,resources=egressroutes/finalizers,verbs=update

// Reconcile handles EgressRoute create/update/delete events.
func (r *EgressRouteReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var er v1alpha1.EgressRoute
	if err := r.Get(ctx, req.NamespacedName, &er); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Deletion path: finalizer cleanup.
	if !er.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&er, egressRouteFinalizer) {
			key := egressRouteKey(&er)
			if err := r.Store.SoftDeleteRouteByKey(ctx, key); err != nil {
				logger.Error(err, "failed to soft-delete egress route on CR deletion", "key", key)
				return ctrl.Result{}, err
			}
			controllerutil.RemoveFinalizer(&er, egressRouteFinalizer)
			if err := r.Update(ctx, &er); err != nil {
				return ctrl.Result{}, err
			}
			logger.Info("soft-deleted egress route", "key", key)
		}
		return ctrl.Result{}, nil
	}

	// Ensure the finalizer is present before materializing.
	if !controllerutil.ContainsFinalizer(&er, egressRouteFinalizer) {
		controllerutil.AddFinalizer(&er, egressRouteFinalizer)
		if err := r.Update(ctx, &er); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Validate the spec before materializing.
	if err := validateEgressRoute(&er); err != nil {
		if perr := r.patchStatus(ctx, &er, false, fmt.Sprintf("invalid spec: %v", err)); perr != nil {
			return ctrl.Result{}, perr
		}
		return ctrl.Result{}, err
	}

	// Materialize the route.
	key := egressRouteKey(&er)
	if _, err := r.Store.UpsertRoute(ctx, key, er.Name, er.Namespace,
		er.Spec.Upstream.BaseURL, toEgressAuth(er.Spec.Auth), toEgressSubject(er.Spec.Subject)); err != nil {
		_ = r.patchStatus(ctx, &er, false, fmt.Sprintf("upsert route: %v", err))
		return ctrl.Result{}, err
	}

	if err := r.patchStatus(ctx, &er, true, ""); err != nil {
		return ctrl.Result{}, err
	}
	logger.Info("materialized egress route", "key", key, "routeID", er.Name,
		"upstream", er.Spec.Upstream.BaseURL, "scheme", er.Spec.Auth.Scheme)
	return ctrl.Result{}, nil
}

// patchStatus updates the CR status subresource with a merge patch.
func (r *EgressRouteReconciler) patchStatus(ctx context.Context, er *v1alpha1.EgressRoute, ready bool, message string) error {
	base := er.DeepCopy()
	er.Status.Ready = ready
	er.Status.ObservedGeneration = er.Generation
	er.Status.Message = message
	return r.Status().Patch(ctx, er, client.MergeFrom(base))
}

// validateEgressRoute checks the auth block matches its scheme.
func validateEgressRoute(er *v1alpha1.EgressRoute) error {
	a := er.Spec.Auth
	switch a.Scheme {
	case "bearer":
		if a.SecretRef == nil || a.SecretRef.Name == "" || a.SecretRef.Key == "" {
			return fmt.Errorf("bearer auth requires secretRef.{name,key}")
		}
	case "oauthClientCredentials":
		if a.TokenURL == "" {
			return fmt.Errorf("oauthClientCredentials auth requires tokenURL")
		}
		if a.ClientID == "" {
			return fmt.Errorf("oauthClientCredentials auth requires clientID")
		}
		if a.ClientSecretRef == nil || a.ClientSecretRef.Name == "" || a.ClientSecretRef.Key == "" {
			return fmt.Errorf("oauthClientCredentials auth requires clientSecretRef.{name,key}")
		}
	default:
		return fmt.Errorf("unknown auth scheme %q", a.Scheme)
	}
	if er.Spec.Upstream.BaseURL == "" {
		return fmt.Errorf("upstream.baseURL is required")
	}
	return nil
}

// egressRouteKey is the stable natural key for a CR-sourced route
// ("<namespace>/<name>"), globally unique across namespaces.
func egressRouteKey(er *v1alpha1.EgressRoute) string {
	return fmt.Sprintf("%s/%s", er.Namespace, er.Name)
}

func toEgressAuth(a v1alpha1.AuthSpec) egress.Auth {
	auth := egress.Auth{Scheme: a.Scheme, TokenURL: a.TokenURL, ClientID: a.ClientID, Scope: a.Scope}
	if a.SecretRef != nil {
		auth.SecretRef = &egress.SecretRef{Name: a.SecretRef.Name, Key: a.SecretRef.Key}
	}
	if a.ClientSecretRef != nil {
		auth.ClientSecretRef = &egress.SecretRef{Name: a.ClientSecretRef.Name, Key: a.ClientSecretRef.Key}
	}
	return auth
}

func toEgressSubject(s *v1alpha1.SubjectSpec) *egress.Subject {
	if s == nil {
		return nil
	}
	return &egress.Subject{Kind: s.Kind, Key: s.Key}
}

// SetupWithManager registers the reconciler with the controller-runtime manager.
func (r *EgressRouteReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.EgressRoute{}).
		Complete(r)
}
