// Package controller contains the control-plane Kubernetes reconcilers. Each
// reconciler materializes a CRD into the Postgres store (Git -> DB bridge).
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
	"github.com/nunocgoncalves/control-plane/internal/identity"
)

// identityMappingFinalizer ensures the Postgres cleanup runs before a CR is
// removed from the cluster.
const identityMappingFinalizer = "platform.iterabase.com/identitymapping-finalizer"

// IdentityMappingReconciler materializes IdentityMapping CRs into the Postgres
// identity store. On add/update it upserts the identity (reviving a
// soft-deleted row on recreate) and replaces its external bindings; on delete
// it soft-deletes the identity and removes its bindings (access revoked, row
// retained for usage/history).
type IdentityMappingReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Store  *identity.Store
}

// +kubebuilder:rbac:groups=platform.iterabase.com,resources=identitymappings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=platform.iterabase.com,resources=identitymappings/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=platform.iterabase.com,resources=identitymappings/finalizers,verbs=update

// Reconcile handles IdentityMapping create/update/delete events.
func (r *IdentityMappingReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var im v1alpha1.IdentityMapping
	if err := r.Get(ctx, req.NamespacedName, &im); err != nil {
		if errors.IsNotFound(err) {
			// CR is gone; cleanup happened in the deletion path below while the
			// object still existed. Nothing to do.
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Deletion path: finalizer cleanup.
	if !im.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&im, identityMappingFinalizer) {
			key := identityKey(&im)
			if err := r.Store.SoftDeleteIdentityByKey(ctx, key); err != nil {
				logger.Error(err, "failed to soft-delete identity on CR deletion", "key", key)
				return ctrl.Result{}, err
			}
			controllerutil.RemoveFinalizer(&im, identityMappingFinalizer)
			if err := r.Update(ctx, &im); err != nil {
				return ctrl.Result{}, err
			}
			logger.Info("soft-deleted identity", "key", key)
		}
		return ctrl.Result{}, nil
	}

	// Ensure the finalizer is present before materializing.
	if !controllerutil.ContainsFinalizer(&im, identityMappingFinalizer) {
		controllerutil.AddFinalizer(&im, identityMappingFinalizer)
		if err := r.Update(ctx, &im); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Materialize the identity + bindings.
	key := identityKey(&im)
	ident, err := r.Store.UpsertIdentity(ctx, key, im.Spec.Identity.Kind, identity.SourceExternal, im.Spec.Identity.DisplayName)
	if err != nil {
		_ = r.patchStatus(ctx, &im, false, "", fmt.Sprintf("upsert identity: %v", err))
		return ctrl.Result{}, err
	}

	if err := r.Store.ReplaceExternalMappings(ctx, ident.ID, toBindings(im.Spec.Bindings)); err != nil {
		_ = r.patchStatus(ctx, &im, false, ident.ID, fmt.Sprintf("replace bindings: %v", err))
		return ctrl.Result{}, err
	}

	if err := r.patchStatus(ctx, &im, true, ident.ID, ""); err != nil {
		return ctrl.Result{}, err
	}
	logger.Info("materialized identity", "key", key, "identityID", ident.ID, "bindings", len(im.Spec.Bindings))
	return ctrl.Result{}, nil
}

// patchStatus updates the CR status subresource with a merge patch.
func (r *IdentityMappingReconciler) patchStatus(ctx context.Context, im *v1alpha1.IdentityMapping, ready bool, identityID, message string) error {
	base := im.DeepCopy()
	im.Status.Ready = ready
	im.Status.IdentityID = identityID
	im.Status.ObservedGeneration = im.Generation
	im.Status.Message = message
	return r.Status().Patch(ctx, im, client.MergeFrom(base))
}

// identityKey is the stable natural key for a CR-sourced identity
// ("<namespace>/<name>"), globally unique across namespaces.
func identityKey(im *v1alpha1.IdentityMapping) string {
	return fmt.Sprintf("%s/%s", im.Namespace, im.Name)
}

// toBindings converts CRD bindings to store bindings.
func toBindings(in []v1alpha1.Binding) []identity.Binding {
	out := make([]identity.Binding, len(in))
	for i, b := range in {
		out[i] = identity.Binding{Provider: b.Provider, Type: b.Type, ExternalID: b.ExternalID}
	}
	return out
}

// SetupWithManager registers the reconciler with the controller-runtime manager.
func (r *IdentityMappingReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.IdentityMapping{}).
		Complete(r)
}
