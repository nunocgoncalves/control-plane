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
	"github.com/nunocgoncalves/control-plane/internal/permissions"
)

// permissionPolicyFinalizer ensures the Postgres cleanup runs before a CR is
// removed from the cluster.
const permissionPolicyFinalizer = "platform.iterabase.com/permissionpolicy-finalizer"

// PermissionPolicyReconciler materializes PermissionPolicy CRs into the Postgres
// permissions store. On add/update it upserts the policy (reviving a
// soft-deleted row on recreate); on delete it soft-deletes the policy (row
// retained for history). In v1 broad-default the effective-capabilities view
// ignores policy rows; enforcement lands in deepen-phase.
type PermissionPolicyReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Store  *permissions.Store
}

// +kubebuilder:rbac:groups=platform.iterabase.com,resources=permissionpolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=platform.iterabase.com,resources=permissionpolicies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=platform.iterabase.com,resources=permissionpolicies/finalizers,verbs=update

// Reconcile handles PermissionPolicy create/update/delete events.
func (r *PermissionPolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var pp v1alpha1.PermissionPolicy
	if err := r.Get(ctx, req.NamespacedName, &pp); err != nil {
		if errors.IsNotFound(err) {
			// CR is gone; cleanup happened in the deletion path below while the
			// object still existed. Nothing to do.
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Deletion path: finalizer cleanup.
	if !pp.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&pp, permissionPolicyFinalizer) {
			key := policyKey(&pp)
			if err := r.Store.SoftDeletePolicyByKey(ctx, key); err != nil {
				logger.Error(err, "failed to soft-delete policy on CR deletion", "key", key)
				return ctrl.Result{}, err
			}
			controllerutil.RemoveFinalizer(&pp, permissionPolicyFinalizer)
			if err := r.Update(ctx, &pp); err != nil {
				return ctrl.Result{}, err
			}
			logger.Info("soft-deleted policy", "key", key)
		}
		return ctrl.Result{}, nil
	}

	// Ensure the finalizer is present before materializing.
	if !controllerutil.ContainsFinalizer(&pp, permissionPolicyFinalizer) {
		controllerutil.AddFinalizer(&pp, permissionPolicyFinalizer)
		if err := r.Update(ctx, &pp); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Materialize the policy.
	key := policyKey(&pp)
	pol, err := r.Store.UpsertPolicy(ctx, key, pp.Spec.Subject.Kind, pp.Spec.Subject.Key, toRateLimits(pp.Spec.RateLimits))
	if err != nil {
		_ = r.patchStatus(ctx, &pp, false, "", fmt.Sprintf("upsert policy: %v", err))
		return ctrl.Result{}, err
	}

	if err := r.patchStatus(ctx, &pp, true, pol.ID, ""); err != nil {
		return ctrl.Result{}, err
	}
	logger.Info("materialized policy", "key", key, "policyID", pol.ID)
	return ctrl.Result{}, nil
}

// patchStatus updates the CR status subresource with a merge patch.
func (r *PermissionPolicyReconciler) patchStatus(ctx context.Context, pp *v1alpha1.PermissionPolicy, ready bool, policyID, message string) error {
	base := pp.DeepCopy()
	pp.Status.Ready = ready
	pp.Status.PolicyID = policyID
	pp.Status.ObservedGeneration = pp.Generation
	pp.Status.Message = message
	return r.Status().Patch(ctx, pp, client.MergeFrom(base))
}

// policyKey is the stable natural key for a CR-sourced policy
// ("<namespace>/<name>"), globally unique across namespaces.
func policyKey(pp *v1alpha1.PermissionPolicy) string {
	return fmt.Sprintf("%s/%s", pp.Namespace, pp.Name)
}

// toRateLimits converts the CRD rate-limits spec to the store type (nil = unlimited).
func toRateLimits(rl *v1alpha1.RateLimitsSpec) *permissions.RateLimits {
	if rl == nil {
		return nil
	}
	return &permissions.RateLimits{RPM: rl.RPM, TPM: rl.TPM}
}

// SetupWithManager registers the reconciler with the controller-runtime manager.
func (r *PermissionPolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.PermissionPolicy{}).
		Complete(r)
}
