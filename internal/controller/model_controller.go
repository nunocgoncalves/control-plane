package controller

import (
	"context"
	"encoding/json"
	"fmt"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/nunocgoncalves/control-plane/api/v1alpha1"
	"github.com/nunocgoncalves/control-plane/internal/catalog"
)

const (
	modelFinalizer       = "platform.iterabase.com/model-finalizer"
	backendRefIndexField = ".spec.backendRef"
	modelRequeueSeconds  = 30
)

// ModelReconciler materializes Model CRs into the Postgres catalog.models table
// (Git -> DB bridge) and reports availability from the referenced ModelBackend's
// health. It watches ModelBackends so availability propagates promptly. The
// effective_catalog view (HOR-268) joins Model -> ModelBackend for the gateway
// (HOR-247) to read directly.
type ModelReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Store  *catalog.Store
}

// +kubebuilder:rbac:groups=platform.iterabase.com,resources=models,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=platform.iterabase.com,resources=models/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=platform.iterabase.com,resources=models/finalizers,verbs=update
// +kubebuilder:rbac:groups=platform.iterabase.com,resources=modelbackends,verbs=get;list;watch

// Reconcile handles Model create/update/delete events.
func (r *ModelReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var m v1alpha1.Model
	if err := r.Get(ctx, req.NamespacedName, &m); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !m.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, &m)
	}

	if !controllerutil.ContainsFinalizer(&m, modelFinalizer) {
		controllerutil.AddFinalizer(&m, modelFinalizer)
		if err := r.Update(ctx, &m); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	return r.reconcileUpsert(ctx, &m)
}

// reconcileDelete soft-deletes the catalog row and removes the finalizer.
func (r *ModelReconciler) reconcileDelete(ctx context.Context, m *v1alpha1.Model) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	if !controllerutil.ContainsFinalizer(m, modelFinalizer) {
		return ctrl.Result{}, nil
	}
	key := modelKey(m)
	if err := r.Store.SoftDeleteModelByKey(ctx, key); err != nil {
		logger.Error(err, "failed to soft-delete model on CR deletion", "key", key)
		return ctrl.Result{}, err
	}
	controllerutil.RemoveFinalizer(m, modelFinalizer)
	if err := r.Update(ctx, m); err != nil {
		return ctrl.Result{}, err
	}
	logger.Info("soft-deleted model", "key", key)
	return ctrl.Result{}, nil
}

// reconcileUpsert materializes the model and reports availability.
func (r *ModelReconciler) reconcileUpsert(ctx context.Context, m *v1alpha1.Model) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if m.Spec.BackendRef == "" {
		_ = r.patchStatus(ctx, m, false, false, "spec.backendRef is required")
		return ctrl.Result{}, nil
	}

	available, healthy, msg, err := r.resolveBackend(ctx, m)
	if err != nil {
		return ctrl.Result{}, err
	}
	if err := r.materialize(ctx, m, available); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.patchStatus(ctx, m, available, healthy, msg); err != nil {
		return ctrl.Result{}, err
	}
	logger.Info("reconciled model", "key", modelKey(m), "available", available)
	// Requeue as a fallback; the ModelBackend watch handles prompt updates.
	return ctrl.Result{RequeueAfter: modelRequeueSeconds}, nil
}

// resolveBackend reads the referenced ModelBackend and derives availability.
func (r *ModelReconciler) resolveBackend(ctx context.Context, m *v1alpha1.Model) (available, healthy bool, msg string, err error) {
	var mb v1alpha1.ModelBackend
	if err = r.Get(ctx, types.NamespacedName{Name: m.Spec.BackendRef, Namespace: m.Namespace}, &mb); err != nil {
		if !errors.IsNotFound(err) {
			return false, false, "", err
		}
		return false, false, fmt.Sprintf("backend %q not found", m.Spec.BackendRef), nil
	}
	healthy = mb.Status.Healthy
	available = mb.Status.Deployed && mb.Status.Healthy
	if !available {
		msg = fmt.Sprintf("backend %q not ready (deployed=%v healthy=%v)", m.Spec.BackendRef, mb.Status.Deployed, mb.Status.Healthy)
	}
	return available, healthy, msg, nil
}

// materialize upserts the model row in catalog.models.
func (r *ModelReconciler) materialize(ctx context.Context, m *v1alpha1.Model, available bool) error {
	caps := m.Spec.Capabilities
	if caps == nil {
		caps = []string{}
	}
	capJSON, _ := json.Marshal(caps)
	dp, _ := json.Marshal(m.Spec.DefaultParams)
	rc, _ := json.Marshal(m.Spec.ReasoningConfig)
	tr, _ := json.Marshal(m.Spec.Transforms)
	rl, _ := json.Marshal(m.Spec.RateLimits)
	if _, err := r.Store.UpsertModel(ctx, catalog.Model{
		Key:             modelKey(m),
		Namespace:       m.Namespace,
		ModelID:         m.Spec.ModelID,
		DisplayName:     m.Spec.DisplayName,
		ContextLength:   m.Spec.ContextLength,
		Capabilities:    capJSON,
		BackendRef:      m.Spec.BackendRef,
		DefaultParams:   dp,
		ReasoningConfig: rc,
		Transforms:      tr,
		RateLimits:      rl,
		Available:       available,
	}); err != nil {
		return fmt.Errorf("upsert model: %w", err)
	}
	return nil
}

// modelKey is the stable natural key for a CR-sourced model ("<namespace>/<name>").
func modelKey(m *v1alpha1.Model) string {
	return fmt.Sprintf("%s/%s", m.Namespace, m.Name)
}

// patchStatus updates the CR status subresource with a merge patch.
func (r *ModelReconciler) patchStatus(ctx context.Context, m *v1alpha1.Model, available, healthy bool, message string) error {
	base := m.DeepCopy()
	now := metav1.Now()
	m.Status.Available = available
	m.Status.Healthy = healthy
	m.Status.LastChecked = &now
	m.Status.ObservedGeneration = m.Generation
	m.Status.Message = message
	return r.Status().Patch(ctx, m, client.MergeFrom(base))
}

// SetupWithManager registers the reconciler, watches ModelBackends, and indexes
// Models by backendRef so a backend health change enqueues referencing Models.
func (r *ModelReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &v1alpha1.Model{}, backendRefIndexField,
		func(obj client.Object) []string {
			return []string{obj.(*v1alpha1.Model).Spec.BackendRef}
		}); err != nil {
		return fmt.Errorf("index models by backendRef: %w", err)
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.Model{}).
		Watches(&v1alpha1.ModelBackend{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []ctrl.Request {
			mb := obj.(*v1alpha1.ModelBackend)
			var list v1alpha1.ModelList
			if err := r.List(ctx, &list, client.InNamespace(mb.Namespace), client.MatchingFields{backendRefIndexField: mb.Name}); err != nil {
				return nil
			}
			reqs := make([]ctrl.Request, 0, len(list.Items))
			for i := range list.Items {
				reqs = append(reqs, ctrl.Request{NamespacedName: types.NamespacedName{
					Name: list.Items[i].Name, Namespace: list.Items[i].Namespace,
				}})
			}
			return reqs
		})).
		Complete(r)
}
