package controller

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/nunocgoncalves/control-plane/api/v1alpha1"
	"github.com/nunocgoncalves/control-plane/internal/catalog"
)

const (
	modelBackendFinalizer = "platform.iterabase.com/modelbackend-finalizer"

	defaultVLLMImage      = "vllm/vllm-openai:latest" // TODO(HOR-306): pin a stable tag/digest.
	defaultServingPort    = 8000
	defaultHealthPath     = "/health"
	defaultModelCachePath = "/data/hf-cache"
	gpuResourceName       = corev1.ResourceName("nvidia.com/gpu")
	gpuNodeLabel          = "nvidia.com/gpu.present"
	nvidiaRuntimeClass    = "nvidia"
	healthRequeueSeconds  = 30
)

// ModelBackendReconciler deploys internal serving workloads (vLLM) or records
// external providers, and materializes ModelBackend CRs into the Postgres
// catalog.backends table (Git -> DB bridge). SGLang is a recognized kind with a
// stubbed reconciler (HOR-323); external reachability is deferred to HOR-307.
type ModelBackendReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Store  *catalog.Store
}

// +kubebuilder:rbac:groups=platform.iterabase.com,resources=modelbackends,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=platform.iterabase.com,resources=modelbackends/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=platform.iterabase.com,resources=modelbackends/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete

// Reconcile handles ModelBackend create/update/delete events.
func (r *ModelBackendReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var mb v1alpha1.ModelBackend
	if err := r.Get(ctx, req.NamespacedName, &mb); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Deletion path: finalizer cleanup.
	if !mb.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&mb, modelBackendFinalizer) {
			key := backendKey(&mb)
			if err := r.Store.SoftDeleteBackendByKey(ctx, key); err != nil {
				logger.Error(err, "failed to soft-delete backend on CR deletion", "key", key)
				return ctrl.Result{}, err
			}
			controllerutil.RemoveFinalizer(&mb, modelBackendFinalizer)
			if err := r.Update(ctx, &mb); err != nil {
				return ctrl.Result{}, err
			}
			logger.Info("soft-deleted backend", "key", key)
		}
		return ctrl.Result{}, nil
	}

	// Ensure the finalizer is present before materializing.
	if !controllerutil.ContainsFinalizer(&mb, modelBackendFinalizer) {
		controllerutil.AddFinalizer(&mb, modelBackendFinalizer)
		if err := r.Update(ctx, &mb); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	switch mb.Spec.Kind {
	case "vLLM":
		return r.reconcileVLLM(ctx, &mb)
	case "SGLang":
		return r.reconcileStub(ctx, &mb, "SGLang reconciler not yet implemented (HOR-323)")
	case "external":
		return r.reconcileExternal(ctx, &mb)
	default:
		return ctrl.Result{}, r.patchStatus(ctx, &mb, false, false, "", fmt.Sprintf("unknown kind %q", mb.Spec.Kind))
	}
}

// reconcileVLLM deploys a vLLM Deployment + Service and materializes the
// backend. Health is derived from the Deployment's Available condition (driven
// by the pod readinessProbe on /health). envtest has no kubelet, so healthy
// stays false there — real serving is validated on the GPU VM (manual runbook)
// and the forge GPU E2E (HOR-324).
func (r *ModelBackendReconciler) reconcileVLLM(ctx context.Context, mb *v1alpha1.ModelBackend) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if mb.Spec.Model == "" {
		return ctrl.Result{}, r.patchStatus(ctx, mb, false, false, "", "spec.model is required for kind vLLM")
	}

	port := servingPort(mb)
	if err := r.ensureDeployment(ctx, mb, port); err != nil {
		_ = r.patchStatus(ctx, mb, false, false, "", fmt.Sprintf("ensure deployment: %v", err))
		return ctrl.Result{}, err
	}
	if err := r.ensureService(ctx, mb, port); err != nil {
		_ = r.patchStatus(ctx, mb, false, false, "", fmt.Sprintf("ensure service: %v", err))
		return ctrl.Result{}, err
	}

	healthy := r.deploymentHealthy(ctx, mb)
	serviceURL := fmt.Sprintf("%s.%s.svc:%d", mb.Name, mb.Namespace, port)
	if err := r.materialize(ctx, mb, serviceURL, true, healthy); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.patchStatus(ctx, mb, true, healthy, serviceURL, ""); err != nil {
		return ctrl.Result{}, err
	}
	logger.Info("reconciled vLLM backend", "key", backendKey(mb), "healthy", healthy)
	// Requeue to re-evaluate health as pods come and go.
	return ctrl.Result{RequeueAfter: healthRequeueSeconds}, nil
}

// reconcileExternal records an external provider backend. No workload is
// deployed; reachability validation is deferred to HOR-307, so healthy is
// assumed true in the skeleton.
func (r *ModelBackendReconciler) reconcileExternal(ctx context.Context, mb *v1alpha1.ModelBackend) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	if mb.Spec.External == nil || mb.Spec.External.BaseURL == "" {
		return ctrl.Result{}, r.patchStatus(ctx, mb, false, false, "", "spec.external.baseURL is required for kind external")
	}
	serviceURL := mb.Spec.External.BaseURL
	if err := r.materialize(ctx, mb, serviceURL, true, true); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.patchStatus(ctx, mb, true, true, serviceURL, ""); err != nil {
		return ctrl.Result{}, err
	}
	logger.Info("recorded external backend", "key", backendKey(mb), "baseURL", serviceURL)
	return ctrl.Result{}, nil
}

// reconcileStub materializes a backend row for a recognized-but-unimplemented
// kind (SGLang) and surfaces a message.
func (r *ModelBackendReconciler) reconcileStub(ctx context.Context, mb *v1alpha1.ModelBackend, msg string) (ctrl.Result, error) {
	if err := r.materialize(ctx, mb, "", false, false); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, r.patchStatus(ctx, mb, false, false, "", msg)
}

// materialize upserts the backend row in catalog.backends.
func (r *ModelBackendReconciler) materialize(ctx context.Context, mb *v1alpha1.ModelBackend, serviceURL string, deployed, healthy bool) error {
	if _, err := r.Store.UpsertBackend(ctx, catalog.Backend{
		Key:        backendKey(mb),
		Name:       mb.Name,
		Namespace:  mb.Namespace,
		Kind:       mb.Spec.Kind,
		Model:      mb.Spec.Model,
		ServiceURL: serviceURL,
		Image:      mb.Spec.Image,
		Deployed:   deployed,
		Healthy:    healthy,
	}); err != nil {
		return fmt.Errorf("upsert backend: %w", err)
	}
	return nil
}

// ensureDeployment creates or updates the vLLM Deployment.
func (r *ModelBackendReconciler) ensureDeployment(ctx context.Context, mb *v1alpha1.ModelBackend, port int32) error {
	dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: mb.Name, Namespace: mb.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, dep, func() error {
		if err := controllerutil.SetControllerReference(mb, dep, r.Scheme); err != nil {
			return err
		}
		dep.Spec = buildDeploymentSpec(mb, port)
		return nil
	})
	return err
}

// ensureService creates or updates the backend Service.
func (r *ModelBackendReconciler) ensureService(ctx context.Context, mb *v1alpha1.ModelBackend, port int32) error {
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: mb.Name, Namespace: mb.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		if err := controllerutil.SetControllerReference(mb, svc, r.Scheme); err != nil {
			return err
		}
		labels := backendLabels(mb)
		svc.Labels = labels
		svc.Spec.Selector = labels
		svc.Spec.Ports = []corev1.ServicePort{{
			Port:       port,
			TargetPort: intstr.FromInt(int(port)),
			Protocol:   corev1.ProtocolTCP,
			Name:       "http",
		}}
		svc.Spec.Type = corev1.ServiceTypeClusterIP
		return nil
	})
	return err
}

// deploymentHealthy reports the Deployment's Available condition.
func (r *ModelBackendReconciler) deploymentHealthy(ctx context.Context, mb *v1alpha1.ModelBackend) bool {
	dep := &appsv1.Deployment{}
	if err := r.Get(ctx, client.ObjectKey{Name: mb.Name, Namespace: mb.Namespace}, dep); err != nil {
		return false
	}
	for _, c := range dep.Status.Conditions {
		if c.Type == appsv1.DeploymentAvailable {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// buildDeploymentSpec renders the vLLM pod spec with the GPU contract from forge
// (HOR-240): runtimeClassName nvidia + nvidia.com/gpu request + GPU node
// selector. hostPath model cache avoids re-downloading weights on restart.
func buildDeploymentSpec(mb *v1alpha1.ModelBackend, port int32) appsv1.DeploymentSpec {
	replicas := int32(1)
	if mb.Spec.Replicas != nil {
		replicas = *mb.Spec.Replicas
	}
	image := mb.Spec.Image
	if image == "" {
		image = defaultVLLMImage
	}

	resources := mb.Spec.Resources
	if resources.Limits == nil {
		resources.Limits = corev1.ResourceList{}
	}
	if resources.Requests == nil {
		resources.Requests = corev1.ResourceList{}
	}
	if _, ok := resources.Limits[gpuResourceName]; !ok {
		resources.Limits[gpuResourceName] = resource.MustParse("1")
	}
	if _, ok := resources.Requests[gpuResourceName]; !ok {
		resources.Requests[gpuResourceName] = resource.MustParse("1")
	}

	nodeSelector := mb.Spec.NodeSelector
	if len(nodeSelector) == 0 {
		nodeSelector = map[string]string{gpuNodeLabel: "true"}
	}

	hostPathType := corev1.HostPathDirectoryOrCreate
	runtimeClassName := nvidiaRuntimeClass
	probe := &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{Path: healthPath(mb), Port: intstr.FromInt(int(port))},
		},
	}
	// startupProbe gives vLLM time to download the model + load it into GPU before
	// the liveness probe can kill it. vLLM is slow to start serving /health
	// (model download + GPU load takes minutes); without this the liveness probe
	// (30s) kills the container in a CrashLoopBackOff before it ever serves.
	startupProbe := &corev1.Probe{
		ProbeHandler:     probe.ProbeHandler,
		PeriodSeconds:    10,
		FailureThreshold: 60, // 10 minutes for model download + GPU load
	}

	return appsv1.DeploymentSpec{
		Replicas: &replicas,
		Selector: &metav1.LabelSelector{MatchLabels: backendLabels(mb)},
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: backendLabels(mb)},
			Spec: corev1.PodSpec{
				RuntimeClassName: &runtimeClassName,
				NodeSelector:     nodeSelector,
				Tolerations:      mb.Spec.Tolerations,
				Volumes: []corev1.Volume{{
					Name: "hf-cache",
					VolumeSource: corev1.VolumeSource{
						HostPath: &corev1.HostPathVolumeSource{
							Path: defaultModelCachePath,
							Type: &hostPathType,
						},
					},
				}},
				Containers: []corev1.Container{{
					Name:  "server",
					Image: image,
					Args:  []string{"--model", mb.Spec.Model, "--port", fmt.Sprintf("%d", port), "--host", "0.0.0.0"},
					Env:   []corev1.EnvVar{{Name: "HF_HOME", Value: defaultModelCachePath}},
					Ports: []corev1.ContainerPort{{ContainerPort: port, Name: "http"}},
					VolumeMounts: []corev1.VolumeMount{{
						Name:      "hf-cache",
						MountPath: defaultModelCachePath,
					}},
					Resources:      resources,
					StartupProbe:   startupProbe,
					ReadinessProbe: probe,
					LivenessProbe:  probe,
				}},
			},
		},
	}
}

// backendLabels returns the labels shared by the Deployment, its pods, and the
// Service selector.
func backendLabels(mb *v1alpha1.ModelBackend) map[string]string {
	return map[string]string{
		"app.kubernetes.io/managed-by":        "control-plane",
		"platform.iterabase.com/modelbackend": mb.Name,
	}
}

func servingPort(mb *v1alpha1.ModelBackend) int32 {
	if mb.Spec.HealthProbe != nil && mb.Spec.HealthProbe.Port != 0 {
		return mb.Spec.HealthProbe.Port
	}
	return defaultServingPort
}

func healthPath(mb *v1alpha1.ModelBackend) string {
	if mb.Spec.HealthProbe != nil && mb.Spec.HealthProbe.Path != "" {
		return mb.Spec.HealthProbe.Path
	}
	return defaultHealthPath
}

// backendKey is the stable natural key for a CR-sourced backend
// ("<namespace>/<name>"), mirroring the identity/permission keys.
func backendKey(mb *v1alpha1.ModelBackend) string {
	return fmt.Sprintf("%s/%s", mb.Namespace, mb.Name)
}

// patchStatus updates the CR status subresource with a merge patch.
func (r *ModelBackendReconciler) patchStatus(ctx context.Context, mb *v1alpha1.ModelBackend, deployed, healthy bool, serviceURL, message string) error {
	base := mb.DeepCopy()
	now := metav1.Now()
	mb.Status.Deployed = deployed
	mb.Status.Healthy = healthy
	mb.Status.ServiceURL = serviceURL
	mb.Status.LastReconciled = &now
	mb.Status.ObservedGeneration = mb.Generation
	mb.Status.Message = message
	return r.Status().Patch(ctx, mb, client.MergeFrom(base))
}

// SetupWithManager registers the reconciler with the controller-runtime manager
// and watches owned Deployments/Services for status propagation.
func (r *ModelBackendReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.ModelBackend{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Complete(r)
}
