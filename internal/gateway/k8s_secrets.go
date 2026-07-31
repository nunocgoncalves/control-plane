package gateway

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// k8sSecretResolver reads K8s Secret values via the in-cluster API (ARCH-008).
// RBAC scopes it to the platform namespace + Secret resource only. Values live
// only in gateway memory + the runner's per-invocation context.
type k8sSecretResolver struct {
	client    kubernetes.Interface
	namespace string
}

// NewK8sSecretResolver builds a SecretResolver from an in-cluster (or override)
// client + namespace.
func NewK8sSecretResolver(client kubernetes.Interface, namespace string) SecretResolver {
	return &k8sSecretResolver{client: client, namespace: namespace}
}

func (k *k8sSecretResolver) Resolve(ctx context.Context, ref SecretRef) (string, error) {
	if ref.Name == "" || ref.Key == "" {
		return "", fmt.Errorf("secret ref must name a secret and key")
	}
	sec, err := k.client.CoreV1().Secrets(k.namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return "", fmt.Errorf("secret %s/%s not found", k.namespace, ref.Name)
		}
		return "", fmt.Errorf("get secret %s/%s: %w", k.namespace, ref.Name, err)
	}
	val, ok := sec.Data[ref.Key]
	if !ok {
		return "", fmt.Errorf("secret %s/%s has no key %q", k.namespace, ref.Name, ref.Key)
	}
	if sec.Type == corev1.SecretTypeServiceAccountToken {
		// Guard: never accidentally return a SA token as a customer credential.
		return "", fmt.Errorf("refusing to read service-account-token secret %s/%s", k.namespace, ref.Name)
	}
	return string(val), nil
}
