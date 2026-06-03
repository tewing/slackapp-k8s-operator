package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// workspaceAnnotation selects which Slack workspace a SlackApp belongs to. Its
// value is a label resolved to a token Secret via the workspace ConfigMap.
const workspaceAnnotation = "slack.te-labs.org/workspace"

// WorkspaceResolver maps a workspace label (from the SlackApp annotation) to the
// name of the Secret holding that workspace's Slack config tokens, using a
// ConfigMap in the operator namespace as the mapping. Each workspace has exactly
// one refresh token, hence one Secret.
type WorkspaceResolver struct {
	client client.Client
	ref    types.NamespacedName
}

func NewWorkspaceResolver(c client.Client, configMap types.NamespacedName) *WorkspaceResolver {
	return &WorkspaceResolver{client: c, ref: configMap}
}

// SecretFor returns the token Secret name mapped to the given workspace label.
func (w *WorkspaceResolver) SecretFor(ctx context.Context, label string) (string, error) {
	var cm corev1.ConfigMap
	if err := w.client.Get(ctx, w.ref, &cm); err != nil {
		return "", fmt.Errorf("read workspace config %s: %w", w.ref, err)
	}
	secret, ok := cm.Data[label]
	if !ok || secret == "" {
		return "", fmt.Errorf("workspace %q is not defined in %s", label, w.ref)
	}
	return secret, nil
}
