package controller

import (
	"context"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/tewing/slackapp-k8s-operator/internal/slack"
)

const (
	keyRefreshToken = "refresh_token"
	keyAccessToken  = "access_token"
	keyExpiresAt    = "expires_at"

	// rotateBuffer rotates the access token this long before it actually
	// expires, so an in-flight reconcile never races the 12h expiry.
	rotateBuffer = 30 * time.Minute
)

// TokenManager mediates access to the Slack config tokens persisted in K8s
// Secrets — one Secret per Slack workspace, all in the operator namespace.
// Slack invalidates the previous refresh token on every rotation, so the
// rotated pair MUST be written back; this type owns that read/rotate/write
// cycle and serializes it per Secret.
type TokenManager struct {
	client    client.Client
	slack     *slack.Client
	namespace string

	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

func NewTokenManager(c client.Client, sc *slack.Client, namespace string) *TokenManager {
	return &TokenManager{
		client:    c,
		slack:     sc,
		namespace: namespace,
		locks:     map[string]*sync.Mutex{},
	}
}

// lockFor returns the per-Secret mutex, so rotation of one workspace's token
// never blocks another's but is serialized within a workspace.
func (m *TokenManager) lockFor(secretName string) *sync.Mutex {
	m.mu.Lock()
	defer m.mu.Unlock()
	l, ok := m.locks[secretName]
	if !ok {
		l = &sync.Mutex{}
		m.locks[secretName] = l
	}
	return l
}

// AccessToken returns a currently-valid Slack config access token for the named
// Secret (in the operator namespace), rotating and persisting the new pair when
// the cached token is missing or near expiry.
func (m *TokenManager) AccessToken(ctx context.Context, secretName string) (string, error) {
	lock := m.lockFor(secretName)
	lock.Lock()
	defer lock.Unlock()

	ref := types.NamespacedName{Namespace: m.namespace, Name: secretName}

	var sec corev1.Secret
	if err := m.client.Get(ctx, ref, &sec); err != nil {
		return "", fmt.Errorf("read token secret %s: %w", ref, err)
	}

	access := string(sec.Data[keyAccessToken])
	if access != "" && !expiringSoon(sec.Data[keyExpiresAt]) {
		return access, nil
	}

	refresh := string(sec.Data[keyRefreshToken])
	if refresh == "" {
		return "", fmt.Errorf("token secret %s has no %q — bootstrap it with a Slack config refresh token", ref, keyRefreshToken)
	}

	set, err := m.slack.RotateToken(ctx, refresh)
	if err != nil {
		return "", fmt.Errorf("rotate config token: %w", err)
	}

	if sec.Data == nil {
		sec.Data = map[string][]byte{}
	}
	sec.Data[keyAccessToken] = []byte(set.AccessToken)
	sec.Data[keyRefreshToken] = []byte(set.RefreshToken)
	sec.Data[keyExpiresAt] = []byte(set.ExpiresAt.UTC().Format(time.RFC3339))
	if err := m.client.Update(ctx, &sec); err != nil {
		// The rotation already burned the old refresh token; if we fail to
		// persist, surface loudly so the secret can be re-bootstrapped.
		return "", fmt.Errorf("persist rotated token to %s (MANUAL FIX may be needed): %w", ref, err)
	}
	return set.AccessToken, nil
}

func expiringSoon(raw []byte) bool {
	if len(raw) == 0 {
		return true
	}
	exp, err := time.Parse(time.RFC3339, string(raw))
	if err != nil {
		return true
	}
	return time.Now().Add(rotateBuffer).After(exp)
}
