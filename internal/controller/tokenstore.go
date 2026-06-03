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

// TokenStore mediates access to the Slack config tokens persisted in a K8s
// Secret. Slack invalidates the previous refresh token on every rotation, so
// the rotated pair MUST be written back; this type owns that read/rotate/write
// cycle and serializes it across concurrent reconciles.
type TokenStore struct {
	client client.Client
	slack  *slack.Client
	ref    types.NamespacedName

	mu sync.Mutex
}

func NewTokenStore(c client.Client, sc *slack.Client, secret types.NamespacedName) *TokenStore {
	return &TokenStore{client: c, slack: sc, ref: secret}
}

// AccessToken returns a currently-valid Slack config access token, rotating
// (and persisting the new pair) when the cached token is missing or near expiry.
func (s *TokenStore) AccessToken(ctx context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var sec corev1.Secret
	if err := s.client.Get(ctx, s.ref, &sec); err != nil {
		return "", fmt.Errorf("read token secret %s: %w", s.ref, err)
	}

	access := string(sec.Data[keyAccessToken])
	if access != "" && !s.expiringSoon(sec.Data[keyExpiresAt]) {
		return access, nil
	}

	refresh := string(sec.Data[keyRefreshToken])
	if refresh == "" {
		return "", fmt.Errorf("token secret %s has no %q — bootstrap it with a Slack config refresh token", s.ref, keyRefreshToken)
	}

	set, err := s.slack.RotateToken(ctx, refresh)
	if err != nil {
		return "", fmt.Errorf("rotate config token: %w", err)
	}

	if sec.Data == nil {
		sec.Data = map[string][]byte{}
	}
	sec.Data[keyAccessToken] = []byte(set.AccessToken)
	sec.Data[keyRefreshToken] = []byte(set.RefreshToken)
	sec.Data[keyExpiresAt] = []byte(set.ExpiresAt.UTC().Format(time.RFC3339))
	if err := s.client.Update(ctx, &sec); err != nil {
		// The rotation already burned the old refresh token; if we fail to
		// persist, surface loudly so the operator/secret can be re-bootstrapped.
		return "", fmt.Errorf("persist rotated token to %s (MANUAL FIX may be needed): %w", s.ref, err)
	}
	return set.AccessToken, nil
}

func (s *TokenStore) expiringSoon(raw []byte) bool {
	if len(raw) == 0 {
		return true
	}
	exp, err := time.Parse(time.RFC3339, string(raw))
	if err != nil {
		return true
	}
	return time.Now().Add(rotateBuffer).After(exp)
}
