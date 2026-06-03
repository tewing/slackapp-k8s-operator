package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	slackv1alpha1 "github.com/tewing/slackapp-k8s-operator/api/v1alpha1"
	"github.com/tewing/slackapp-k8s-operator/internal/slack"
)

const (
	finalizer = "slack.te-labs.org/finalizer"

	condReady       = "Ready"
	condIconApplied = "IconApplied"

	// resyncPeriod keeps tokens warm and corrects drift even when nothing
	// changes in the CR.
	resyncPeriod = time.Hour

	maxIconBytes = 5 << 20 // 5 MiB

	// iconUserAgent identifies the operator when fetching icon images. Some
	// hosts (e.g. Wikimedia) return 403 to the Go default User-Agent and
	// require a descriptive one.
	iconUserAgent = "slackapp-k8s-operator (+https://github.com/tewing/slackapp-k8s-operator)"
)

// SlackAppReconciler reconciles a SlackApp object against the Slack API.
type SlackAppReconciler struct {
	client.Client
	// APIReader is an uncached reader (direct to the API server). The create
	// decision is gated on it rather than the cached client, because the cache
	// can lag a just-persisted status write — which previously let a second
	// reconcile re-run the non-idempotent apps.manifest.create.
	APIReader client.Reader
	Slack     *slack.Client
	Tokens    *TokenStore
}

// +kubebuilder:rbac:groups=slack.te-labs.org,resources=slackapps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=slack.te-labs.org,resources=slackapps/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=slack.te-labs.org,resources=slackapps/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;update

func (r *SlackAppReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	var app slackv1alpha1.SlackApp
	if err := r.Get(ctx, req.NamespacedName, &app); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !app.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, &app)
	}

	if controllerutil.AddFinalizer(&app, finalizer) {
		if err := r.Update(ctx, &app); err != nil {
			return ctrl.Result{}, err
		}
		// Requeue: re-fetch with the finalizer persisted before doing work.
		return ctrl.Result{Requeue: true}, nil
	}

	manifestJSON, manifestHash, err := canonicalManifest(app.Spec.Manifest.Raw)
	if err != nil {
		return r.fail(ctx, req.NamespacedName, "InvalidManifest", err)
	}

	token, err := r.Tokens.AccessToken(ctx)
	if err != nil {
		return r.fail(ctx, req.NamespacedName, "TokenUnavailable", err)
	}

	// Authoritative app ID. The cached status can be stale immediately after a
	// create; an uncached read ensures we never create a second app for a CR
	// that already has one recorded.
	appID, err := r.currentAppID(ctx, req.NamespacedName, app.Status.AppID)
	if err != nil {
		return ctrl.Result{}, err
	}

	switch {
	case appID == "":
		newID, err := r.Slack.CreateApp(ctx, token, manifestJSON)
		if err != nil {
			return r.fail(ctx, req.NamespacedName, "CreateFailed", err)
		}
		l.Info("created Slack app", "appID", newID)
		appID = newID
		// Record the new ID immediately and durably, before any further work or
		// requeue can re-enter and create a duplicate. apps.manifest.create is
		// not idempotent, so this write is the only thing preventing duplicates.
		if err := r.patchStatus(ctx, req.NamespacedName, func(a *slackv1alpha1.SlackApp) {
			a.Status.AppID = newID
			a.Status.ManifestHash = manifestHash
		}); err != nil {
			l.Error(err, "created Slack app but FAILED to persist its ID — manual cleanup may be needed", "appID", newID)
			return ctrl.Result{}, err
		}
	case app.Status.ManifestHash != manifestHash:
		if err := r.Slack.UpdateApp(ctx, token, appID, manifestJSON); err != nil {
			return r.fail(ctx, req.NamespacedName, "UpdateFailed", err)
		}
		l.Info("updated Slack app", "appID", appID)
	}

	icon := r.reconcileIcon(ctx, appID, app.Spec.IconURL, app.Status.IconHash, token)

	if err := r.patchStatus(ctx, req.NamespacedName, func(a *slackv1alpha1.SlackApp) {
		a.Status.AppID = appID
		a.Status.ManifestHash = manifestHash
		a.Status.ObservedGeneration = a.Generation
		meta.SetStatusCondition(&a.Status.Conditions, metav1.Condition{
			Type:               condReady,
			Status:             metav1.ConditionTrue,
			Reason:             "Synced",
			Message:            fmt.Sprintf("Slack app %s is in sync", appID),
			ObservedGeneration: a.Generation,
		})
		icon.apply(a)
	}); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: resyncPeriod}, nil
}

// currentAppID returns the cached value if set, otherwise consults the API
// server directly (the cache may not yet reflect a freshly persisted AppID).
func (r *SlackAppReconciler) currentAppID(ctx context.Context, key types.NamespacedName, cached string) (string, error) {
	if cached != "" {
		return cached, nil
	}
	var fresh slackv1alpha1.SlackApp
	if err := r.APIReader.Get(ctx, key, &fresh); err != nil {
		return "", err
	}
	return fresh.Status.AppID, nil
}

func (r *SlackAppReconciler) reconcileDelete(ctx context.Context, app *slackv1alpha1.SlackApp) (ctrl.Result, error) {
	l := log.FromContext(ctx)
	if !controllerutil.ContainsFinalizer(app, finalizer) {
		return ctrl.Result{}, nil
	}

	// Use the authoritative AppID so a delete that races a just-completed
	// create still removes the Slack app rather than orphaning it.
	appID, err := r.currentAppID(ctx, client.ObjectKeyFromObject(app), app.Status.AppID)
	if err != nil {
		return ctrl.Result{}, err
	}
	if appID != "" {
		token, err := r.Tokens.AccessToken(ctx)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("cannot delete Slack app without a token: %w", err)
		}
		if err := r.Slack.DeleteApp(ctx, token, appID); err != nil {
			return ctrl.Result{}, fmt.Errorf("delete Slack app %s: %w", appID, err)
		}
		l.Info("deleted Slack app", "appID", appID)
	}

	controllerutil.RemoveFinalizer(app, finalizer)
	return ctrl.Result{}, r.Update(ctx, app)
}

// iconOutcome carries the result of a best-effort icon apply so it can be folded
// into the status update transaction. A nil outcome means "no change".
type iconOutcome struct {
	hash string
	err  error
}

func (o *iconOutcome) apply(a *slackv1alpha1.SlackApp) {
	if o == nil {
		return
	}
	if o.err != nil {
		meta.SetStatusCondition(&a.Status.Conditions, metav1.Condition{
			Type:               condIconApplied,
			Status:             metav1.ConditionFalse,
			Reason:             "IconFailed",
			Message:            o.err.Error(),
			ObservedGeneration: a.Generation,
		})
		return
	}
	a.Status.IconHash = o.hash
	meta.SetStatusCondition(&a.Status.Conditions, metav1.Condition{
		Type:               condIconApplied,
		Status:             metav1.ConditionTrue,
		Reason:             "Applied",
		Message:            "Icon uploaded via apps.icon.set",
		ObservedGeneration: a.Generation,
	})
}

// reconcileIcon applies the icon on a best-effort basis (Slack's icon endpoint
// is unofficial). It returns nil when there is nothing to do.
func (r *SlackAppReconciler) reconcileIcon(ctx context.Context, appID, iconURL, currentHash, token string) *iconOutcome {
	if iconURL == "" {
		return nil
	}
	h := hashString(iconURL)
	if h == currentHash {
		return nil
	}

	filename, data, err := fetchImage(ctx, iconURL)
	if err == nil {
		err = r.Slack.SetIcon(ctx, token, appID, filename, bytes.NewReader(data))
	}
	if err != nil {
		log.FromContext(ctx).Error(err, "best-effort icon apply failed", "iconURL", iconURL)
	}
	return &iconOutcome{hash: h, err: err}
}

func (r *SlackAppReconciler) fail(ctx context.Context, key types.NamespacedName, reason string, cause error) (ctrl.Result, error) {
	if err := r.patchStatus(ctx, key, func(a *slackv1alpha1.SlackApp) {
		meta.SetStatusCondition(&a.Status.Conditions, metav1.Condition{
			Type:               condReady,
			Status:             metav1.ConditionFalse,
			Reason:             reason,
			Message:            cause.Error(),
			ObservedGeneration: a.Generation,
		})
	}); err != nil {
		log.FromContext(ctx).Error(err, "failed to update status after error")
	}
	return ctrl.Result{}, cause
}

// patchStatus applies mutate to a freshly-read copy of the object and writes the
// status, retrying on optimistic-lock conflicts. The read is uncached so each
// attempt starts from the latest resourceVersion.
func (r *SlackAppReconciler) patchStatus(ctx context.Context, key types.NamespacedName, mutate func(*slackv1alpha1.SlackApp)) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var cur slackv1alpha1.SlackApp
		if err := r.APIReader.Get(ctx, key, &cur); err != nil {
			return err
		}
		mutate(&cur)
		return r.Status().Update(ctx, &cur)
	})
}

func (r *SlackAppReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.APIReader = mgr.GetAPIReader()
	return ctrl.NewControllerManagedBy(mgr).
		For(&slackv1alpha1.SlackApp{}).
		Complete(r)
}

// canonicalManifest re-marshals the manifest to deterministic JSON (sorted keys)
// so the hash is stable and the string sent to Slack is well-formed.
func canonicalManifest(raw []byte) (string, string, error) {
	if len(raw) == 0 {
		return "", "", fmt.Errorf("spec.manifest is empty")
	}
	var obj any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return "", "", fmt.Errorf("spec.manifest is not valid JSON/YAML object: %w", err)
	}
	b, err := json.Marshal(obj)
	if err != nil {
		return "", "", err
	}
	return string(b), hashString(string(b)), nil
}

func hashString(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func fetchImage(ctx context.Context, rawURL string) (string, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return "", nil, err
	}
	req.Header.Set("User-Agent", iconUserAgent)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", nil, fmt.Errorf("fetch icon: status %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxIconBytes+1))
	if err != nil {
		return "", nil, err
	}
	if len(data) > maxIconBytes {
		return "", nil, fmt.Errorf("icon exceeds %d bytes", maxIconBytes)
	}

	filename := path.Base(resp.Request.URL.Path)
	if ext := path.Ext(filename); ext == "" {
		switch resp.Header.Get("Content-Type") {
		case "image/png":
			filename = "icon.png"
		case "image/jpeg":
			filename = "icon.jpg"
		default:
			filename = "icon"
		}
	}
	return filename, data, nil
}
