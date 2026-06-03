package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
)

// SlackAppReconciler reconciles a SlackApp object against the Slack API.
type SlackAppReconciler struct {
	client.Client
	Slack  *slack.Client
	Tokens *TokenStore
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
		return r.fail(ctx, &app, "InvalidManifest", err)
	}

	token, err := r.Tokens.AccessToken(ctx)
	if err != nil {
		return r.fail(ctx, &app, "TokenUnavailable", err)
	}

	if app.Status.AppID == "" {
		appID, err := r.Slack.CreateApp(ctx, token, manifestJSON)
		if err != nil {
			return r.fail(ctx, &app, "CreateFailed", err)
		}
		l.Info("created Slack app", "appID", appID)
		app.Status.AppID = appID
		app.Status.ManifestHash = manifestHash
	} else if app.Status.ManifestHash != manifestHash {
		if err := r.Slack.UpdateApp(ctx, token, app.Status.AppID, manifestJSON); err != nil {
			return r.fail(ctx, &app, "UpdateFailed", err)
		}
		l.Info("updated Slack app", "appID", app.Status.AppID)
		app.Status.ManifestHash = manifestHash
	}

	r.reconcileIcon(ctx, &app, token)

	app.Status.ObservedGeneration = app.Generation
	meta.SetStatusCondition(&app.Status.Conditions, metav1.Condition{
		Type:               condReady,
		Status:             metav1.ConditionTrue,
		Reason:             "Synced",
		Message:            fmt.Sprintf("Slack app %s is in sync", app.Status.AppID),
		ObservedGeneration: app.Generation,
	})
	if err := r.Status().Update(ctx, &app); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: resyncPeriod}, nil
}

func (r *SlackAppReconciler) reconcileDelete(ctx context.Context, app *slackv1alpha1.SlackApp) (ctrl.Result, error) {
	l := log.FromContext(ctx)
	if !controllerutil.ContainsFinalizer(app, finalizer) {
		return ctrl.Result{}, nil
	}

	if app.Status.AppID != "" {
		token, err := r.Tokens.AccessToken(ctx)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("cannot delete Slack app without a token: %w", err)
		}
		if err := r.Slack.DeleteApp(ctx, token, app.Status.AppID); err != nil {
			return ctrl.Result{}, fmt.Errorf("delete Slack app %s: %w", app.Status.AppID, err)
		}
		l.Info("deleted Slack app", "appID", app.Status.AppID)
	}

	controllerutil.RemoveFinalizer(app, finalizer)
	return ctrl.Result{}, r.Update(ctx, app)
}

// reconcileIcon applies the icon on a best-effort basis. Failures are recorded
// on the IconApplied condition but never fail the reconcile, because Slack's
// icon endpoint is unofficial.
func (r *SlackAppReconciler) reconcileIcon(ctx context.Context, app *slackv1alpha1.SlackApp, token string) {
	if app.Spec.IconURL == "" {
		return
	}
	iconHash := hashString(app.Spec.IconURL)
	if iconHash == app.Status.IconHash {
		return
	}

	filename, data, err := fetchImage(ctx, app.Spec.IconURL)
	if err == nil {
		err = r.Slack.SetIcon(ctx, token, app.Status.AppID, filename, strings.NewReader(string(data)))
	}
	if err != nil {
		meta.SetStatusCondition(&app.Status.Conditions, metav1.Condition{
			Type:               condIconApplied,
			Status:             metav1.ConditionFalse,
			Reason:             "IconFailed",
			Message:            err.Error(),
			ObservedGeneration: app.Generation,
		})
		log.FromContext(ctx).Error(err, "best-effort icon apply failed", "iconURL", app.Spec.IconURL)
		return
	}
	app.Status.IconHash = iconHash
	meta.SetStatusCondition(&app.Status.Conditions, metav1.Condition{
		Type:               condIconApplied,
		Status:             metav1.ConditionTrue,
		Reason:             "Applied",
		Message:            "Icon uploaded via apps.icon.set",
		ObservedGeneration: app.Generation,
	})
}

func (r *SlackAppReconciler) fail(ctx context.Context, app *slackv1alpha1.SlackApp, reason string, cause error) (ctrl.Result, error) {
	meta.SetStatusCondition(&app.Status.Conditions, metav1.Condition{
		Type:               condReady,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            cause.Error(),
		ObservedGeneration: app.Generation,
	})
	if err := r.Status().Update(ctx, app); err != nil && !apierrors.IsConflict(err) {
		log.FromContext(ctx).Error(err, "failed to update status after error")
	}
	return ctrl.Result{}, cause
}

func (r *SlackAppReconciler) SetupWithManager(mgr ctrl.Manager) error {
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
