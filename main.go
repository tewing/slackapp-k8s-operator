package main

import (
	"flag"
	"os"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	slackv1alpha1 "github.com/tewing/slackapp-k8s-operator/api/v1alpha1"
	"github.com/tewing/slackapp-k8s-operator/internal/controller"
	"github.com/tewing/slackapp-k8s-operator/internal/slack"
)

var setupLog = ctrl.Log.WithName("setup")

func main() {
	var metricsAddr, probeAddr string
	var operatorNamespace, workspaceConfigName string
	var enableLeaderElection bool

	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "Address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "Address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false, "Enable leader election for controller manager.")
	flag.StringVar(&operatorNamespace, "namespace", "", "Namespace holding the per-workspace token Secrets and the workspace ConfigMap (defaults to the pod namespace).")
	flag.StringVar(&workspaceConfigName, "workspace-config-name", "slack-operator-workspaces", "Name of the ConfigMap mapping workspace labels to token Secret names.")

	opts := zap.Options{Development: false}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	scheme := clientgoscheme.Scheme
	utilruntime.Must(slackv1alpha1.AddToScheme(scheme))

	if operatorNamespace == "" {
		operatorNamespace = podNamespace()
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "slack-operator.slack.te-labs.org",
		// SlackApps are watched cluster-wide, but the only Secrets/ConfigMaps the
		// operator reads are the token Secrets and workspace ConfigMap in its own
		// namespace — scope those informers so it doesn't need (and isn't granted)
		// cluster-wide access.
		Cache: cache.Options{
			ByObject: map[client.Object]cache.ByObject{
				&corev1.Secret{}: {
					Namespaces: map[string]cache.Config{operatorNamespace: {}},
				},
				&corev1.ConfigMap{}: {
					Namespaces: map[string]cache.Config{operatorNamespace: {}},
				},
			},
		},
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	slackClient := slack.New()
	tokens := controller.NewTokenManager(mgr.GetClient(), slackClient, operatorNamespace)
	workspaces := controller.NewWorkspaceResolver(mgr.GetClient(), types.NamespacedName{
		Name:      workspaceConfigName,
		Namespace: operatorNamespace,
	})

	if err := (&controller.SlackAppReconciler{
		Client:     mgr.GetClient(),
		Slack:      slackClient,
		Tokens:     tokens,
		Workspaces: workspaces,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "SlackApp")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager", "namespace", operatorNamespace, "workspaceConfig", workspaceConfigName)
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}

func podNamespace() string {
	if ns, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err == nil {
		if s := string(ns); s != "" {
			return s
		}
	}
	return "slack-operator"
}
