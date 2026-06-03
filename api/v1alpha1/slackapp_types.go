package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// SlackAppSpec defines the desired state of a Slack app.
type SlackAppSpec struct {
	// Manifest is the Slack app manifest, written inline as a YAML/JSON object.
	// It is sent verbatim to Slack's apps.manifest.create/update API. The schema
	// is intentionally open — see https://docs.slack.dev/reference/app-manifest.
	//
	// +kubebuilder:validation:Required
	// +kubebuilder:pruning:PreserveUnknownFields
	// +kubebuilder:validation:Schemaless
	// +kubebuilder:validation:Type=object
	Manifest runtime.RawExtension `json:"manifest"`

	// IconURL is an optional HTTP(S) URL to a square PNG/JPG used as the app's
	// icon. Slack's public manifest API does not support icons, so the operator
	// applies it on a best-effort basis via the internal apps.icon.set endpoint;
	// failures are surfaced on status but do not fail the whole reconcile.
	//
	// +optional
	// +kubebuilder:validation:Pattern=`^https?://.+`
	IconURL string `json:"iconURL,omitempty"`
}

// SlackAppStatus defines the observed state of a Slack app.
type SlackAppStatus struct {
	// AppID is the Slack-assigned application ID (e.g. A0123456789). Its presence
	// is what distinguishes a create from an update on subsequent reconciles.
	// +optional
	AppID string `json:"appID,omitempty"`

	// ManifestHash is the SHA-256 of the last manifest successfully pushed to
	// Slack. Used to skip no-op updates.
	// +optional
	ManifestHash string `json:"manifestHash,omitempty"`

	// IconHash is the SHA-256 of the icon URL last successfully applied.
	// +optional
	IconHash string `json:"iconHash,omitempty"`

	// ObservedGeneration is the .metadata.generation last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions represent the latest available observations of the app's state.
	// Types: Ready, IconApplied.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=slackapp
// +kubebuilder:printcolumn:name="App ID",type=string,JSONPath=`.status.appID`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// SlackApp is the Schema for the slackapps API.
type SlackApp struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SlackAppSpec   `json:"spec,omitempty"`
	Status SlackAppStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// SlackAppList contains a list of SlackApp.
type SlackAppList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SlackApp `json:"items"`
}

func init() {
	SchemeBuilder.Register(&SlackApp{}, &SlackAppList{})
}
