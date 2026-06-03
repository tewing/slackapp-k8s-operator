// Package slack is a thin client over the Slack App Management API methods the
// operator needs: config-token rotation and manifest CRUD.
package slack

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const defaultBaseURL = "https://slack.com/api"

// Client talks to the Slack Web API. It is stateless with respect to tokens —
// callers pass the access token per call so the operator can manage rotation.
type Client struct {
	BaseURL string
	HTTP    *http.Client
}

// New returns a Client with sane defaults.
func New() *Client {
	return &Client{
		BaseURL: defaultBaseURL,
		HTTP:    &http.Client{Timeout: 30 * time.Second},
	}
}

// apiError is the shape of a Slack error response ({"ok":false,"error":"..."}).
type apiError struct {
	OK      bool   `json:"ok"`
	Error   string `json:"error"`
	Errors  any    `json:"errors,omitempty"`
	Warning string `json:"warning,omitempty"`
}

// TokenSet is the result of a config-token rotation.
type TokenSet struct {
	AccessToken  string
	RefreshToken string
	// ExpiresAt is when the access token stops working.
	ExpiresAt time.Time
}

type rotateResponse struct {
	apiError
	Token   string `json:"token"`
	Refresh string `json:"refresh_token"`
	// ExpAt is a unix timestamp (seconds).
	ExpAt int64 `json:"exp"`
}

// RotateToken exchanges a refresh token for a fresh access+refresh pair via
// tooling.tokens.rotate. The previous refresh token is invalidated by Slack on
// success, so callers MUST persist the returned pair.
func (c *Client) RotateToken(ctx context.Context, refreshToken string) (*TokenSet, error) {
	form := url.Values{}
	form.Set("refresh_token", refreshToken)

	var out rotateResponse
	if err := c.postForm(ctx, "tooling.tokens.rotate", "", form, &out); err != nil {
		return nil, err
	}
	if !out.OK {
		return nil, fmt.Errorf("tooling.tokens.rotate failed: %s", out.Error)
	}
	return &TokenSet{
		AccessToken:  out.Token,
		RefreshToken: out.Refresh,
		ExpiresAt:    time.Unix(out.ExpAt, 0),
	}, nil
}

type manifestCreateResponse struct {
	apiError
	AppID string `json:"app_id"`
}

// CreateApp creates a new Slack app from a manifest (JSON-encoded string) and
// returns the assigned app ID.
func (c *Client) CreateApp(ctx context.Context, accessToken string, manifestJSON string) (string, error) {
	form := url.Values{}
	form.Set("manifest", manifestJSON)

	var out manifestCreateResponse
	if err := c.postForm(ctx, "apps.manifest.create", accessToken, form, &out); err != nil {
		return "", err
	}
	if !out.OK {
		return "", fmt.Errorf("apps.manifest.create failed: %s", describe(out.apiError))
	}
	return out.AppID, nil
}

// UpdateApp replaces an existing app's configuration with the given manifest.
// apps.manifest.update is a full replace — the manifest must be complete.
func (c *Client) UpdateApp(ctx context.Context, accessToken, appID, manifestJSON string) error {
	form := url.Values{}
	form.Set("app_id", appID)
	form.Set("manifest", manifestJSON)

	var out apiError
	if err := c.postForm(ctx, "apps.manifest.update", accessToken, form, &out); err != nil {
		return err
	}
	if !out.OK {
		return fmt.Errorf("apps.manifest.update failed: %s", describe(out))
	}
	return nil
}

// DeleteApp permanently deletes a Slack app.
func (c *Client) DeleteApp(ctx context.Context, accessToken, appID string) error {
	form := url.Values{}
	form.Set("app_id", appID)

	var out apiError
	if err := c.postForm(ctx, "apps.manifest.delete", accessToken, form, &out); err != nil {
		return err
	}
	if !out.OK {
		// app_not_found means it's already gone — treat as success so the
		// finalizer can be removed.
		if out.Error == "app_not_found" {
			return nil
		}
		return fmt.Errorf("apps.manifest.delete failed: %s", out.Error)
	}
	return nil
}

// postForm POSTs application/x-www-form-urlencoded and decodes the JSON result
// into out. If accessToken is non-empty it is sent as a Bearer token.
func (c *Client) postForm(ctx context.Context, method, accessToken string, form url.Values, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/"+method, strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if accessToken != "" {
		req.Header.Set("Authorization", "Bearer "+accessToken)
	}

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return err
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("%s: decode response (status %d): %w", method, resp.StatusCode, err)
	}
	return nil
}

// describe renders a Slack error including the detailed errors array when the
// manifest fails validation, which is the common failure mode.
func describe(e apiError) string {
	if e.Errors != nil {
		if b, err := json.Marshal(e.Errors); err == nil {
			return fmt.Sprintf("%s: %s", e.Error, string(b))
		}
	}
	return e.Error
}
