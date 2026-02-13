package httpclient

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/luizfelipeneves/api-fundo/go-worker/internal/config"
)

// FnetClient handles FNET requests with session management
type FnetClient struct {
	httpClient *http.Client
	cfg        *config.Config
}

// NewFnetClient creates a new FNET client
func NewFnetClient(cfg *config.Config) *FnetClient {
	return &FnetClient{
		httpClient: &http.Client{
			Timeout: time.Duration(cfg.HTTPTimeoutMS) * time.Millisecond,
		},
		cfg: cfg,
	}
}

// FetchWithSession performs a two-phase FNET request with session management
func (c *FnetClient) FetchWithSession(ctx context.Context, initURL, dataURL string, result interface{}) error {
	var lastErr error

	for attempt := 0; attempt < c.cfg.HTTPRetryMax; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(c.cfg.HTTPRetryDelayMS) * time.Millisecond)
		}

		// Phase 1: Initialize session and get cookies (JSESSIONID + others)
		cookies, err := c.initSession(ctx, initURL)
		if err != nil {
			lastErr = err
			continue
		}

		if extractJSessionIDFromCookies(cookies) == "" {
			lastErr = fmt.Errorf("FNET_INIT_NO_JSESSIONID")
			continue
		}

		// Phase 2: Fetch data with session cookie
		err = c.fetchData(ctx, dataURL, initURL, cookies, result)
		if err != nil {
			// Retry on 401/403 (session expired)
			if isAuthError(err) && attempt+1 < c.cfg.HTTPRetryMax {
				lastErr = err
				continue
			}
			return err
		}

		return nil
	}

	if lastErr != nil {
		return fmt.Errorf("FNET request failed after %d attempts: %w", c.cfg.HTTPRetryMax, lastErr)
	}

	return fmt.Errorf("FNET request failed after %d attempts", c.cfg.HTTPRetryMax)
}

// initSession initializes FNET session and returns response cookies
func (c *FnetClient) initSession(ctx context.Context, initURL string) ([]*http.Cookie, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", initURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create init request: %w", err)
	}

	c.setFnetInitHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute init request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("init request failed with status: %d", resp.StatusCode)
	}

	return resp.Cookies(), nil
}

// fetchData fetches data from FNET with session cookie
func (c *FnetClient) fetchData(ctx context.Context, dataURL, refererURL string, cookies []*http.Cookie, result interface{}) error {
	req, err := http.NewRequestWithContext(ctx, "GET", dataURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create data request: %w", err)
	}

	c.setFnetDataHeaders(req, cookies)
	if strings.TrimSpace(refererURL) != "" {
		req.Header.Set("Referer", refererURL)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute data request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return &authError{status: resp.StatusCode}
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("data request failed with status: %d", resp.StatusCode)
	}

	// Check content type
	contentType := resp.Header.Get("Content-Type")
	if contentType != "" && !contains(contentType, "application/json") {
		// If HTML, might need to retry (session issue)
		if contains(contentType, "text/html") {
			return &authError{status: 403}
		}
		return fmt.Errorf("unexpected content type: %s", contentType)
	}

	// Decode JSON response
	if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
		return fmt.Errorf("failed to decode JSON: %w", err)
	}

	return nil
}

// setFnetInitHeaders sets headers for FNET init request
func (c *FnetClient) setFnetInitHeaders(req *http.Request) {
	req.Header.Set("User-Agent", "Mozilla/5.0")
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Connection", "keep-alive")
}

// setFnetDataHeaders sets headers for FNET data request
func (c *FnetClient) setFnetDataHeaders(req *http.Request, cookies []*http.Cookie) {
	req.Header.Set("User-Agent", "Mozilla/5.0")
	req.Header.Set("Accept", "application/json, text/javascript, */*; q=0.01")
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	req.Header.Set("Connection", "keep-alive")
	for _, cookie := range cookies {
		if cookie == nil || cookie.Name == "" {
			continue
		}
		value := strings.TrimSpace(cookie.Value)
		if value == "" {
			continue
		}
		req.AddCookie(&http.Cookie{Name: cookie.Name, Value: value})
	}
}

// extractJSessionID extracts JSESSIONID from response headers
func extractJSessionIDFromCookies(cookies []*http.Cookie) string {
	for _, cookie := range cookies {
		if cookie.Name == "JSESSIONID" {
			return strings.TrimSpace(cookie.Value)
		}
	}
	return ""
}

// authError represents an authentication error
type authError struct {
	status int
}

func (e *authError) Error() string {
	return fmt.Sprintf("auth error: status %d", e.status)
}

// isAuthError checks if error is an authentication error
func isAuthError(err error) bool {
	_, ok := err.(*authError)
	return ok
}

// contains checks if a string contains a substring (case-insensitive)
func contains(s, substr string) bool {
	return regexp.MustCompile(`(?i)` + regexp.QuoteMeta(substr)).MatchString(s)
}
