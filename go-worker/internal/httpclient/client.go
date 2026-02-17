package httpclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/luizfelipeneves/api-fundo/go-worker/internal/config"
)

const (
	BaseURL          = "https://investidor10.com.br"
	StatusInvestBase = "https://statusinvest.com.br"
	FnetBase         = "https://fnet.bmfbovespa.com.br/fnet/publico"
	DefaultUserAgent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"
)

// Client is an HTTP client with cookie jar, CSRF token, and retry logic
type Client struct {
	httpClient *http.Client
	cfg        *config.Config
}

// New creates a new HTTP client with cookie jar
func New(cfg *config.Config) (*Client, error) {
	jar, err := cookiejar.New(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create cookie jar: %w", err)
	}

	return &Client{
		httpClient: &http.Client{
			Jar:     jar,
			Timeout: time.Duration(cfg.HTTPTimeoutMS) * time.Millisecond,
		},
		cfg: cfg,
	}, nil
}

// GetJSON performs a GET request and decodes JSON response
func (c *Client) GetJSON(ctx context.Context, url string, result interface{}) error {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.doWithRetry(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
		return fmt.Errorf("failed to decode JSON: %w", err)
	}

	return nil
}

// PostForm performs a POST request with form data and decodes JSON response
func (c *Client) PostForm(ctx context.Context, url string, formData string, result interface{}) error {
	csrfToken, cookieHeader, err := c.GetInvestidor10Session(ctx)
	if err != nil {
		return err
	}
	return c.PostFormInvestidor10(ctx, url, formData, csrfToken, cookieHeader, result)
}

func (c *Client) GetInvestidor10Session(ctx context.Context) (csrfToken string, cookieHeader string, err error) {
	html, err := c.GetHTML(ctx, BaseURL+"/fiis/busca-avancada/")
	if err != nil {
		return "", "", err
	}

	csrfToken = extractCSRFTokenFromHTML(html)
	if strings.TrimSpace(csrfToken) == "" {
		return "", "", fmt.Errorf("csrf-token not found in investidor10 HTML")
	}

	cookieHeader = c.investidor10CookieHeader()
	if strings.TrimSpace(cookieHeader) == "" {
		return "", "", fmt.Errorf("investidor10 cookies not captured")
	}

	return csrfToken, cookieHeader, nil
}

func (c *Client) PostFormInvestidor10(ctx context.Context, url string, formData string, csrfToken string, cookieHeader string, result interface{}) error {
	req, err := http.NewRequestWithContext(ctx, "POST", url, strings.NewReader(formData))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	c.setInvestidor10HeadersDynamic(req, csrfToken, cookieHeader)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")

	resp, err := c.doWithRetry(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
		return fmt.Errorf("failed to decode JSON: %w", err)
	}

	return nil
}

// GetHTML performs a GET request and returns HTML as string
func (c *Client) GetHTML(ctx context.Context, url string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	c.setHTMLHeaders(req)

	resp, err := c.doWithRetry(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusGone {
		return "", fmt.Errorf("FII_NOT_FOUND")
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response body: %w", err)
	}

	return string(body), nil
}

// PostFormStatusInvest performs a POST to statusinvest.com.br
func (c *Client) PostFormStatusInvest(ctx context.Context, url string, formData string, result interface{}) error {
	req, err := http.NewRequestWithContext(ctx, "POST", url, strings.NewReader(formData))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	c.setStatusInvestHeaders(req)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")

	resp, err := c.doWithRetry(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
		return fmt.Errorf("failed to decode JSON: %w", err)
	}

	return nil
}

// setInvestidor10Headers sets headers for investidor10.com.br requests
func (c *Client) GetJSONStatusInvest(ctx context.Context, url string, result interface{}) error {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	c.setStatusInvestHeaders(req)

	resp, err := c.doWithRetry(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
		return fmt.Errorf("failed to decode JSON: %w", err)
	}

	return nil
}

func (c *Client) setInvestidor10HeadersDynamic(req *http.Request, csrfToken string, cookieHeader string) {
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("Accept-Language", "pt-BR,pt;q=0.9")
	req.Header.Set("Origin", BaseURL)
	req.Header.Set("Referer", BaseURL+"/fiis/busca-avancada/")
	req.Header.Set("User-Agent", DefaultUserAgent)
	if strings.TrimSpace(csrfToken) != "" {
		req.Header.Set("X-CSRF-TOKEN", csrfToken)
	}
	if v := extractXSRFTokenFromCookieHeader(cookieHeader); strings.TrimSpace(v) != "" {
		req.Header.Set("X-XSRF-TOKEN", v)
	}
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	if strings.TrimSpace(cookieHeader) != "" {
		req.Header.Set("Cookie", cookieHeader)
	}
}

// setHTMLHeaders sets headers for HTML requests
func (c *Client) setHTMLHeaders(req *http.Request) {
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	req.Header.Set("Accept-Language", "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7")
	req.Header.Set("User-Agent", DefaultUserAgent)
	req.Header.Set("Connection", "keep-alive")
}

// setStatusInvestHeaders sets headers for statusinvest.com.br requests
func (c *Client) setStatusInvestHeaders(req *http.Request) {
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Language", "pt-BR,pt;q=0.8")
	req.Header.Set("Origin", StatusInvestBase)
	req.Header.Set("Referer", StatusInvestBase+"/fundos-imobiliarios/")
	req.Header.Set("User-Agent", DefaultUserAgent)
}

// doWithRetry performs the request with retry logic
func (c *Client) doWithRetry(req *http.Request) (*http.Response, error) {
	var lastErr error

	for attempt := 0; attempt < c.cfg.HTTPRetryMax; attempt++ {
		if attempt > 0 {
			// Sleep before retry
			sleepDuration := time.Duration(c.cfg.HTTPRetryDelayMS) * time.Millisecond
			time.Sleep(sleepDuration)
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = err
			continue
		}

		// Check if we should retry based on status code
		if c.isRetryableStatus(resp.StatusCode) && attempt+1 < c.cfg.HTTPRetryMax {
			resp.Body.Close()

			// Check for Retry-After header
			if retryAfter := resp.Header.Get("Retry-After"); retryAfter != "" {
				if seconds, err := strconv.Atoi(retryAfter); err == nil {
					time.Sleep(time.Duration(seconds) * time.Second)
				}
			}

			lastErr = fmt.Errorf("retryable status code: %d", resp.StatusCode)
			continue
		}

		return resp, nil
	}

	if lastErr != nil {
		return nil, fmt.Errorf("request failed after %d attempts: %w", c.cfg.HTTPRetryMax, lastErr)
	}

	return nil, fmt.Errorf("request failed after %d attempts", c.cfg.HTTPRetryMax)
}

var csrfMetaTokenRe = regexp.MustCompile(`(?i)<meta[^>]*\bname=["']csrf-token["'][^>]*\bcontent=["']([^"']+)["'][^>]*>`)
var csrfMetaTokenReAlt = regexp.MustCompile(`(?i)<meta[^>]*\bcontent=["']([^"']+)["'][^>]*\bname=["']csrf-token["'][^>]*>`)

func extractCSRFTokenFromHTML(html string) string {
	if strings.TrimSpace(html) == "" {
		return ""
	}
	if m := csrfMetaTokenRe.FindStringSubmatch(html); len(m) > 1 {
		return strings.TrimSpace(m[1])
	}
	if m := csrfMetaTokenReAlt.FindStringSubmatch(html); len(m) > 1 {
		return strings.TrimSpace(m[1])
	}
	return ""
}

func (c *Client) investidor10CookieHeader() string {
	u, err := url.Parse(BaseURL)
	if err != nil || u == nil {
		return ""
	}

	if c.httpClient == nil || c.httpClient.Jar == nil {
		return ""
	}

	merged := map[string]string{}
	for _, ck := range c.httpClient.Jar.Cookies(u) {
		if ck == nil || strings.TrimSpace(ck.Name) == "" {
			continue
		}
		value := strings.TrimSpace(ck.Value)
		if value == "" {
			continue
		}
		merged[ck.Name] = value
	}

	return buildCookieHeader(merged)
}

func parseCookieHeaderToMap(raw string) map[string]string {
	out := map[string]string{}
	for _, part := range strings.Split(raw, ";") {
		p := strings.TrimSpace(part)
		if p == "" {
			continue
		}
		kv := strings.SplitN(p, "=", 2)
		if len(kv) != 2 {
			continue
		}
		name := strings.TrimSpace(kv[0])
		if name == "" {
			continue
		}
		out[name] = strings.TrimSpace(kv[1])
	}
	return out
}

func extractXSRFTokenFromCookieHeader(cookieHeader string) string {
	cookies := parseCookieHeaderToMap(cookieHeader)
	raw := strings.TrimSpace(cookies["XSRF-TOKEN"])
	if raw == "" {
		return ""
	}
	decoded, err := url.QueryUnescape(raw)
	if err != nil {
		return raw
	}
	return strings.TrimSpace(decoded)
}

func buildCookieHeader(m map[string]string) string {
	if len(m) == 0 {
		return ""
	}

	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+m[k])
	}
	return strings.Join(parts, "; ")
}

// isRetryableStatus checks if a status code is retryable
func (c *Client) isRetryableStatus(status int) bool {
	return status == 429 || status == 520 || status >= 500
}

// CalculateExponentialBackoff calculates exponential backoff duration
func CalculateExponentialBackoff(attempt int, baseDelayMS int) time.Duration {
	delay := float64(baseDelayMS) * math.Pow(2, float64(attempt))
	return time.Duration(delay) * time.Millisecond
}
