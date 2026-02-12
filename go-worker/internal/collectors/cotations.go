package collectors

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// CotationsCollector collects historical cotations
type CotationsCollector struct {
	client *http.Client
}

// NewCotationsCollector creates a new historical cotations collector
func NewCotationsCollector() *CotationsCollector {
	return &CotationsCollector{
		client: &http.Client{
			Timeout: 45 * time.Second,
		},
	}
}

// Name returns the collector name
func (c *CotationsCollector) Name() string {
	return "cotations"
}

// Collect fetches historical cotations from the API
// Note: This uses the same endpoint as cotations_today but may have different processing
func (c *CotationsCollector) Collect(ctx context.Context, req CollectRequest) (*CollectResult, error) {
	if req.FundCode == "" {
		return nil, fmt.Errorf("fund_code is required")
	}

	url := fmt.Sprintf("https://investidor10.com.br/api/fundos-imobiliarios/%s/cotations", req.FundCode)

	httpReq, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch cotations: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(body))
	}

	var cotations CotationsResponse
	if err := json.NewDecoder(resp.Body).Decode(&cotations); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &CollectResult{
		FundCode:  req.FundCode,
		Data:      cotations,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}, nil
}
