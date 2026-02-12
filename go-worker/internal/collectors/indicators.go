package collectors

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// IndicatorsCollector collects fund indicators
type IndicatorsCollector struct {
	client *http.Client
}

// NewIndicatorsCollector creates a new indicators collector
func NewIndicatorsCollector() *IndicatorsCollector {
	return &IndicatorsCollector{
		client: &http.Client{
			Timeout: 45 * time.Second,
		},
	}
}

// Name returns the collector name
func (c *IndicatorsCollector) Name() string {
	return "indicators"
}

// IndicatorValue represents a single indicator value
type IndicatorValue struct {
	Year  string   `json:"year"`
	Value *float64 `json:"value"` // Nullable
}

// Indicators represents fund indicators
type Indicators struct {
	PVP           []IndicatorValue `json:"p_vp"`
	DividendYield []IndicatorValue `json:"dividend_yield"`
}

// Collect fetches fund indicators from the API
func (c *IndicatorsCollector) Collect(ctx context.Context, req CollectRequest) (*CollectResult, error) {
	if req.FundCode == "" {
		return nil, fmt.Errorf("fund_code is required")
	}

	url := fmt.Sprintf("https://investidor10.com.br/api/fundos-imobiliarios/%s/indicators", req.FundCode)

	httpReq, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch indicators: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(body))
	}

	var indicators Indicators
	if err := json.NewDecoder(resp.Body).Decode(&indicators); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &CollectResult{
		FundCode:  req.FundCode,
		Data:      indicators,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}, nil
}
