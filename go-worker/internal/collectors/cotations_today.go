package collectors

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// CotationsTodayCollector collects today's cotations
type CotationsTodayCollector struct {
	client *http.Client
}

// NewCotationsTodayCollector creates a new cotations today collector
func NewCotationsTodayCollector() *CotationsTodayCollector {
	return &CotationsTodayCollector{
		client: &http.Client{
			Timeout: 45 * time.Second,
		},
	}
}

// Name returns the collector name
func (c *CotationsTodayCollector) Name() string {
	return "cotations_today"
}

// CotationEntry represents a single cotation entry
type CotationEntry struct {
	Price float64 `json:"price"`
	Date  string  `json:"date"`
}

// CotationsResponse represents cotations data
type CotationsResponse struct {
	Real  []CotationEntry `json:"real"`
	Dolar []CotationEntry `json:"dolar"`
	Euro  []CotationEntry `json:"euro"`
}

// Collect fetches today's cotations from the API
func (c *CotationsTodayCollector) Collect(ctx context.Context, req CollectRequest) (*CollectResult, error) {
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
