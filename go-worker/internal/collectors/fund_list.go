package collectors

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// FundListCollector collects the list of all funds
type FundListCollector struct {
	client *http.Client
}

// NewFundListCollector creates a new fund list collector
func NewFundListCollector() *FundListCollector {
	return &FundListCollector{
		client: &http.Client{
			Timeout: 45 * time.Second,
		},
	}
}

// Name returns the collector name
func (c *FundListCollector) Name() string {
	return "fund_list"
}

// FundListItem represents a single fund in the list
type FundListItem struct {
	Code                    string  `json:"code"`
	Sector                  string  `json:"sector"`
	PVP                     float64 `json:"p_vp"`
	DividendYield           float64 `json:"dividend_yield"`
	DividendYieldLast5Years float64 `json:"dividend_yield_last_5_years"`
	DailyLiquidity          float64 `json:"daily_liquidity"`
	NetWorth                float64 `json:"net_worth"`
	Type                    string  `json:"type"`
}

// FundListResponse represents the API response
type FundListResponse struct {
	Total int            `json:"total"`
	Data  []FundListItem `json:"data"`
}

// Collect fetches the fund list from the API
func (c *FundListCollector) Collect(ctx context.Context, req CollectRequest) (*CollectResult, error) {
	url := "https://investidor10.com.br/api/fundos-imobiliarios/"

	httpReq, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch fund list: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(body))
	}

	var result FundListResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &CollectResult{
		FundCode:  "", // Fund list doesn't have a single fund code
		Data:      result.Data,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}, nil
}
