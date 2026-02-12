package collectors

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"time"

	"github.com/luizfelipeneves/api-fundo/go-worker/internal/httpclient"
	"github.com/luizfelipeneves/api-fundo/go-worker/internal/parsers"
)

// FundListCollector collects the list of all funds
type FundListCollector struct {
	client *httpclient.Client
}

// NewFundListCollector creates a new fund list collector
func NewFundListCollector(client *httpclient.Client) *FundListCollector {
	return &FundListCollector{client: client}
}

// Name returns the collector name
func (c *FundListCollector) Name() string {
	return "fund_list"
}

// Collect fetches the fund list
func (c *FundListCollector) Collect(ctx context.Context, req CollectRequest) (*CollectResult, error) {
	log.Println("[fund_list] collecting fund list")

	// Build form data for advanced search
	formData := buildFundListParams()

	// POST to investidor10 API
	var response FundListResponse
	err := c.client.PostForm(ctx, httpclient.BaseURL+"/api/fii/advanced-search", formData, &response)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch fund list: %w", err)
	}

	// Normalize fund codes
	var funds []FundListItem
	for _, item := range response.Data {
		funds = append(funds, FundListItem{
			Code:                    parsers.NormalizeFundCode(item.Name),
			Sector:                  item.Sector,
			PVP:                     item.PVP,
			DividendYield:           item.DividendYield,
			DividendYieldLast5Years: item.DividendYieldLast5Years,
			DailyLiquidity:          item.DailyLiquidity,
			NetWorth:                item.NetWorth,
			Type:                    item.Type,
		})
	}

	return &CollectResult{
		Data:      funds,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}, nil
}

// FundListResponse represents the API response
type FundListResponse struct {
	Total int               `json:"total"`
	Data  []FundListAPIItem `json:"data"`
}

func (r *FundListResponse) UnmarshalJSON(data []byte) error {
	b := bytes.TrimSpace(data)
	if len(b) == 0 {
		r.Total = 0
		r.Data = nil
		return nil
	}

	if b[0] == '[' {
		var items []FundListAPIItem
		if err := json.Unmarshal(b, &items); err != nil {
			return err
		}
		r.Total = len(items)
		r.Data = items
		return nil
	}

	type alias FundListResponse
	var tmp alias
	if err := json.Unmarshal(b, &tmp); err != nil {
		return err
	}
	*r = FundListResponse(tmp)
	return nil
}

// FundListAPIItem represents a fund item from the API
type FundListAPIItem struct {
	Name                    string  `json:"name"`
	Sector                  string  `json:"sector"`
	PVP                     float64 `json:"p_vp"`
	DividendYield           float64 `json:"dividend_yield"`
	DividendYieldLast5Years float64 `json:"dividend_yield_last_5_years"`
	DailyLiquidity          float64 `json:"daily_liquidity"`
	NetWorth                float64 `json:"net_worth"`
	Type                    string  `json:"type"`
}

// FundListItem represents a normalized fund item
type FundListItem struct {
	Code                    string
	Sector                  string
	PVP                     float64
	DividendYield           float64
	DividendYieldLast5Years float64
	DailyLiquidity          float64
	NetWorth                float64
	Type                    string
}

// buildFundListParams builds the form parameters for fund list request
func buildFundListParams() string {
	params := url.Values{}
	params.Set("current_page", "0")
	params.Set("per_page", "100000")
	params.Set("order_by_field", "ticker")
	params.Set("order_by_direction", "asc")
	params.Set("search[tipo][]", "FII")

	return params.Encode()
}
