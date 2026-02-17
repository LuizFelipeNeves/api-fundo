package collectors

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strconv"
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
	if verboseLogs() {
		log.Println("[fund_list] collecting fund list")
	}

	csrfToken, cookieHeader, err := c.client.GetInvestidor10Session(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize investidor10 session: %w", err)
	}

	start := 0
	length := 1000
	draw := 1

	all := make([]FundListAPIItem, 0, length)
	for page := 0; page < 250; page++ {
		formData := buildFundListParamsPage(start, length, draw)

		var response FundListResponse
		err = c.client.PostFormInvestidor10(ctx, httpclient.BaseURL+"/api/fii/advanced-search", formData, csrfToken, cookieHeader, &response)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch fund list: %w", err)
		}

		if len(response.Data) == 0 {
			break
		}

		all = append(all, response.Data...)

		start += length
		draw++

		if response.Total > 0 && len(all) >= response.Total {
			break
		}
		if len(response.Data) < length {
			break
		}
	}

	// Normalize fund codes
	var funds []FundListItem
	for _, item := range all {
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

	type dataTablesResponse struct {
		Draw            int               `json:"draw"`
		Total           int               `json:"total"`
		RecordsTotal    int               `json:"recordsTotal"`
		RecordsFiltered int               `json:"recordsFiltered"`
		Data            []FundListAPIItem `json:"data"`
	}

	var dt dataTablesResponse
	if err := json.Unmarshal(b, &dt); err == nil {
		if dt.Draw != 0 || dt.Total != 0 || dt.RecordsTotal != 0 || dt.RecordsFiltered != 0 {
			switch {
			case dt.Total > 0:
				r.Total = dt.Total
			case dt.RecordsFiltered > 0:
				r.Total = dt.RecordsFiltered
			default:
				r.Total = dt.RecordsTotal
			}
			r.Data = dt.Data
			return nil
		}
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
	return buildFundListParamsPage(0, 1000, 1)
}

func buildFundListParamsPage(start int, length int, draw int) string {
	params := url.Values{}
	params.Set("draw", strconv.Itoa(draw))
	params.Set("start", strconv.Itoa(start))
	params.Set("length", strconv.Itoa(length))
	params.Set("search[value]", "")
	params.Set("search[regex]", "false")

	params.Set("columns[0][data]", "")
	params.Set("columns[0][name]", "name")
	params.Set("columns[0][searchable]", "true")
	params.Set("columns[0][orderable]", "true")
	params.Set("columns[0][search][value]", "")
	params.Set("columns[0][search][regex]", "false")

	params.Set("columns[1][data]", "p_vp")
	params.Set("columns[1][name]", "p_vp")
	params.Set("columns[1][searchable]", "true")
	params.Set("columns[1][orderable]", "true")
	params.Set("columns[1][search][value]", "")
	params.Set("columns[1][search][regex]", "false")

	params.Set("columns[2][data]", "dividend_yield")
	params.Set("columns[2][name]", "dividend_yield")
	params.Set("columns[2][searchable]", "true")
	params.Set("columns[2][orderable]", "true")
	params.Set("columns[2][search][value]", "")
	params.Set("columns[2][search][regex]", "false")

	params.Set("columns[3][data]", "dividend_yield_last_5_years")
	params.Set("columns[3][name]", "dividend_yield_last_5_years")
	params.Set("columns[3][searchable]", "true")
	params.Set("columns[3][orderable]", "true")
	params.Set("columns[3][search][value]", "")
	params.Set("columns[3][search][regex]", "false")

	params.Set("columns[4][data]", "daily_liquidity")
	params.Set("columns[4][name]", "daily_liquidity")
	params.Set("columns[4][searchable]", "true")
	params.Set("columns[4][orderable]", "true")
	params.Set("columns[4][search][value]", "")
	params.Set("columns[4][search][regex]", "false")

	params.Set("columns[5][data]", "net_worth")
	params.Set("columns[5][name]", "net_worth")
	params.Set("columns[5][searchable]", "true")
	params.Set("columns[5][orderable]", "true")
	params.Set("columns[5][search][value]", "")
	params.Set("columns[5][search][regex]", "false")

	params.Set("columns[6][data]", "type")
	params.Set("columns[6][name]", "type")
	params.Set("columns[6][searchable]", "true")
	params.Set("columns[6][orderable]", "true")
	params.Set("columns[6][search][value]", "")
	params.Set("columns[6][search][regex]", "false")

	params.Set("columns[7][data]", "sector")
	params.Set("columns[7][name]", "sector")
	params.Set("columns[7][searchable]", "true")
	params.Set("columns[7][orderable]", "true")
	params.Set("columns[7][search][value]", "")
	params.Set("columns[7][search][regex]", "false")

	params.Set("type_page", "fiis")
	params.Set("sector", "")
	params.Set("type", "")

	params.Set("ranges[p_vp][0]", "0")
	params.Set("ranges[p_vp][1]", "100")
	params.Set("ranges[p_vp][2]", "true")
	params.Set("ranges[p_vp][3]", "true")

	params.Set("ranges[dividend_yield][0]", "0")
	params.Set("ranges[dividend_yield][1]", "100")
	params.Set("ranges[dividend_yield][2]", "true")
	params.Set("ranges[dividend_yield][3]", "true")

	params.Set("ranges[dividend_yield_last_5_years][0]", "0")
	params.Set("ranges[dividend_yield_last_5_years][1]", "100")
	params.Set("ranges[dividend_yield_last_5_years][2]", "false")
	params.Set("ranges[dividend_yield_last_5_years][3]", "false")

	params.Set("daily_liquidity", "")
	params.Set("net_worth", "")

	return params.Encode()
}
