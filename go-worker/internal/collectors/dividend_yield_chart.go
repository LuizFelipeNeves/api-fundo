package collectors

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/luizfelipeneves/api-fundo/go-worker/internal/httpclient"
)

// DividendYieldChartCollector collects dividend yield data from chart API
type DividendYieldChartCollector struct {
	client *httpclient.Client
}

// NewDividendYieldChartCollector creates a new dividend yield chart collector
func NewDividendYieldChartCollector(client *httpclient.Client) *DividendYieldChartCollector {
	return &DividendYieldChartCollector{client: client}
}

// Name returns the collector name
func (c *DividendYieldChartCollector) Name() string {
	return "dividend_yield_chart"
}

// Collect fetches dividend yield chart data
func (c *DividendYieldChartCollector) Collect(ctx context.Context, req CollectRequest) (*CollectResult, error) {
	id := req.ID
	if verboseLogs() {
		log.Printf("[dividend_yield_chart] collecting for id=%s\n", id)
	}

	// Validate ID is a valid numeric Status Invest ID
	if id == "" || !isNumericID(id) {
		return nil, fmt.Errorf("invalid or missing Status Invest ID: %s", id)
	}

	// Fetch chart data from API
	var chartData []DividendYieldChartItem
	url := fmt.Sprintf("%s/api/fii/dividend-yield/chart/%s/1825/mes", httpclient.BaseURL, id)
	if err := c.client.GetJSON(ctx, url, &chartData); err != nil {
		return nil, fmt.Errorf("failed to fetch dividend yield chart: %w", err)
	}

	return &CollectResult{
		Data: DividendYieldChartData{
			Items: chartData,
		},
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}, nil
}

// isNumericID checks if a string is a valid numeric Status Invest ID
func isNumericID(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// DividendYieldChartItem represents a single item in the dividend yield chart
type DividendYieldChartItem struct {
	CreatedAt string      `json:"created_at"` // "12/2024"
	Price     interface{} `json:"price"`      // can be "1.01" (string) or 1.01 (number)
}

// DividendYieldChartData represents the dividend yield chart data result
type DividendYieldChartData struct {
	Items []DividendYieldChartItem
}

// DividendYieldItem represents a normalized dividend yield entry for persistence
type DividendYieldItem struct {
	Month      string  // "2024-12"
	Year       int     // 2024
	MonthNum   int     // 12
	YieldValue float64 // 1.01
}

// ParseDividendYields parses chart items into normalized yield entries
func ParseDividendYields(items []DividendYieldChartItem) []DividendYieldItem {
	var results []DividendYieldItem
	for _, item := range items {
		if item.CreatedAt == "" {
			continue
		}

		// Parse "12/2024" format
		parts := splitString(item.CreatedAt, "/")
		if len(parts) != 2 {
			continue
		}

		monthNum, err := strconv.Atoi(parts[0])
		if err != nil || monthNum < 1 || monthNum > 12 {
			continue
		}

		year, err := strconv.Atoi(parts[1])
		if err != nil || year < 2000 || year > 2100 {
			continue
		}

		yieldValue, err := parsePrice(item.Price)
		if err != nil || yieldValue < 0 {
			continue
		}

		results = append(results, DividendYieldItem{
			Month:      fmt.Sprintf("%d-%02d", year, monthNum),
			Year:       year,
			MonthNum:   monthNum,
			YieldValue: yieldValue,
		})
	}
	return results
}

// parsePrice extracts float64 from interface{} (can be string or number)
func parsePrice(v interface{}) (float64, error) {
	switch val := v.(type) {
	case float64:
		return val, nil
	case string:
		return strconv.ParseFloat(val, 64)
	case nil:
		return 0, fmt.Errorf("price is nil")
	default:
		return 0, fmt.Errorf("unexpected price type: %T", v)
	}
}

func splitString(s string, sep string) []string {
	if s == "" {
		return nil
	}
	result := []string{}
	current := ""
	for _, c := range s {
		if string(c) == sep {
			if current != "" {
				result = append(result, current)
			}
			current = ""
		} else {
			current += string(c)
		}
	}
	if current != "" {
		result = append(result, current)
	}
	return result
}
