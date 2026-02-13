package collectors

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/luizfelipeneves/api-fundo/go-worker/internal/httpclient"
	"github.com/luizfelipeneves/api-fundo/go-worker/internal/parsers"
)

// FundDetailsCollector collects fund details via HTML scraping
type FundDetailsCollector struct {
	client *httpclient.Client
}

// NewFundDetailsCollector creates a new fund details collector
func NewFundDetailsCollector(client *httpclient.Client) *FundDetailsCollector {
	return &FundDetailsCollector{client: client}
}

// Name returns the collector name
func (c *FundDetailsCollector) Name() string {
	return "fund_details"
}

// Collect fetches fund details by scraping HTML
func (c *FundDetailsCollector) Collect(ctx context.Context, req CollectRequest) (*CollectResult, error) {
	code := parsers.NormalizeFundCode(req.FundCode)
	if verboseLogs() {
		log.Printf("[fund_details] collecting details for %s\n", code)
	}

	// Fetch HTML page
	html, err := c.client.GetHTML(ctx, fmt.Sprintf("%s/fiis/%s/", httpclient.BaseURL, code))
	if err != nil {
		return nil, fmt.Errorf("failed to fetch HTML: %w", err)
	}

	// Parse fund details from HTML
	details, err := parsers.ExtractFundDetails(html, code)
	if err != nil {
		return nil, fmt.Errorf("failed to parse fund details: %w", err)
	}

	// Extract dividends history from HTML
	dividends, err := parsers.ExtractDividendsHistory(html)
	if err != nil {
		return nil, fmt.Errorf("failed to parse dividends: %w", err)
	}

	// Normalize dividends
	var normalizedDividends []DividendItem
	for _, d := range dividends {
		dateISO := parsers.ToDateISO(d.Date)
		paymentISO := parsers.ToDateISO(d.Payment)

		if dateISO == "" || paymentISO == "" {
			continue
		}

		normalizedDividends = append(normalizedDividends, DividendItem{
			FundCode: code,
			DateISO:  dateISO,
			Payment:  paymentISO,
			Type:     strconv.Itoa(parsers.DividendTypeToCode(d.Type)),
			Value:    d.Value,
		})
	}

	// Deduplicate dividends by key (fund_code|date_iso|type)
	dividendMap := make(map[string]DividendItem)
	for _, div := range normalizedDividends {
		key := fmt.Sprintf("%s|%s|%s", div.FundCode, div.DateISO, div.Type)
		existing, exists := dividendMap[key]

		if !exists {
			dividendMap[key] = div
			continue
		}

		// Keep the one with the latest payment date or different value
		if div.Payment > existing.Payment {
			dividendMap[key] = div
		} else if div.Payment == existing.Payment && div.Value != existing.Value {
			dividendMap[key] = div
		}
	}

	// Convert map back to slice
	var finalDividends []DividendItem
	for _, div := range dividendMap {
		finalDividends = append(finalDividends, div)
	}

	return &CollectResult{
		Data: FundDetailsData{
			Details:   details,
			Dividends: finalDividends,
		},
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}, nil
}

// FundDetailsData represents fund details with dividends
type FundDetailsData struct {
	Details   *parsers.FundDetails
	Dividends []DividendItem
}

// DividendItem represents a dividend entry
type DividendItem struct {
	FundCode string
	DateISO  string
	Payment  string
	Type     string
	Value    float64
}
