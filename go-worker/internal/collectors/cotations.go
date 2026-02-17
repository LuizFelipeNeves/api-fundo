package collectors

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/luizfelipeneves/api-fundo/go-worker/internal/db"
	"github.com/luizfelipeneves/api-fundo/go-worker/internal/httpclient"
	"github.com/luizfelipeneves/api-fundo/go-worker/internal/parsers"
)

// CotationsCollector collects historical cotations
type CotationsCollector struct {
	client *httpclient.Client
	db     *db.DB
}

// NewCotationsCollector creates a new cotations collector
func NewCotationsCollector(client *httpclient.Client, database *db.DB) *CotationsCollector {
	return &CotationsCollector{
		client: client,
		db:     database,
	}
}

// Name returns the collector name
func (c *CotationsCollector) Name() string {
	return "cotations"
}

// Collect fetches historical cotations
func (c *CotationsCollector) Collect(ctx context.Context, req CollectRequest) (*CollectResult, error) {
	code := parsers.NormalizeFundCode(req.FundCode)
	days := 3650 // ~10 years

	if verboseLogs() {
		log.Printf("[cotations] collecting cotations for %s (days=%d)\n", code, days)
	}

	// Get fund ID from database
	fundID, err := c.db.GetFundIDByCode(ctx, code)
	if err != nil {
		return nil, fmt.Errorf("failed to get fund ID: %w", err)
	}

	if fundID == "" {
		return nil, fmt.Errorf("FII_NOT_FOUND: %s", code)
	}

	// Fetch cotations from API
	url := fmt.Sprintf("%s/api/fii/cotacoes/chart/%s/%d/true", httpclient.BaseURL, fundID, days)

	var rawData map[string][]interface{}
	err = c.client.GetJSON(ctx, url, &rawData)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch cotations: %w", err)
	}

	// Normalize cotations
	cotations := parsers.NormalizeCotations(rawData)

	// Convert to items with fund code and ISO dates
	var items []CotationItem
	for _, cot := range cotations.Real {
		dateISO := parsers.ToDateISO(cot.Date)
		if dateISO == "" {
			continue
		}

		items = append(items, CotationItem{
			FundCode: code,
			DateISO:  dateISO,
			Price:    cot.Price,
		})
	}

	// Deduplicate by fund_code|date_iso
	cotationMap := make(map[string]CotationItem)
	for _, item := range items {
		key := fmt.Sprintf("%s|%s", item.FundCode, item.DateISO)
		cotationMap[key] = item
	}

	// Convert back to slice
	var finalItems []CotationItem
	for _, item := range cotationMap {
		finalItems = append(finalItems, item)
	}

	return &CollectResult{
		Data:      finalItems,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}, nil
}

// CotationItem represents a single cotation entry
type CotationItem struct {
	FundCode string
	DateISO  string
	Price    float64
}
