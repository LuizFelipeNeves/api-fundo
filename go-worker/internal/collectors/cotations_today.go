package collectors

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"time"

	"github.com/luizfelipeneves/api-fundo/go-worker/internal/httpclient"
	"github.com/luizfelipeneves/api-fundo/go-worker/internal/parsers"
)

// CotationsTodayCollector collects today's cotations from statusinvest
type CotationsTodayCollector struct {
	client *httpclient.Client
}

// NewCotationsTodayCollector creates a new cotations_today collector
func NewCotationsTodayCollector(client *httpclient.Client) *CotationsTodayCollector {
	return &CotationsTodayCollector{client: client}
}

// Name returns the collector name
func (c *CotationsTodayCollector) Name() string {
	return "cotations_today"
}

// Collect fetches today's cotations from statusinvest.com.br
func (c *CotationsTodayCollector) Collect(ctx context.Context, req CollectRequest) (*CollectResult, error) {
	code := parsers.NormalizeFundCode(req.FundCode)
	if verboseLogs() {
		log.Printf("[cotations_today] collecting today's cotations for %s\n", code)
	}

	// Build form data
	params := url.Values{}
	params.Set("ticker", code)
	params.Set("type", "-1")
	params.Add("currences[]", "1")

	// POST to statusinvest API
	var rawData interface{}
	err := c.client.PostFormStatusInvest(
		ctx,
		httpclient.StatusInvestBase+"/fii/tickerprice",
		params.Encode(),
		&rawData,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch cotations today: %w", err)
	}

	// Normalize data
	data := parsers.NormalizeCotationsToday(rawData)

	timestamp := time.Now().UTC().Format(time.RFC3339)
	dateISO := timestamp[:10] // YYYY-MM-DD

	return &CollectResult{
		Data: CotationsTodayData{
			FundCode:  code,
			DateISO:   dateISO,
			FetchedAt: timestamp,
			Data:      data,
		},
		Timestamp: timestamp,
	}, nil
}

// CotationsTodayData represents today's cotation data
type CotationsTodayData struct {
	FundCode  string
	DateISO   string
	FetchedAt string
	Data      parsers.CotationsTodayData
}
