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

// IndicatorsCollector collects fund indicators
type IndicatorsCollector struct {
	client *httpclient.Client
	db     *db.DB
}

// NewIndicatorsCollector creates a new indicators collector
func NewIndicatorsCollector(client *httpclient.Client, database *db.DB) *IndicatorsCollector {
	return &IndicatorsCollector{
		client: client,
		db:     database,
	}
}

// Name returns the collector name
func (c *IndicatorsCollector) Name() string {
	return "indicators"
}

// Collect fetches fund indicators
func (c *IndicatorsCollector) Collect(ctx context.Context, req CollectRequest) (*CollectResult, error) {
	code := parsers.NormalizeFundCode(req.FundCode)
	if verboseLogs() {
		log.Printf("[indicators] collecting indicators for %s\n", code)
	}

	// Get fund ID from database
	fundID, err := c.db.GetFundIDByCode(ctx, code)
	if err != nil {
		return nil, fmt.Errorf("failed to get fund ID: %w", err)
	}

	if fundID == "" {
		return nil, fmt.Errorf("FII_NOT_FOUND: %s", code)
	}

	// Fetch indicators from API
	url := fmt.Sprintf("%s/api/fii/historico-indicadores/%s/5", httpclient.BaseURL, fundID)

	var rawData interface{}
	err = c.client.GetJSON(ctx, url, &rawData)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch indicators: %w", err)
	}

	// Normalize indicators
	indicators := parsers.NormalizeIndicatorsAny(rawData)

	return &CollectResult{
		Data: IndicatorsData{
			FundCode: code,
			Data:     indicators,
		},
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}, nil
}

// IndicatorsData represents indicators data
type IndicatorsData struct {
	FundCode string
	Data     parsers.NormalizedIndicators
}
