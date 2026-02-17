package collectors

import (
	"context"
	"time"

	"github.com/luizfelipeneves/api-fundo/go-worker/internal/parsers"
	"github.com/luizfelipeneves/api-fundo/go-worker/internal/statusinvest"
)

type MarketSnapshotCollector struct {
	svc *statusinvest.AdvancedSearchService
}

func NewMarketSnapshotCollector(svc *statusinvest.AdvancedSearchService) *MarketSnapshotCollector {
	return &MarketSnapshotCollector{svc: svc}
}

func (c *MarketSnapshotCollector) Name() string {
	return "market_snapshot"
}

func (c *MarketSnapshotCollector) Collect(ctx context.Context, req CollectRequest) (*CollectResult, error) {
	quotes, err := c.svc.ListQuotes(ctx)
	if err != nil {
		return nil, err
	}

	now := time.Now().In(time.Local)
	dateISO := now.Format("2006-01-02")
	hour := now.Format("15:04")
	fetchedAt := now.UTC().Format(time.RFC3339)

	items := make([]MarketSnapshotItem, 0, len(quotes))
	for _, q := range quotes {
		code := parsers.NormalizeFundCode(q.Ticker)
		if code == "" || q.Price <= 0 {
			continue
		}
		items = append(items, MarketSnapshotItem{FundCode: code, Price: q.Price})
	}

	return &CollectResult{
		Data: MarketSnapshotData{
			DateISO:   dateISO,
			Hour:      hour,
			FetchedAt: fetchedAt,
			Items:     items,
		},
		Timestamp: fetchedAt,
	}, nil
}

type MarketSnapshotItem struct {
	FundCode string
	Price    float64
}

type MarketSnapshotData struct {
	DateISO   string
	Hour      string
	FetchedAt string
	Items     []MarketSnapshotItem
}
