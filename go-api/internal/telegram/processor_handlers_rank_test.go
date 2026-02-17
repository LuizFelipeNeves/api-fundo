package telegram

import (
	"testing"
	"time"

	"github.com/luizfelipeneves/api-fundo/go-api/internal/model"
)

func TestLast12MonthlyDividendSeries_CountsPaidMonthsAndFillsZeros(t *testing.T) {
	now := time.Date(2026, 2, 17, 12, 0, 0, 0, time.UTC)

	dividends := make([]model.DividendData, 0, 12)
	for m := time.February; m <= time.December; m++ {
		dividends = append(dividends, model.DividendData{
			Type:  model.Dividendos,
			Date:  brDate(2025, m, 10),
			Value: 1,
		})
	}
	dividends = append(dividends, model.DividendData{
		Type:  model.Dividendos,
		Date:  brDate(2026, time.January, 10),
		Value: 1,
	})

	series, paidMonths, ok := last12MonthlyDividendSeries(dividends, now)
	if !ok {
		t.Fatalf("expected ok=true")
	}
	if len(series) != 12 {
		t.Fatalf("expected 12 points, got %d", len(series))
	}
	if paidMonths != 12 {
		t.Fatalf("expected paidMonths=12, got %d", paidMonths)
	}

	for _, p := range series {
		if p.Value <= 0 {
			t.Fatalf("expected all months paid, got month=%s value=%v", p.Iso, p.Value)
		}
	}
}

func TestLast12MonthlyDividendSeries_MissingMonthReturnsPaidMonths11(t *testing.T) {
	now := time.Date(2026, 2, 17, 12, 0, 0, 0, time.UTC)

	dividends := make([]model.DividendData, 0, 11)
	for m := time.February; m <= time.December; m++ {
		if m == time.June {
			continue
		}
		dividends = append(dividends, model.DividendData{
			Type:  model.Dividendos,
			Date:  brDate(2025, m, 10),
			Value: 1,
		})
	}
	dividends = append(dividends, model.DividendData{
		Type:  model.Dividendos,
		Date:  brDate(2026, time.January, 10),
		Value: 1,
	})

	series, paidMonths, ok := last12MonthlyDividendSeries(dividends, now)
	if !ok {
		t.Fatalf("expected ok=true")
	}
	if len(series) != 12 {
		t.Fatalf("expected 12 points, got %d", len(series))
	}
	if paidMonths != 11 {
		t.Fatalf("expected paidMonths=11, got %d", paidMonths)
	}

	zeroCount := 0
	for _, p := range series {
		if p.Value == 0 {
			zeroCount++
		}
	}
	if zeroCount != 1 {
		t.Fatalf("expected exactly 1 zero month, got %d", zeroCount)
	}
}

func brDate(year int, month time.Month, day int) string {
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC).Format("02/01/2006")
}
