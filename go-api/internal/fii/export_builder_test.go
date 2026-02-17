package fii

import (
	"math"
	"testing"

	"github.com/luizfelipeneves/api-fundo/go-api/internal/model"
)

func TestBuildExportFundJSON_PVPCurrent_FallbackFromDetails(t *testing.T) {
	details := &model.FundDetails{
		Code:                 "TEST11",
		ValorPatrimonialCota: 100,
	}
	cotations := &model.NormalizedCotations{
		Real: []model.CotationItem{
			{Date: "01/01/2026", Price: 70},
			{Date: "02/01/2026", Price: 80},
		},
	}

	got := buildExportFundJSON(details, cotations, nil, nil, nil, 0)

	if math.Abs(got.Metrics.Valuation.PVPCurrent-0.8) > 1e-9 {
		t.Fatalf("expected pvp_current=0.8, got %.10f", got.Metrics.Valuation.PVPCurrent)
	}
	if math.Abs(got.Metrics.Valuation.PVPPercentile-1.0) > 1e-9 {
		t.Fatalf("expected pvp_percentile=1.0, got %.10f", got.Metrics.Valuation.PVPPercentile)
	}
}

func TestBuildExportFundJSON_PVPCurrent_UsesSnapshot(t *testing.T) {
	v := 1.0
	indicatorSnapshots := []IndicatorsSnapshot{
		{
			FetchedAt: "2026-02-17T12:00:00.000Z",
			Data: model.NormalizedIndicators{
				"pvp": []model.IndicatorItem{
					{Year: "2026", Value: &v},
				},
			},
		},
	}

	details := &model.FundDetails{
		Code:                 "TEST11",
		ValorPatrimonialCota: 100,
	}
	cotations := &model.NormalizedCotations{
		Real: []model.CotationItem{
			{Date: "01/01/2026", Price: 70},
			{Date: "02/01/2026", Price: 80},
		},
	}

	got := buildExportFundJSON(details, cotations, nil, indicatorSnapshots, nil, 0)

	if math.Abs(got.Metrics.Valuation.PVPCurrent-1.0) > 1e-9 {
		t.Fatalf("expected pvp_current=1.0, got %.10f", got.Metrics.Valuation.PVPCurrent)
	}
	if math.Abs(got.Metrics.Valuation.PVPPercentile-1.0) > 1e-9 {
		t.Fatalf("expected pvp_percentile=1.0, got %.10f", got.Metrics.Valuation.PVPPercentile)
	}
}

func TestBuildExportFundJSON_DividendYieldMonthly_IgnoresZeroCotationInMonthLastPrice(t *testing.T) {
	details := &model.FundDetails{
		Code: "TEST11",
	}

	cotations := &model.NormalizedCotations{
		Real: []model.CotationItem{
			{Date: "01/01/2026", Price: 100},
			{Date: "30/01/2026", Price: 100},
			{Date: "31/01/2026", Price: 0},
		},
	}

	dividends := []model.DividendData{
		{Type: model.Dividendos, Date: "15/01/2026", Value: 2},
	}

	got := buildExportFundJSON(details, cotations, dividends, nil, nil, 0)

	if math.Abs(got.Metrics.DividendYield.MonthlyMean-0.02) > 1e-9 {
		t.Fatalf("expected dy_monthly=0.02, got %.10f", got.Metrics.DividendYield.MonthlyMean)
	}
}
