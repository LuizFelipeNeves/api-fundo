package fii

import (
	"context"
	"time"

	"github.com/luizfelipeneves/api-fundo/go-api/internal/model"
)

func (s *Service) ExportFund(ctx context.Context, code string, opts ExportFundOptions) (*ExportFundJSON, bool, error) {
	details, err := s.GetFundDetails(ctx, code)
	if err != nil {
		return nil, false, err
	}
	if details == nil {
		return nil, false, nil
	}

	cotDays := clampInt(opts.CotationsDays, 1825, 1, 5000)
	snapLimit := clampInt(opts.IndicatorsSnapshotsLimit, 365, 1, 5000)

	cotations, err := s.GetCotations(ctx, code, cotDays)
	if err != nil {
		return nil, false, err
	}

	dividends, _, err := s.GetDividends(ctx, code)
	if err != nil {
		return nil, false, err
	}
	if dividends == nil {
		dividends = []model.DividendData{}
	}

	snapshots, err := s.GetLatestIndicatorsSnapshots(ctx, code, snapLimit)
	if err != nil {
		return nil, false, err
	}

	today, _, err := s.GetLatestCotationsToday(ctx, code)
	if err != nil {
		return nil, false, err
	}
	if today == nil {
		today = []model.CotationTodayItem{}
	}

	cotationItems := []model.CotationItem{}
	if cotations != nil {
		cotationItems = cotations.Real
	}

	periodStart := ""
	periodEnd := ""
	startISO := ""
	endISO := ""
	if len(cotationItems) > 0 {
		periodStart = cotationItems[0].Date
		periodEnd = cotationItems[len(cotationItems)-1].Date
		startISO = ToDateISOFromBR(periodStart)
		endISO = ToDateISOFromBR(periodEnd)
	}

	dividendsInPeriod := dividends
	if startISO != "" && endISO != "" {
		tmp := make([]model.DividendData, 0, len(dividends))
		for _, d := range dividends {
			iso := ToDateISOFromBR(d.Date)
			if iso == "" {
				iso = ToDateISOFromBR(d.Payment)
			}
			if iso == "" || iso < startISO || iso > endISO {
				continue
			}
			tmp = append(tmp, d)
		}
		dividendsInPeriod = tmp
	}

	out := ExportFundJSON{
		GeneratedAt: time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
		Fund:        details,
		Period: ExportFundPeriod{
			Start:              periodStart,
			End:                periodEnd,
			CotationsDaysLimit: cotDays,
		},
		Data: ExportFundData{
			Cotations:      cotationItems,
			Dividends:      dividendsInPeriod,
			CotationsToday: today,
		},
	}
	if len(snapshots) > 0 {
		out.Data.IndicatorsLatest = snapshots[0].Data
	}

	m, ok, err := s.GetFundMetricsLatest(ctx, code)
	if err != nil {
		return nil, false, err
	}
	if ok && m != nil {
		paid := m.DividendPaidMonths12m
		if paid < 0 {
			paid = 0
		}
		if paid > 12 {
			paid = 12
		}
		out.Metrics.Valuation.PVPCurrent = m.PVPCurrent
		out.Metrics.Valuation.PVPPercentile = m.PVPPercentile
		out.Metrics.DividendYield.MonthlyMean = m.DYMonthlyMean
		out.Metrics.Dividends.CV = m.DividendCV
		out.Metrics.Dividends.TrendSlope = m.DividendTrendSlope
		out.Metrics.Dividends.MonthsWithPayment = paid
		out.Metrics.Dividends.MonthsWithoutPayment = 12 - paid
		out.Metrics.Dividends.Regularity = m.DividendRegularity12m
		out.Metrics.Dividends.Mean = m.DividendMean12m
		out.Metrics.Dividends.Max = m.DividendMax12m
		out.Metrics.Dividends.Min = m.DividendMin12m
		out.Metrics.Risk.DrawdownMax = m.DrawdownMax
		out.Metrics.Risk.RecoveryTimeDays = m.RecoveryTimeDays
		out.Metrics.Risk.VolatilityAnnualized = m.VolAnnual
		out.Metrics.Risk.Sharpe = m.Sharpe
		out.Metrics.Liquidity.Mean = m.LiqMean
		out.Metrics.Liquidity.PctDaysTraded = m.PctDaysTraded
		out.Metrics.Price.Last3dReturn = m.PriceLast3dReturn
		out.Metrics.Price.VolatilityAnnualized = m.VolAnnual
		out.Metrics.Today.Return = m.TodayReturn
	}
	return &out, true, nil
}
