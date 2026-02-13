package telegram

import (
	"context"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/luizfelipeneves/api-fundo/go-api/internal/fii"
	"github.com/luizfelipeneves/api-fundo/go-api/internal/model"
)

func (p *Processor) handleRankHoje(ctx context.Context, chatID string, codes []string) error {
	if p.FII == nil {
		return p.Client.SendText(ctx, chatID, "Serviço indisponível.", nil)
	}

	requested := uniqueUppercase(codes)
	if len(requested) == 0 {
		funds, err := p.Repo.ListUserFunds(ctx, chatID)
		if err != nil {
			return err
		}
		requested = uniqueUppercase(funds)
	}
	if len(requested) == 0 {
		return p.Client.SendText(ctx, chatID, "Sua lista está vazia.", nil)
	}

	existing, err := p.Repo.ListExistingFundCodes(ctx, requested)
	if err != nil {
		return err
	}
	missing := diffStrings(uppercaseAll(requested), existing)

	ranked := make([]RankHojeItem, 0, len(existing))
	for _, code := range existing {
		exp, found, err := p.FII.ExportFund(ctx, code, fii.ExportFundOptions{})
		if err != nil || !found || exp == nil || exp.Fund == nil {
			continue
		}

		vacancia := exp.Fund.Vacancia
		dailyLiquidity := 0.0
		if exp.Fund.DailyLiquidity != nil && *exp.Fund.DailyLiquidity > 0 {
			dailyLiquidity = *exp.Fund.DailyLiquidity
		}

		pvp := exp.Metrics.Valuation.PVPCurrent
		dyMonthly := exp.Metrics.DividendYield.MonthlyMean
		sharpe := exp.Metrics.Risk.Sharpe
		todayReturn := exp.Metrics.Today.Return
		last3dReturn := exp.Metrics.Price.Last3dReturn

		notMelting := todayReturn > -0.02 && last3dReturn > -0.05
		if pvp < 0.94 && dyMonthly > 0.011 && vacancia == 0 && dailyLiquidity > 300_000 && sharpe >= 1.7 && notMelting {
			ranked = append(ranked, RankHojeItem{
				Code:                 code,
				PVP:                  pvp,
				DividendYieldMonthly: dyMonthly,
				Sharpe:               sharpe,
				TodayReturn:          todayReturn,
			})
		}
	}

	sort.Slice(ranked, func(i, j int) bool {
		dy := ranked[j].DividendYieldMonthly - ranked[i].DividendYieldMonthly
		if dy != 0 {
			return dy < 0
		}
		sh := ranked[j].Sharpe - ranked[i].Sharpe
		if sh != 0 {
			return sh < 0
		}
		return ranked[i].PVP < ranked[j].PVP
	})

	return p.Client.SendText(ctx, chatID, FormatRankHojeMessage(ranked, len(existing), missing), nil)
}

func (p *Processor) handleRankV(ctx context.Context, chatID string) error {
	if p.FII == nil {
		return p.Client.SendText(ctx, chatID, "Serviço indisponível.", nil)
	}

	allCodes, err := p.Repo.ListAllFundCodes(ctx)
	if err != nil {
		return err
	}

	if len(allCodes) == 0 {
		return p.Client.SendText(ctx, chatID, "Não encontrei fundos na base.", nil)
	}

	ranked := make([]RankVItem, 0, len(allCodes))
	for _, code := range allCodes {
		exp, found, err := p.FII.ExportFund(ctx, code, fii.ExportFundOptions{})
		if err != nil || !found || exp == nil {
			continue
		}

		pvp := exp.Metrics.Valuation.PVPCurrent
		dyMonthly := exp.Metrics.DividendYield.MonthlyMean
		regularity := exp.Metrics.Dividends.Regularity
		monthsWithoutPayment := exp.Metrics.Dividends.MonthsWithoutPayment
		dividendCv := exp.Metrics.Dividends.CV
		dividendTrend := exp.Metrics.Dividends.TrendSlope
		drawdownMax := exp.Metrics.Risk.DrawdownMax
		recoveryDays := exp.Metrics.Risk.RecoveryTimeDays
		volAnnual := exp.Metrics.Risk.VolatilityAnnualized
		pvpPercentile := exp.Metrics.Valuation.PVPPercentile
		liqMean := exp.Metrics.Liquidity.Mean
		pctDaysTraded := exp.Metrics.Liquidity.PctDaysTraded
		last3dReturn := exp.Metrics.Price.Last3dReturn
		todayReturn := exp.Metrics.Today.Return

		series, ok := lastYearDividendSeries(exp.Data.Dividends)
		if !ok {
			continue
		}
		dividendValues := make([]float64, 0, len(series))
		for _, it := range series {
			dividendValues = append(dividendValues, it.Value)
		}
		dividendMax := maxFloat(dividendValues)
		dividendMin := minFloat(dividendValues)
		dividendMean := meanFloat(dividendValues)
		lastDividend := series[len(series)-1].Value

		prevMean := 0.0
		hasPrevMean := false
		if len(series) >= 4 {
			prevValues := dividendValues[:len(dividendValues)-1]
			prevMean = meanFloat(prevValues)
			hasPrevMean = prevMean > 0
		}

		split := 0
		if len(series) >= 6 {
			split = len(series) / 2
		}
		firstHalfMean := 0.0
		lastHalfMean := 0.0
		hasHalves := false
		if split >= 3 && len(series)-split >= 3 {
			firstHalfMean = meanFloat(dividendValues[:split])
			lastHalfMean = meanFloat(dividendValues[split:])
			hasHalves = firstHalfMean > 0 && lastHalfMean > 0
		}

		spikeOk := dividendMean > 0 && dividendMax <= dividendMean*2.5
		lastSpikeOk := !hasPrevMean || lastDividend <= prevMean*2.2
		minOk := dividendMean > 0 && dividendMin >= dividendMean*0.4
		regimeOk := !hasHalves || lastHalfMean <= firstHalfMean*1.8
		regularityYear := math.Min(1, float64(len(series))/12.0)
		notMelting := todayReturn > -0.01 && last3dReturn >= 0

		if pvp <= 0.7 &&
			dyMonthly > 0.0116 &&
			monthsWithoutPayment == 0 &&
			regularityYear >= 0.999 &&
			dividendCv <= 0.6 &&
			dividendTrend > 0 &&
			drawdownMax > -0.25 &&
			recoveryDays <= 120 &&
			volAnnual <= 0.3 &&
			pvpPercentile <= 0.25 &&
			liqMean >= 400000 &&
			pctDaysTraded >= 0.95 &&
			spikeOk &&
			lastSpikeOk &&
			minOk &&
			regimeOk &&
			notMelting {
			ranked = append(ranked, RankVItem{
				Code:                 code,
				PVP:                  pvp,
				DividendYieldMonthly: dyMonthly,
				Regularity:           regularity,
				TodayReturn:          todayReturn,
			})
		}
	}

	sort.Slice(ranked, func(i, j int) bool {
		dy := ranked[j].DividendYieldMonthly - ranked[i].DividendYieldMonthly
		if dy != 0 {
			return dy < 0
		}
		pvp := ranked[i].PVP - ranked[j].PVP
		if pvp != 0 {
			return pvp < 0
		}
		return ranked[j].Regularity > ranked[i].Regularity
	})

	return p.Client.SendText(ctx, chatID, FormatRankVMessage(ranked, len(allCodes)), nil)
}

type dividendPoint struct {
	Iso   string
	Value float64
}

func lastYearDividendSeries(dividends []model.DividendData) ([]dividendPoint, bool) {
	if len(dividends) == 0 {
		return nil, false
	}
	cutoff := time.Now().UTC().AddDate(-1, 0, 0).Format("2006-01-02")
	out := make([]dividendPoint, 0, len(dividends))
	for _, d := range dividends {
		if d.Type != model.Dividendos || d.Value <= 0 {
			continue
		}
		iso := toDateIsoFromBr(d.Date)
		if iso == "" || iso < cutoff {
			continue
		}
		out = append(out, dividendPoint{Iso: iso, Value: d.Value})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Iso < out[j].Iso })
	if len(out) < 12 {
		return nil, false
	}
	return out, true
}

func toDateIsoFromBr(dateBr string) string {
	parts := strings.Split(strings.TrimSpace(dateBr), "/")
	if len(parts) != 3 {
		return ""
	}
	dd := strings.TrimSpace(parts[0])
	mm := strings.TrimSpace(parts[1])
	yy := strings.TrimSpace(parts[2])
	if len(dd) != 2 || len(mm) != 2 || len(yy) != 4 {
		return ""
	}
	return yy + "-" + mm + "-" + dd
}

func meanFloat(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	acc := 0.0
	for _, v := range values {
		acc += v
	}
	return acc / float64(len(values))
}

func maxFloat(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	m := values[0]
	for _, v := range values[1:] {
		if v > m {
			m = v
		}
	}
	return m
}

func minFloat(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	m := values[0]
	for _, v := range values[1:] {
		if v < m {
			m = v
		}
	}
	return m
}
