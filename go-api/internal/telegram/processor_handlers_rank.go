package telegram

import (
	"context"
	"sort"
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

	if len(existing) > 0 {
		rows, err := p.FII.ListRankHojeSources(ctx, existing)
		if err != nil {
			return err
		}

		for _, r := range rows {
			if !r.VacanciaValid || !isFinite(r.Vacancia) {
				continue
			}
			v := r.Vacancia
			dailyLiquidity := 0.0
			if r.DailyLiquidityValid && isFinite(r.DailyLiquidity) && r.DailyLiquidity > 0 {
				dailyLiquidity = r.DailyLiquidity
			}

			if !isFinite(r.PVPCurrent) || r.PVPCurrent <= 0 {
				continue
			}
			notMelting := r.TodayReturn > -0.02 && r.PriceLast3dReturn > -0.05
			if r.PVPCurrent < 0.94 && r.DYMonthlyMean > 0.011 && v == 0 && dailyLiquidity > 300_000 && r.Sharpe >= 1.7 && notMelting {
				ranked = append(ranked, RankHojeItem{
					Code:                 r.Code,
					PVP:                  r.PVPCurrent,
					DividendYieldMonthly: r.DYMonthlyMean,
					Sharpe:               r.Sharpe,
					TodayReturn:          r.TodayReturn,
				})
			}
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
	candidates, err := p.FII.ListRankVCandidates(ctx, allCodes)
	if err != nil {
		return err
	}

	for _, c := range candidates {
		spikeOk := c.DividendMean12m > 0 && c.DividendMax12m <= c.DividendMean12m*2.5
		lastSpikeOk := c.DividendPrevMean11m <= 0 || c.DividendLastValue <= c.DividendPrevMean11m*2.2
		minOk := c.DividendMean12m > 0 && c.DividendMin12m >= c.DividendMean12m*0.4
		regimeOk := c.DividendFirstHalfMean <= 0 || c.DividendLastHalfMean <= c.DividendFirstHalfMean*1.8

		if spikeOk && lastSpikeOk && minOk && regimeOk {
			ranked = append(ranked, RankVItem{
				Code:                 c.Code,
				PVP:                  c.PVPCurrent,
				DividendYieldMonthly: c.DYMonthlyMean,
				Regularity:           c.DividendRegularity12m,
				TodayReturn:          c.TodayReturn,
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

func last12MonthlyDividendSeries(dividends []model.DividendData, now time.Time) ([]dividendPoint, int, bool) {
	if len(dividends) == 0 {
		return nil, 0, false
	}

	lastDayPrevMonth := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, -1)
	endMonth := time.Date(lastDayPrevMonth.Year(), lastDayPrevMonth.Month(), 1, 0, 0, 0, 0, time.UTC)

	months := make([]string, 0, 12)
	for i := 11; i >= 0; i-- {
		m := endMonth.AddDate(0, -i, 0)
		months = append(months, m.Format("2006-01"))
	}

	dividendByMonth := map[string]float64{}
	for _, d := range dividends {
		if d.Type != model.Dividendos || d.Value <= 0 {
			continue
		}
		iso := fii.ToDateISOFromBR(d.Date)
		if iso == "" {
			iso = fii.ToDateISOFromBR(d.Payment)
		}
		if len(iso) < 7 {
			continue
		}
		mk := iso[:7]
		dividendByMonth[mk] += d.Value
	}

	out := make([]dividendPoint, 0, 12)
	paidMonths := 0
	for _, mk := range months {
		v := dividendByMonth[mk]
		if v > 0 {
			paidMonths++
		}
		out = append(out, dividendPoint{Iso: mk + "-01", Value: v})
	}
	if paidMonths == 0 {
		return nil, 0, false
	}
	return out, paidMonths, true
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
