package telegram

import (
	"context"
	"database/sql"
	"sort"
	"strings"
	"time"

	"github.com/lib/pq"
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
		rows, err := p.FII.DB.QueryContext(ctx, `
			SELECT
				m.fund_code,
				COALESCE(m.pvp_current, 0),
				COALESCE(m.dy_monthly_mean, 0),
				COALESCE(m.sharpe, 0),
				COALESCE(m.today_return, 0),
				COALESCE(m.price_last3d_return, 0),
				fm.vacancia,
				fm.daily_liquidity
			FROM fund_metrics_latest m
			JOIN fund_master fm ON fm.code = m.fund_code
			WHERE m.fund_code = ANY($1)
		`, pq.Array(existing))
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var (
				code         string
				pvp          float64
				dyMonthly    float64
				sharpe       float64
				todayReturn  float64
				last3dReturn float64
				vacancia     sql.NullFloat64
				daily        sql.NullFloat64
			)
			if err := rows.Scan(&code, &pvp, &dyMonthly, &sharpe, &todayReturn, &last3dReturn, &vacancia, &daily); err != nil {
				return err
			}

			if !vacancia.Valid || !isFinite(vacancia.Float64) {
				continue
			}
			v := vacancia.Float64
			dailyLiquidity := 0.0
			if daily.Valid && isFinite(daily.Float64) && daily.Float64 > 0 {
				dailyLiquidity = daily.Float64
			}

			if !isFinite(pvp) || pvp <= 0 {
				continue
			}
			notMelting := todayReturn > -0.02 && last3dReturn > -0.05
			if pvp < 0.94 && dyMonthly > 0.011 && v == 0 && dailyLiquidity > 300_000 && sharpe >= 1.7 && notMelting {
				ranked = append(ranked, RankHojeItem{
					Code:                 code,
					PVP:                  pvp,
					DividendYieldMonthly: dyMonthly,
					Sharpe:               sharpe,
					TodayReturn:          todayReturn,
				})
			}
		}
		if err := rows.Err(); err != nil {
			return err
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
	rows, err := p.FII.DB.QueryContext(ctx, `
		SELECT
			fund_code,
			COALESCE(pvp_current, 0),
			COALESCE(dy_monthly_mean, 0),
			COALESCE(today_return, 0),
			COALESCE(dividend_regularity_12m, 0),
			COALESCE(dividend_mean_12m, 0),
			COALESCE(dividend_prev_mean_11m, 0),
			COALESCE(dividend_first_half_mean_12m, 0),
			COALESCE(dividend_last_half_mean_12m, 0),
			COALESCE(dividend_max_12m, 0),
			COALESCE(dividend_min_12m, 0),
			COALESCE(dividend_last_value, 0)
		FROM fund_metrics_latest
		WHERE fund_code = ANY($1)
			AND COALESCE(pvp_current, 1e9) <= 0.7
			AND COALESCE(dy_monthly_mean, 0) > 0.0116
			AND COALESCE(dividend_cv, 1e9) <= 0.6
			AND COALESCE(dividend_trend_slope, -1e9) > 0
			AND COALESCE(drawdown_max, -1e9) > -0.25
			AND COALESCE(recovery_time_days, 1e9) <= 120
			AND COALESCE(vol_annual, 1e9) <= 0.3
			AND COALESCE(pvp_percentile, 1e9) <= 0.25
			AND COALESCE(liq_mean, 0) >= 400000
			AND COALESCE(pct_days_traded, 0) >= 0.95
			AND COALESCE(price_last3d_return, -1e9) >= 0
			AND COALESCE(today_return, -1e9) > -0.01
			AND COALESCE(dividend_paid_months_12m, 0) >= 12
	`, pq.Array(allCodes))
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			code                  string
			pvp                   float64
			dyMonthly             float64
			todayReturn           float64
			regularity12m         float64
			dividendMean12m       float64
			dividendPrevMean11m   float64
			dividendFirstHalfMean float64
			dividendLastHalfMean  float64
			dividendMax12m        float64
			dividendMin12m        float64
			dividendLastValue     float64
		)
		if err := rows.Scan(
			&code,
			&pvp,
			&dyMonthly,
			&todayReturn,
			&regularity12m,
			&dividendMean12m,
			&dividendPrevMean11m,
			&dividendFirstHalfMean,
			&dividendLastHalfMean,
			&dividendMax12m,
			&dividendMin12m,
			&dividendLastValue,
		); err != nil {
			return err
		}

		spikeOk := dividendMean12m > 0 && dividendMax12m <= dividendMean12m*2.5
		lastSpikeOk := dividendPrevMean11m <= 0 || dividendLastValue <= dividendPrevMean11m*2.2
		minOk := dividendMean12m > 0 && dividendMin12m >= dividendMean12m*0.4
		regimeOk := dividendFirstHalfMean <= 0 || dividendLastHalfMean <= dividendFirstHalfMean*1.8

		if spikeOk && lastSpikeOk && minOk && regimeOk {
			ranked = append(ranked, RankVItem{
				Code:                 code,
				PVP:                  pvp,
				DividendYieldMonthly: dyMonthly,
				Regularity:           regularity12m,
				TodayReturn:          todayReturn,
			})
		}
	}
	if err := rows.Err(); err != nil {
		return err
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
		iso := toDateIsoFromBr(d.Date)
		if iso == "" {
			iso = toDateIsoFromBr(d.Payment)
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
