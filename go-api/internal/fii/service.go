package fii

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/luizfelipeneves/api-fundo/go-api/internal/db"
	"github.com/luizfelipeneves/api-fundo/go-api/internal/model"
)

type Service struct {
	DB *db.DB
}

func New(db *db.DB) *Service {
	return &Service{DB: db}
}

const cotationPriceScale = 10000

func fromPriceInt(priceInt int) float64 {
	if priceInt <= 0 {
		return 0
	}
	return float64(priceInt) / float64(cotationPriceScale)
}

type FundMetricsLatest struct {
	FundCode              string
	ComputedAt            time.Time
	AsOfDateISO           string
	PVPCurrent            float64
	PVPPercentile         float64
	DYMonthlyMean         float64
	DividendCV            float64
	DividendTrendSlope    float64
	DividendPaidMonths12m int
	DividendRegularity12m float64
	DividendMean12m       float64
	DividendPrevMean11m   float64
	DividendFirstHalfMean float64
	DividendLastHalfMean  float64
	DividendMax12m        float64
	DividendMin12m        float64
	DividendLastValue     float64
	DrawdownMax           float64
	RecoveryTimeDays      int
	VolAnnual             float64
	Sharpe                float64
	LiqMean               float64
	PctDaysTraded         float64
	PriceLast3dReturn     float64
	TodayReturn           float64
}

func (s *Service) GetFundMetricsLatest(ctx context.Context, code string) (*FundMetricsLatest, bool, error) {
	var (
		fundCode           string
		computedAt         time.Time
		asOfDate           time.Time
		pvpCurrent         sql.NullFloat64
		pvpPercentile      sql.NullFloat64
		dyMonthlyMean      sql.NullFloat64
		dividendCV         sql.NullFloat64
		dividendTrendSlope sql.NullFloat64
		paidMonths12m      sql.NullInt64
		regularity12m      sql.NullFloat64
		mean12m            sql.NullFloat64
		prevMean11m        sql.NullFloat64
		firstHalfMean12m   sql.NullFloat64
		lastHalfMean12m    sql.NullFloat64
		max12m             sql.NullFloat64
		min12m             sql.NullFloat64
		lastValue          sql.NullFloat64
		drawdownMax        sql.NullFloat64
		recoveryDays       sql.NullInt64
		volAnnual          sql.NullFloat64
		sharpe             sql.NullFloat64
		liqMean            sql.NullFloat64
		pctDaysTraded      sql.NullFloat64
		last3dReturn       sql.NullFloat64
		todayReturn        sql.NullFloat64
	)

	err := s.DB.QueryRowContext(ctx, `
		SELECT
			fund_code,
			computed_at,
			as_of_date,
			pvp_current,
			pvp_percentile,
			dy_monthly_mean,
			dividend_cv,
			dividend_trend_slope,
			dividend_paid_months_12m,
			dividend_regularity_12m,
			dividend_mean_12m,
			dividend_prev_mean_11m,
			dividend_first_half_mean_12m,
			dividend_last_half_mean_12m,
			dividend_max_12m,
			dividend_min_12m,
			dividend_last_value,
			drawdown_max,
			recovery_time_days,
			vol_annual,
			sharpe,
			liq_mean,
			pct_days_traded,
			price_last3d_return,
			today_return
		FROM fund_metrics_latest
		WHERE fund_code = $1
		LIMIT 1
	`, code).Scan(
		&fundCode,
		&computedAt,
		&asOfDate,
		&pvpCurrent,
		&pvpPercentile,
		&dyMonthlyMean,
		&dividendCV,
		&dividendTrendSlope,
		&paidMonths12m,
		&regularity12m,
		&mean12m,
		&prevMean11m,
		&firstHalfMean12m,
		&lastHalfMean12m,
		&max12m,
		&min12m,
		&lastValue,
		&drawdownMax,
		&recoveryDays,
		&volAnnual,
		&sharpe,
		&liqMean,
		&pctDaysTraded,
		&last3dReturn,
		&todayReturn,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}

	out := &FundMetricsLatest{
		FundCode:              strings.ToUpper(strings.TrimSpace(fundCode)),
		ComputedAt:            computedAt.UTC(),
		AsOfDateISO:           asOfDate.UTC().Format("2006-01-02"),
		PVPCurrent:            nullFloat(pvpCurrent),
		PVPPercentile:         nullFloat(pvpPercentile),
		DYMonthlyMean:         nullFloat(dyMonthlyMean),
		DividendCV:            nullFloat(dividendCV),
		DividendTrendSlope:    nullFloat(dividendTrendSlope),
		DividendPaidMonths12m: int(paidMonths12m.Int64),
		DividendRegularity12m: nullFloat(regularity12m),
		DividendMean12m:       nullFloat(mean12m),
		DividendPrevMean11m:   nullFloat(prevMean11m),
		DividendFirstHalfMean: nullFloat(firstHalfMean12m),
		DividendLastHalfMean:  nullFloat(lastHalfMean12m),
		DividendMax12m:        nullFloat(max12m),
		DividendMin12m:        nullFloat(min12m),
		DividendLastValue:     nullFloat(lastValue),
		DrawdownMax:           nullFloat(drawdownMax),
		RecoveryTimeDays:      int(recoveryDays.Int64),
		VolAnnual:             nullFloat(volAnnual),
		Sharpe:                nullFloat(sharpe),
		LiqMean:               nullFloat(liqMean),
		PctDaysTraded:         nullFloat(pctDaysTraded),
		PriceLast3dReturn:     nullFloat(last3dReturn),
		TodayReturn:           nullFloat(todayReturn),
	}
	return out, true, nil
}

func (s *Service) ListFundMetricsLatest(ctx context.Context, codes []string) ([]FundMetricsLatest, error) {
	if len(codes) == 0 {
		return []FundMetricsLatest{}, nil
	}

	rows, err := s.DB.QueryContext(ctx, `
		SELECT
			fund_code,
			computed_at,
			as_of_date,
			pvp_current,
			pvp_percentile,
			dy_monthly_mean,
			dividend_cv,
			dividend_trend_slope,
			dividend_paid_months_12m,
			dividend_regularity_12m,
			dividend_mean_12m,
			dividend_prev_mean_11m,
			dividend_first_half_mean_12m,
			dividend_last_half_mean_12m,
			dividend_max_12m,
			dividend_min_12m,
			dividend_last_value,
			drawdown_max,
			recovery_time_days,
			vol_annual,
			sharpe,
			liq_mean,
			pct_days_traded,
			price_last3d_return,
			today_return
		FROM fund_metrics_latest
		WHERE fund_code = ANY($1)
	`, pq.Array(codes))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]FundMetricsLatest, 0, len(codes))
	for rows.Next() {
		var (
			fundCode           string
			computedAt         time.Time
			asOfDate           time.Time
			pvpCurrent         sql.NullFloat64
			pvpPercentile      sql.NullFloat64
			dyMonthlyMean      sql.NullFloat64
			dividendCV         sql.NullFloat64
			dividendTrendSlope sql.NullFloat64
			paidMonths12m      sql.NullInt64
			regularity12m      sql.NullFloat64
			mean12m            sql.NullFloat64
			prevMean11m        sql.NullFloat64
			firstHalfMean12m   sql.NullFloat64
			lastHalfMean12m    sql.NullFloat64
			max12m             sql.NullFloat64
			min12m             sql.NullFloat64
			lastValue          sql.NullFloat64
			drawdownMax        sql.NullFloat64
			recoveryDays       sql.NullInt64
			volAnnual          sql.NullFloat64
			sharpe             sql.NullFloat64
			liqMean            sql.NullFloat64
			pctDaysTraded      sql.NullFloat64
			last3dReturn       sql.NullFloat64
			todayReturn        sql.NullFloat64
		)
		if err := rows.Scan(
			&fundCode,
			&computedAt,
			&asOfDate,
			&pvpCurrent,
			&pvpPercentile,
			&dyMonthlyMean,
			&dividendCV,
			&dividendTrendSlope,
			&paidMonths12m,
			&regularity12m,
			&mean12m,
			&prevMean11m,
			&firstHalfMean12m,
			&lastHalfMean12m,
			&max12m,
			&min12m,
			&lastValue,
			&drawdownMax,
			&recoveryDays,
			&volAnnual,
			&sharpe,
			&liqMean,
			&pctDaysTraded,
			&last3dReturn,
			&todayReturn,
		); err != nil {
			return nil, err
		}

		out = append(out, FundMetricsLatest{
			FundCode:              strings.ToUpper(strings.TrimSpace(fundCode)),
			ComputedAt:            computedAt.UTC(),
			AsOfDateISO:           asOfDate.UTC().Format("2006-01-02"),
			PVPCurrent:            nullFloat(pvpCurrent),
			PVPPercentile:         nullFloat(pvpPercentile),
			DYMonthlyMean:         nullFloat(dyMonthlyMean),
			DividendCV:            nullFloat(dividendCV),
			DividendTrendSlope:    nullFloat(dividendTrendSlope),
			DividendPaidMonths12m: int(paidMonths12m.Int64),
			DividendRegularity12m: nullFloat(regularity12m),
			DividendMean12m:       nullFloat(mean12m),
			DividendPrevMean11m:   nullFloat(prevMean11m),
			DividendFirstHalfMean: nullFloat(firstHalfMean12m),
			DividendLastHalfMean:  nullFloat(lastHalfMean12m),
			DividendMax12m:        nullFloat(max12m),
			DividendMin12m:        nullFloat(min12m),
			DividendLastValue:     nullFloat(lastValue),
			DrawdownMax:           nullFloat(drawdownMax),
			RecoveryTimeDays:      int(recoveryDays.Int64),
			VolAnnual:             nullFloat(volAnnual),
			Sharpe:                nullFloat(sharpe),
			LiqMean:               nullFloat(liqMean),
			PctDaysTraded:         nullFloat(pctDaysTraded),
			PriceLast3dReturn:     nullFloat(last3dReturn),
			TodayReturn:           nullFloat(todayReturn),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

type RankHojeSource struct {
	Code                string
	PVPCurrent          float64
	DYMonthlyMean       float64
	Sharpe              float64
	TodayReturn         float64
	PriceLast3dReturn   float64
	Vacancia            float64
	VacanciaValid       bool
	DailyLiquidity      float64
	DailyLiquidityValid bool
}

func (s *Service) ListRankHojeSources(ctx context.Context, codes []string) ([]RankHojeSource, error) {
	if len(codes) == 0 {
		return []RankHojeSource{}, nil
	}

	rows, err := s.DB.QueryContext(ctx, `
		WITH m AS (
			SELECT
				fund_code,
				COALESCE(pvp_current, 0) AS pvp_current,
				COALESCE(dy_monthly_mean, 0) AS dy_monthly_mean,
				COALESCE(sharpe, 0) AS sharpe,
				COALESCE(today_return, 0) AS today_return,
				COALESCE(price_last3d_return, 0) AS price_last3d_return
			FROM fund_metrics_latest
			WHERE fund_code = ANY($1)
		)
		SELECT
			f.code,
			m.pvp_current,
			m.dy_monthly_mean,
			m.sharpe,
			m.today_return,
			m.price_last3d_return,
			f.vacancia,
			f.daily_liquidity
		FROM m
		JOIN fund_master f ON f.code = m.fund_code
		WHERE
			m.pvp_current > 0
			AND m.dy_monthly_mean > 0
		ORDER BY m.today_return DESC
		LIMIT 20
	`, pq.Array(codes))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]RankHojeSource, 0, 20)
	for rows.Next() {
		var (
			code          string
			pvpCurrent    float64
			dyMonthlyMean float64
			sharpe        float64
			todayReturn   float64
			last3dReturn  float64
			vacancia      sql.NullFloat64
			daily         sql.NullFloat64
		)
		if err := rows.Scan(
			&code,
			&pvpCurrent,
			&dyMonthlyMean,
			&sharpe,
			&todayReturn,
			&last3dReturn,
			&vacancia,
			&daily,
		); err != nil {
			return nil, err
		}

		out = append(out, RankHojeSource{
			Code:                strings.ToUpper(strings.TrimSpace(code)),
			PVPCurrent:          pvpCurrent,
			DYMonthlyMean:       dyMonthlyMean,
			Sharpe:              sharpe,
			TodayReturn:         todayReturn,
			PriceLast3dReturn:   last3dReturn,
			Vacancia:            nullFloat(vacancia),
			VacanciaValid:       vacancia.Valid,
			DailyLiquidity:      nullFloat(daily),
			DailyLiquidityValid: daily.Valid,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

type RankVCandidate struct {
	Code                  string
	PVPCurrent            float64
	DYMonthlyMean         float64
	TodayReturn           float64
	DividendRegularity12m float64
	DividendMean12m       float64
	DividendPrevMean11m   float64
	DividendFirstHalfMean float64
	DividendLastHalfMean  float64
	DividendMax12m        float64
	DividendMin12m        float64
	DividendLastValue     float64
}

func (s *Service) ListRankVCandidates(ctx context.Context, codes []string) ([]RankVCandidate, error) {
	if len(codes) == 0 {
		return []RankVCandidate{}, nil
	}

	rows, err := s.DB.QueryContext(ctx, `
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
	`, pq.Array(codes))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]RankVCandidate, 0, 20)
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
			return nil, err
		}
		out = append(out, RankVCandidate{
			Code:                  strings.ToUpper(strings.TrimSpace(code)),
			PVPCurrent:            pvp,
			DYMonthlyMean:         dyMonthly,
			TodayReturn:           todayReturn,
			DividendRegularity12m: regularity12m,
			DividendMean12m:       dividendMean12m,
			DividendPrevMean11m:   dividendPrevMean11m,
			DividendFirstHalfMean: dividendFirstHalfMean,
			DividendLastHalfMean:  dividendLastHalfMean,
			DividendMax12m:        dividendMax12m,
			DividendMin12m:        dividendMin12m,
			DividendLastValue:     dividendLastValue,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

type CotationStats struct {
	AsOfISO      string
	LastPrice    float64
	Ret7         *float64
	Ret30        *float64
	Ret90        *float64
	DrawdownMax  *float64
	VolAnnual30d *float64
	VolAnnual90d *float64
}

func (s *Service) GetCotationStats(ctx context.Context, fundCode string) (*CotationStats, bool, error) {
	code := strings.ToUpper(strings.TrimSpace(fundCode))
	if code == "" {
		return nil, false, nil
	}

	var (
		asOf         sql.NullString
		lastPrice    sql.NullFloat64
		ret7         sql.NullFloat64
		ret30        sql.NullFloat64
		ret90        sql.NullFloat64
		drawdownMax  sql.NullFloat64
		volAnnual30d sql.NullFloat64
		volAnnual90d sql.NullFloat64
	)

	if err := s.DB.QueryRowContext(ctx, `
		WITH
		prices AS (
			SELECT date_iso, price_int
			FROM cotation
			WHERE fund_code = $1
			ORDER BY date_iso DESC
			LIMIT 91
		),
		ordered AS (
			SELECT date_iso, price_int, ROW_NUMBER() OVER (ORDER BY date_iso DESC) AS rn
			FROM prices
		),
		vol30 AS (
			SELECT sqrt(252) * stddev_samp(r) AS v
			FROM (
				SELECT (price_int::double precision / NULLIF(lag(price_int) OVER (ORDER BY date_iso), 0)::double precision) - 1 AS r
				FROM (
					SELECT date_iso, price_int
					FROM prices
					ORDER BY date_iso DESC
					LIMIT 31
				) p
				ORDER BY date_iso
			) x
			WHERE r IS NOT NULL
		),
		vol90 AS (
			SELECT sqrt(252) * stddev_samp(r) AS v
			FROM (
				SELECT (price_int::double precision / NULLIF(lag(price_int) OVER (ORDER BY date_iso), 0)::double precision) - 1 AS r
				FROM (
					SELECT date_iso, price_int
					FROM prices
					ORDER BY date_iso DESC
					LIMIT 91
				) p
				ORDER BY date_iso
			) x
			WHERE r IS NOT NULL
		)
		SELECT
			MAX(CASE WHEN rn = 1 THEN date_iso END)::text AS as_of,
			MAX(CASE WHEN rn = 1 THEN price_int END)::double precision / 10000.0 AS last_price,
			CASE
				WHEN MAX(CASE WHEN rn = 8 THEN price_int END) IS NOT NULL AND MAX(CASE WHEN rn = 8 THEN price_int END) > 0
					THEN (MAX(CASE WHEN rn = 1 THEN price_int END)::double precision / MAX(CASE WHEN rn = 8 THEN price_int END)::double precision) - 1
				ELSE NULL
			END AS ret_7,
			CASE
				WHEN MAX(CASE WHEN rn = 31 THEN price_int END) IS NOT NULL AND MAX(CASE WHEN rn = 31 THEN price_int END) > 0
					THEN (MAX(CASE WHEN rn = 1 THEN price_int END)::double precision / MAX(CASE WHEN rn = 31 THEN price_int END)::double precision) - 1
				ELSE NULL
			END AS ret_30,
			CASE
				WHEN MAX(CASE WHEN rn = 91 THEN price_int END) IS NOT NULL AND MAX(CASE WHEN rn = 91 THEN price_int END) > 0
					THEN (MAX(CASE WHEN rn = 1 THEN price_int END)::double precision / MAX(CASE WHEN rn = 91 THEN price_int END)::double precision) - 1
				ELSE NULL
			END AS ret_90,
			(SELECT drawdown_max::double precision FROM fund_metrics_latest WHERE fund_code = $1) AS drawdown_max,
			(SELECT v FROM vol30) AS vol_30,
			(SELECT v FROM vol90) AS vol_90
		FROM ordered
	`, code).Scan(&asOf, &lastPrice, &ret7, &ret30, &ret90, &drawdownMax, &volAnnual30d, &volAnnual90d); err != nil {
		return nil, false, err
	}

	if !asOf.Valid || !lastPrice.Valid || lastPrice.Float64 <= 0 {
		return nil, false, nil
	}

	toPtr := func(v sql.NullFloat64) *float64 {
		if !v.Valid {
			return nil
		}
		x := v.Float64
		return &x
	}

	return &CotationStats{
		AsOfISO:      asOf.String,
		LastPrice:    lastPrice.Float64,
		Ret7:         toPtr(ret7),
		Ret30:        toPtr(ret30),
		Ret90:        toPtr(ret90),
		DrawdownMax:  toPtr(drawdownMax),
		VolAnnual30d: toPtr(volAnnual30d),
		VolAnnual90d: toPtr(volAnnual90d),
	}, true, nil
}

var fiiCodeRe = regexp.MustCompile(`^[A-Za-z]{4}11$`)
var isoDateRe = regexp.MustCompile(`^(\d{4})-(\d{2})-(\d{2})`)

func ValidateFundCode(raw string) (string, bool) {
	v := strings.TrimSpace(raw)
	if v == "" || !fiiCodeRe.MatchString(v) {
		return "", false
	}
	return strings.ToUpper(v), true
}

func toDateBrFromIso(dateIso string) string {
	v := strings.TrimSpace(dateIso)
	if v == "" {
		return ""
	}
	t, err := time.Parse("2006-01-02", v)
	if err != nil {
		return ""
	}
	return t.Format("02/01/2006")
}

func (s *Service) ListFunds(ctx context.Context) (model.FundListResponse, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT code, sector, p_vp, dividend_yield, dividend_yield_last_5_years, daily_liquidity, net_worth, type
		FROM fund_master
		ORDER BY code ASC
	`)
	if err != nil {
		return model.FundListResponse{}, err
	}
	defer rows.Close()

	out := model.FundListResponse{}
	for rows.Next() {
		var (
			code                  string
			sector, typ           sql.NullString
			pvp, dy, dy5, liq, nw sql.NullFloat64
		)
		if err := rows.Scan(&code, &sector, &pvp, &dy, &dy5, &liq, &nw, &typ); err != nil {
			return model.FundListResponse{}, err
		}
		out.Data = append(out.Data, model.FundListItem{
			Code:                  strings.ToUpper(strings.TrimSpace(code)),
			Sector:                nullString(sector),
			PVP:                   nullFloat(pvp),
			DividendYield:         nullFloat(dy),
			DividendYieldLast5Yrs: nullFloat(dy5),
			DailyLiquidity:        nullFloat(liq),
			NetWorth:              nullFloat(nw),
			Type:                  nullString(typ),
		})
	}
	out.Total = len(out.Data)
	return out, rows.Err()
}

func nullString(v sql.NullString) string {
	if !v.Valid {
		return ""
	}
	return v.String
}

func nullFloat(v sql.NullFloat64) float64 {
	if !v.Valid {
		return 0
	}
	return v.Float64
}

func (s *Service) GetFundDetails(ctx context.Context, code string) (*model.FundDetails, error) {
	var (
		id                                                                          sql.NullString
		rowCode                                                                     string
		razao, cnpj, publico, mandato, segmento, tipoFundo, prazo, tipoGestao, taxa sql.NullString
		daily                                                                       sql.NullFloat64
		vac, cotistas, emitidas, vpc, vp, ultimo                                    sql.NullFloat64
	)

	err := s.DB.QueryRowContext(ctx, `
		SELECT id, code, razao_social, cnpj, publico_alvo, mandato, segmento, tipo_fundo, prazo_duracao, tipo_gestao,
		       taxa_adminstracao, daily_liquidity, vacancia, numero_cotistas, cotas_emitidas, valor_patrimonial_cota,
		       valor_patrimonial, ultimo_rendimento
		FROM fund_master
		WHERE code = $1
		LIMIT 1
	`, code).Scan(
		&id, &rowCode, &razao, &cnpj, &publico, &mandato, &segmento, &tipoFundo, &prazo, &tipoGestao,
		&taxa, &daily, &vac, &cotistas, &emitidas, &vpc, &vp, &ultimo,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !id.Valid || strings.TrimSpace(id.String) == "" || !cnpj.Valid || strings.TrimSpace(cnpj.String) == "" {
		return nil, nil
	}

	var dailyPtr *float64
	if daily.Valid {
		v := daily.Float64
		dailyPtr = &v
	}

	var vacPtr *float64
	if vac.Valid {
		v := vac.Float64
		vacPtr = &v
	}

	return &model.FundDetails{
		ID:                   id.String,
		Code:                 strings.ToUpper(strings.TrimSpace(rowCode)),
		RazaoSocial:          nullString(razao),
		CNPJ:                 nullString(cnpj),
		PublicoAlvo:          nullString(publico),
		Mandato:              nullString(mandato),
		Segmento:             nullString(segmento),
		TipoFundo:            nullString(tipoFundo),
		PrazoDuracao:         nullString(prazo),
		TipoGestao:           nullString(tipoGestao),
		TaxaAdminstracao:     nullString(taxa),
		DailyLiquidity:       dailyPtr,
		Vacancia:             vacPtr,
		NumeroCotistas:       nullFloat(cotistas),
		CotasEmitidas:        nullFloat(emitidas),
		ValorPatrimonialCota: nullFloat(vpc),
		ValorPatrimonial:     nullFloat(vp),
		UltimoRendimento:     nullFloat(ultimo),
	}, nil
}

func (s *Service) GetLatestIndicators(ctx context.Context, code string) (model.NormalizedIndicators, bool, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT
			ano,
			cotas_emitidas,
			numero_de_cotistas,
			vacancia,
			valor_patrimonial_cota,
			valor_patrimonial,
			liquidez_diaria,
			dividend_yield,
			pvp,
			valor_mercado
		FROM indicators_snapshot
		WHERE fund_code = $1
		ORDER BY (CASE WHEN ano = 0 THEN 32767 ELSE ano END) ASC
	`, code)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()

	nullFloatPtr := func(v sql.NullFloat64) *float64 {
		if !v.Valid {
			return nil
		}
		x := v.Float64
		return &x
	}

	out := make(model.NormalizedIndicators, 9)
	currentYear := int16(time.Now().In(time.Local).Year())
	seenYears := map[int16]struct{}{}
	rowCount := 0
	for rows.Next() {
		rowCount++
		var (
			ano              int16
			cotasEmitidas    sql.NullFloat64
			numeroDeCotistas sql.NullFloat64
			vacancia         sql.NullFloat64
			valorPatCota     sql.NullFloat64
			valorPat         sql.NullFloat64
			liquidezDiaria   sql.NullFloat64
			dividendYield    sql.NullFloat64
			pvp              sql.NullFloat64
			valorMercado     sql.NullFloat64
		)
		if err := rows.Scan(
			&ano,
			&cotasEmitidas,
			&numeroDeCotistas,
			&vacancia,
			&valorPatCota,
			&valorPat,
			&liquidezDiaria,
			&dividendYield,
			&pvp,
			&valorMercado,
		); err != nil {
			return nil, false, err
		}

		if ano == 0 {
			ano = currentYear
		}
		if _, ok := seenYears[ano]; ok {
			continue
		}
		seenYears[ano] = struct{}{}

		year := strconv.Itoa(int(ano))

		out["cotas_emitidas"] = append(out["cotas_emitidas"], model.IndicatorItem{Year: year, Value: nullFloatPtr(cotasEmitidas)})
		out["numero_de_cotistas"] = append(out["numero_de_cotistas"], model.IndicatorItem{Year: year, Value: nullFloatPtr(numeroDeCotistas)})
		out["vacancia"] = append(out["vacancia"], model.IndicatorItem{Year: year, Value: nullFloatPtr(vacancia)})
		out["valor_patrimonial_cota"] = append(out["valor_patrimonial_cota"], model.IndicatorItem{Year: year, Value: nullFloatPtr(valorPatCota)})
		out["valor_patrimonial"] = append(out["valor_patrimonial"], model.IndicatorItem{Year: year, Value: nullFloatPtr(valorPat)})
		out["liquidez_diaria"] = append(out["liquidez_diaria"], model.IndicatorItem{Year: year, Value: nullFloatPtr(liquidezDiaria)})
		out["dividend_yield"] = append(out["dividend_yield"], model.IndicatorItem{Year: year, Value: nullFloatPtr(dividendYield)})
		out["pvp"] = append(out["pvp"], model.IndicatorItem{Year: year, Value: nullFloatPtr(pvp)})
		out["valor_mercado"] = append(out["valor_mercado"], model.IndicatorItem{Year: year, Value: nullFloatPtr(valorMercado)})
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	if rowCount == 0 {
		return nil, false, nil
	}

	return out, true, nil
}

func (s *Service) GetCotations(ctx context.Context, code string, days int) (*model.NormalizedCotations, error) {
	limit := 1825
	if days > 0 {
		if days > 5000 {
			days = 5000
		}
		limit = days
	}

	rows, err := s.DB.QueryContext(ctx, `
		SELECT date_iso, price_int
		FROM cotation
		WHERE fund_code = $1
		ORDER BY date_iso DESC
		LIMIT $2
	`, code, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type row struct {
		dateIso string
		price   int
	}
	var all []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.dateIso, &r.price); err != nil {
			return nil, err
		}
		all = append(all, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(all) == 0 {
		return nil, nil
	}

	real := make([]model.CotationItem, 0, len(all))
	for i := len(all) - 1; i >= 0; i-- {
		real = append(real, model.CotationItem{
			Date:  toDateBrFromIso(all[i].dateIso),
			Price: fromPriceInt(all[i].price),
		})
	}

	return &model.NormalizedCotations{Real: real, Dolar: []model.CotationItem{}, Euro: []model.CotationItem{}}, nil
}

func dividendTypeFromCode(code int) (model.DividendType, bool) {
	if code == 1 {
		return model.Dividendos, true
	}
	if code == 2 {
		return model.Amortizacao, true
	}
	return "", false
}

func (s *Service) GetDividends(ctx context.Context, code string) ([]model.DividendData, bool, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT date_iso, payment, type, value, yield
		FROM dividend
		WHERE fund_code = $1
		ORDER BY date_iso DESC
	`, code)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()

	var out []model.DividendData
	for rows.Next() {
		var (
			dateIso    string
			paymentIso string
			typeCode   int
			value      float64
			yield      float64
		)
		if err := rows.Scan(&dateIso, &paymentIso, &typeCode, &value, &yield); err != nil {
			return nil, false, err
		}
		t, ok := dividendTypeFromCode(typeCode)
		if !ok {
			continue
		}
		out = append(out, model.DividendData{
			Value:   value,
			Yield:   yield,
			Date:    toDateBrFromIso(dateIso),
			Payment: toDateBrFromIso(paymentIso),
			Type:    t,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	if len(out) == 0 {
		return nil, false, nil
	}
	return out, true, nil
}

func (s *Service) GetLatestCotationsToday(ctx context.Context, code string) ([]model.CotationTodayItem, bool, error) {
	var latestDate time.Time
	err := s.DB.QueryRowContext(ctx, `
		SELECT date_iso
		FROM cotation_today
		WHERE fund_code = $1
		ORDER BY date_iso DESC
		LIMIT 1
	`, code).Scan(&latestDate)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}

	rows, err := s.DB.QueryContext(ctx, `
		SELECT to_char(hour, 'HH24:MI') as hour, price_int
		FROM cotation_today
		WHERE fund_code = $1 AND date_iso = $2
		ORDER BY hour ASC
	`, code, latestDate.Format("2006-01-02"))
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()

	out := []model.CotationTodayItem{}
	for rows.Next() {
		var (
			hour     string
			priceInt int
		)
		if err := rows.Scan(&hour, &priceInt); err != nil {
			return nil, false, err
		}
		out = append(out, model.CotationTodayItem{Hour: hour, Price: fromPriceInt(priceInt)})
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}

	if len(out) == 0 {
		return nil, false, nil
	}

	return out, true, nil
}

func (s *Service) GetDocuments(ctx context.Context, code string) ([]model.DocumentData, bool, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT
			document_id,
			title,
			category,
			type,
			to_char(date AT TIME ZONE 'UTC', 'DD/MM/YYYY') as date,
			to_char("dateUpload" AT TIME ZONE 'UTC', 'DD/MM/YYYY') as "dateUpload",
			url,
			status,
			version
		FROM document
		WHERE fund_code = $1
		ORDER BY document."dateUpload" DESC, document_id DESC
	`, code)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()

	var out []model.DocumentData
	for rows.Next() {
		var d model.DocumentData
		if err := rows.Scan(&d.ID, &d.Title, &d.Category, &d.Type, &d.Date, &d.DateUpload, &d.URL, &d.Status, &d.Version); err != nil {
			return nil, false, err
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	if len(out) == 0 {
		return nil, false, nil
	}
	return out, true, nil
}
