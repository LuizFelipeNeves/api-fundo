package fii

import "github.com/luizfelipeneves/api-fundo/go-api/internal/model"

type IndicatorsSnapshot struct {
	FetchedAt string                     `json:"fetched_at"`
	Data      model.NormalizedIndicators `json:"data"`
}

type ExportFundJSON struct {
	GeneratedAt string             `json:"generated_at"`
	Fund        *model.FundDetails `json:"fund"`
	Period      ExportFundPeriod   `json:"period"`
	Data        ExportFundData     `json:"data"`
	Metrics     ExportFundMetrics  `json:"metrics"`
}

type ExportFundPeriod struct {
	Start              string `json:"start"`
	End                string `json:"end"`
	CotationsDaysLimit int    `json:"cotations_days_limit"`
}

type ExportFundData struct {
	Cotations        []model.CotationItem      `json:"cotations"`
	Dividends        []model.DividendData      `json:"dividends"`
	IndicatorsLatest any                       `json:"indicators_latest"`
	CotationsToday   []model.CotationTodayItem `json:"cotations_today"`
}

type ExportFundMetrics struct {
	Price         ExportFundMetricsPrice         `json:"price"`
	Dividends     ExportFundMetricsDividends     `json:"dividends"`
	DividendYield ExportFundMetricsDividendYield `json:"dividend_yield"`
	Valuation     ExportFundMetricsValuation     `json:"valuation"`
	Liquidity     ExportFundMetricsLiquidity     `json:"liquidity"`
	Risk          ExportFundMetricsRisk          `json:"risk"`
	Structure     ExportFundMetricsStructure     `json:"structure"`
	Consistency   ExportFundMetricsConsistency   `json:"consistency"`
	Quality       ExportFundMetricsQuality       `json:"quality"`
	Today         ExportFundMetricsToday         `json:"today"`
}

type ExportFundMetricsPrice struct {
	Initial               float64 `json:"initial"`
	Final                 float64 `json:"final"`
	Min                   float64 `json:"min"`
	Max                   float64 `json:"max"`
	Mean                  float64 `json:"mean"`
	MeanDailyReturn       float64 `json:"mean_daily_return"`
	SimpleReturn          float64 `json:"simple_return"`
	CumulativeReturn      float64 `json:"cumulative_return"`
	CagrAnnualized        float64 `json:"cagr_annualized"`
	Last3dReturn          float64 `json:"last_3d_return"`
	AvgMonthlyReturn      float64 `json:"avg_monthly_return"`
	Volatility            float64 `json:"volatility"`
	VolatilityAnnualized  float64 `json:"volatility_annualized"`
	DownsideVolatility    float64 `json:"downside_volatility"`
	DownsideVolAnnualized float64 `json:"downside_volatility_annualized"`
	DrawdownMax           float64 `json:"drawdown_max"`
	DrawdownDurationDays  int     `json:"drawdown_duration_days"`
	RecoveryTimeDays      int     `json:"recovery_time_days"`
	MaxDailyDrop          float64 `json:"max_daily_drop"`
	MaxDailyGain          float64 `json:"max_daily_gain"`
	PositiveDays          int     `json:"positive_days"`
	NegativeDays          int     `json:"negative_days"`
	PctPositiveDays       float64 `json:"pct_positive_days"`
	VariationAmplitude    float64 `json:"variation_amplitude"`
}

type ExportFundMetricsDividends struct {
	Total                float64 `json:"total"`
	Payments             int     `json:"payments"`
	Mean                 float64 `json:"mean"`
	Median               float64 `json:"median"`
	Max                  float64 `json:"max"`
	Min                  float64 `json:"min"`
	Stdev                float64 `json:"stdev"`
	CV                   float64 `json:"cv"`
	MonthsWithPayment    int     `json:"months_with_payment"`
	MonthsWithoutPayment int     `json:"months_without_payment"`
	Regularity           float64 `json:"regularity"`
	AvgIntervalDays      int     `json:"avg_interval_days"`
	TrendSlope           float64 `json:"trend_slope"`
}

type ExportFundMetricsDividendYield struct {
	Period      float64 `json:"period"`
	MonthlyMean float64 `json:"monthly_mean"`
	Annualized  float64 `json:"annualized"`
}

type ExportFundMetricsValuation struct {
	PVPCurrent    float64 `json:"pvp_current"`
	PVPMean       float64 `json:"pvp_mean"`
	PVPMin        float64 `json:"pvp_min"`
	PVPMax        float64 `json:"pvp_max"`
	PVPStdev      float64 `json:"pvp_stdev"`
	PVPPercentile float64 `json:"pvp_percentile"`
	PctDaysPVPGt1 float64 `json:"pct_days_pvp_gt_1"`
	PVPAmplitude  float64 `json:"pvp_amplitude"`
}

type ExportFundMetricsLiquidity struct {
	Mean          float64 `json:"mean"`
	Min           float64 `json:"min"`
	Max           float64 `json:"max"`
	ZeroDays      int     `json:"zero_days"`
	PctDaysTraded float64 `json:"pct_days_traded"`
	TrendSlope    float64 `json:"trend_slope"`
}

type ExportFundMetricsRisk struct {
	Volatility            float64 `json:"volatility"`
	VolatilityAnnualized  float64 `json:"volatility_annualized"`
	DownsideVolatility    float64 `json:"downside_volatility"`
	DownsideVolAnnualized float64 `json:"downside_volatility_annualized"`
	DrawdownMax           float64 `json:"drawdown_max"`
	DrawdownDurationDays  int     `json:"drawdown_duration_days"`
	RecoveryTimeDays      int     `json:"recovery_time_days"`
	Var95                 float64 `json:"var_95"`
	Sharpe                float64 `json:"sharpe"`
	Sortino               float64 `json:"sortino"`
	Calmar                float64 `json:"calmar"`
}

type ExportFundMetricsStructure struct {
	NetWorthSeriesPoints int                        `json:"net_worth_series_points"`
	NetWorthSeries       []ExportFundSeriesPoint    `json:"net_worth_series"`
	NetWorthGrowth12m    float64                    `json:"net_worth_growth_12m"`
	NetWorthGrowth3m     float64                    `json:"net_worth_growth_3m"`
	NetWorthMin          float64                    `json:"net_worth_min"`
	NetWorthMax          float64                    `json:"net_worth_max"`
	NetWorthVolatility   float64                    `json:"net_worth_volatility"`
	CotistasSeriesPoints int                        `json:"cotistas_series_points"`
	CotistasSeries       []ExportFundSeriesPointInt `json:"cotistas_series"`
	CotistasGrowth12m    float64                    `json:"cotistas_growth_12m"`
}

type ExportFundSeriesPoint struct {
	At    string  `json:"at"`
	Value float64 `json:"value"`
}

type ExportFundSeriesPointInt struct {
	At    string `json:"at"`
	Value int    `json:"value"`
}

type ExportFundMetricsConsistency struct {
	MaxConsecutiveMonthsPaid   int     `json:"max_consecutive_months_paid"`
	MaxConsecutiveMonthsUnpaid int     `json:"max_consecutive_months_unpaid"`
	PctMonthsWithHistory       float64 `json:"pct_months_with_history"`
	FundAgeDays                int     `json:"fund_age_days"`
}

type ExportFundMetricsQuality struct {
	ScoreStability   float64 `json:"score_stability"`
	ScoreVolatility  float64 `json:"score_volatility"`
	ScoreLiquidity   float64 `json:"score_liquidity"`
	ScoreConsistency float64 `json:"score_consistency"`
	ScoreComposite   float64 `json:"score_composite"`
}

type ExportFundMetricsToday struct {
	First              float64 `json:"first"`
	Last               float64 `json:"last"`
	Min                float64 `json:"min"`
	Max                float64 `json:"max"`
	Return             float64 `json:"return"`
	Direction          string  `json:"direction"`
	Ticks              int     `json:"ticks"`
	MaxTickDrop        float64 `json:"max_tick_drop"`
	MaxTickGain        float64 `json:"max_tick_gain"`
	PositiveTicks      int     `json:"positive_ticks"`
	NegativeTicks      int     `json:"negative_ticks"`
	PctPositiveTicks   float64 `json:"pct_positive_ticks"`
	Volatility         float64 `json:"volatility"`
	VariationAmplitude float64 `json:"variation_amplitude"`
}

type ExportFundOptions struct {
	CotationsDays            int
	IndicatorsSnapshotsLimit int
}
