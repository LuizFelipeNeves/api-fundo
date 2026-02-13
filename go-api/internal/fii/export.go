package fii

import (
	"context"
	"encoding/json"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/luizfelipeneves/api-fundo/go-api/internal/model"
)

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

	out := buildExportFundJSON(details, cotations, dividends, snapshots, today, cotDays)
	return &out, true, nil
}

func (s *Service) GetLatestIndicatorsSnapshots(ctx context.Context, code string, limit int) ([]IndicatorsSnapshot, error) {
	safe := clampInt(limit, 365, 1, 5000)

	rows, err := s.DB.QueryContext(ctx, `
		SELECT fetched_at, data_json
		FROM indicators_snapshot
		WHERE fund_code = $1
		ORDER BY fetched_at DESC
		LIMIT $2
	`, code, safe)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]IndicatorsSnapshot, 0, safe)
	for rows.Next() {
		var (
			fetched time.Time
			raw     []byte
		)
		if err := rows.Scan(&fetched, &raw); err != nil {
			return nil, err
		}
		var parsed model.NormalizedIndicators
		if err := json.Unmarshal(raw, &parsed); err != nil {
			continue
		}
		out = append(out, IndicatorsSnapshot{
			FetchedAt: fetched.UTC().Format("2006-01-02T15:04:05.000Z"),
			Data:      parsed,
		})
	}
	return out, rows.Err()
}

var brDateRe = regexp.MustCompile(`^(\d{2})/(\d{2})/(\d{4})$`)

func toDateIsoFromBr(dateBr string) string {
	m := brDateRe.FindStringSubmatch(strings.TrimSpace(dateBr))
	if len(m) != 4 {
		return ""
	}
	return m[3] + "-" + m[2] + "-" + m[1]
}

func toMonthKeyFromBr(dateBr string) string {
	iso := toDateIsoFromBr(dateBr)
	if len(iso) < 7 {
		return ""
	}
	return iso[:7]
}

func monthKeyToParts(monthKey string) (int, int, bool) {
	parts := strings.Split(strings.TrimSpace(monthKey), "-")
	if len(parts) != 2 {
		return 0, 0, false
	}
	y, err1 := strconv.Atoi(parts[0])
	m, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil || m < 1 || m > 12 {
		return 0, 0, false
	}
	return y, m, true
}

func monthKeyDiff(a string, b string) int {
	ay, am, okA := monthKeyToParts(a)
	by, bm, okB := monthKeyToParts(b)
	if !okA || !okB {
		return 0
	}
	return (by-ay)*12 + (bm - am)
}

func monthKeyAdd(monthKey string, deltaMonths int) string {
	y, m, ok := monthKeyToParts(monthKey)
	if !ok {
		return ""
	}
	base := y*12 + (m - 1)
	next := base + deltaMonths
	yy := next / 12
	mm := (next % 12) + 1
	return leftPad4(yy) + "-" + leftPad2(mm)
}

func leftPad2(v int) string {
	if v < 10 {
		return "0" + strconv.Itoa(v)
	}
	return strconv.Itoa(v)
}

func leftPad4(v int) string {
	if v < 10 {
		return "000" + strconv.Itoa(v)
	}
	if v < 100 {
		return "00" + strconv.Itoa(v)
	}
	if v < 1000 {
		return "0" + strconv.Itoa(v)
	}
	return strconv.Itoa(v)
}

func listMonthKeysBetweenInclusive(startKey string, endKey string) []string {
	diff := monthKeyDiff(startKey, endKey)
	if diff < 0 {
		return []string{}
	}
	out := make([]string, 0, diff+1)
	for i := 0; i <= diff; i++ {
		k := monthKeyAdd(startKey, i)
		if k != "" {
			out = append(out, k)
		}
	}
	return out
}

func countWeekdaysBetweenIso(startIso string, endIso string) int {
	start, err1 := time.Parse("2006-01-02", strings.TrimSpace(startIso))
	end, err2 := time.Parse("2006-01-02", strings.TrimSpace(endIso))
	if err1 != nil || err2 != nil || end.Before(start) {
		return 0
	}

	count := 0
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		wd := d.Weekday()
		if wd >= time.Monday && wd <= time.Friday {
			count++
		}
	}
	return count
}

func buildExportFundJSON(
	details *model.FundDetails,
	cotations *model.NormalizedCotations,
	dividends []model.DividendData,
	indicatorSnapshots []IndicatorsSnapshot,
	cotationsToday []model.CotationTodayItem,
	cotationsDays int,
) ExportFundJSON {
	cotationItems := []model.CotationItem{}
	if cotations != nil {
		cotationItems = cotations.Real
	}

	cotationPrices := make([]float64, 0, len(cotationItems))
	cotationDatesIso := make([]string, 0, len(cotationItems))
	for _, it := range cotationItems {
		if isFiniteFloat(it.Price) && it.Price > 0 {
			cotationPrices = append(cotationPrices, it.Price)
		}
		if iso := toDateIsoFromBr(it.Date); iso != "" {
			cotationDatesIso = append(cotationDatesIso, iso)
		}
	}

	periodStart := ""
	periodEnd := ""
	if len(cotationItems) > 0 {
		periodStart = cotationItems[0].Date
		periodEnd = cotationItems[len(cotationItems)-1].Date
	}

	periodDays := 0
	if len(cotationDatesIso) >= 2 {
		a, err1 := time.Parse("2006-01-02", cotationDatesIso[0])
		b, err2 := time.Parse("2006-01-02", cotationDatesIso[len(cotationDatesIso)-1])
		if err1 == nil && err2 == nil && b.After(a) {
			periodDays = int(math.Round(b.Sub(a).Hours() / 24))
			if periodDays < 1 {
				periodDays = 1
			}
		}
	}

	dailyReturns := make([]float64, 0, len(cotationPrices))
	for i := 1; i < len(cotationPrices); i++ {
		prev := cotationPrices[i-1]
		cur := cotationPrices[i]
		if prev > 0 {
			dailyReturns = append(dailyReturns, cur/prev-1)
		}
	}

	meanDailyReturn := mean(dailyReturns)

	priceInitial := 0.0
	priceFinal := 0.0
	if len(cotationPrices) > 0 {
		priceInitial = cotationPrices[0]
		priceFinal = cotationPrices[len(cotationPrices)-1]
	}
	simpleReturn := 0.0
	if priceInitial > 0 {
		simpleReturn = priceFinal/priceInitial - 1
	}
	cumulativeReturn := 0.0
	if len(dailyReturns) > 0 {
		acc := 1.0
		for _, r := range dailyReturns {
			acc *= 1 + r
		}
		cumulativeReturn = acc - 1
	}
	cagrAnnualized := annualizeCagr(simpleReturn, periodDays)

	last3dReturn := 0.0
	if len(cotationPrices) >= 3 && cotationPrices[len(cotationPrices)-3] > 0 {
		last3dReturn = cotationPrices[len(cotationPrices)-1]/cotationPrices[len(cotationPrices)-3] - 1
	}

	priceMin := 0.0
	priceMax := 0.0
	if len(cotationPrices) > 0 {
		priceMin = cotationPrices[0]
		priceMax = cotationPrices[0]
		for _, p := range cotationPrices {
			if p < priceMin {
				priceMin = p
			}
			if p > priceMax {
				priceMax = p
			}
		}
	}
	priceMean := mean(cotationPrices)

	volatility := stdev(dailyReturns)
	downsideReturns := make([]float64, 0, len(dailyReturns))
	for _, r := range dailyReturns {
		if r < 0 {
			downsideReturns = append(downsideReturns, r)
		}
	}
	downsideVolatility := stdev(downsideReturns)
	volatilityAnnualized := annualizeVolatility(volatility, 252)
	downsideVolAnnualized := annualizeVolatility(downsideVolatility, 252)

	maxDown := 0.0
	maxUp := 0.0
	posDays := 0
	negDays := 0
	if len(dailyReturns) > 0 {
		maxDown = dailyReturns[0]
		maxUp = dailyReturns[0]
		for _, r := range dailyReturns {
			if r < maxDown {
				maxDown = r
			}
			if r > maxUp {
				maxUp = r
			}
			if r > 0 {
				posDays++
			} else if r < 0 {
				negDays++
			}
		}
	}
	pctPositiveDays := 0.0
	if len(dailyReturns) > 0 {
		pctPositiveDays = float64(posDays) / float64(len(dailyReturns))
	}
	variationAmplitude := 0.0
	if priceMin > 0 {
		variationAmplitude = priceMax/priceMin - 1
	}
	var95 := quantile(dailyReturns, 0.05)

	dd := computeDrawdown(cotationPrices)
	sharpe := sharpeRatio(meanDailyReturn, volatility, 252)
	sortino := sortinoRatio(meanDailyReturn, downsideVolatility, 252)
	calmar := calmarRatio(cagrAnnualized, dd.MaxDrawdown)

	monthLastPrice := map[string]float64{}
	for _, it := range cotationItems {
		mk := toMonthKeyFromBr(it.Date)
		if mk == "" {
			continue
		}
		monthLastPrice[mk] = it.Price
	}
	monthKeys := make([]string, 0, len(monthLastPrice))
	for k := range monthLastPrice {
		monthKeys = append(monthKeys, k)
	}
	sort.Strings(monthKeys)

	monthPrices := make([]float64, 0, len(monthKeys))
	for _, k := range monthKeys {
		p := monthLastPrice[k]
		if isFiniteFloat(p) && p > 0 {
			monthPrices = append(monthPrices, p)
		}
	}
	monthReturns := make([]float64, 0, len(monthPrices))
	for i := 1; i < len(monthPrices); i++ {
		prev := monthPrices[i-1]
		cur := monthPrices[i]
		if prev > 0 {
			monthReturns = append(monthReturns, cur/prev-1)
		}
	}
	avgMonthlyReturn := mean(monthReturns)

	dividendsInPeriod := dividends
	if len(cotationDatesIso) >= 2 {
		start := cotationDatesIso[0]
		end := cotationDatesIso[len(cotationDatesIso)-1]
		tmp := make([]model.DividendData, 0, len(dividends))
		for _, d := range dividends {
			iso := toDateIsoFromBr(d.Date)
			if iso == "" || iso < start || iso > end {
				continue
			}
			tmp = append(tmp, d)
		}
		dividendsInPeriod = tmp
	}

	dividendsOnly := make([]model.DividendData, 0, len(dividendsInPeriod))
	for _, d := range dividendsInPeriod {
		if d.Type != model.Dividendos {
			continue
		}
		if !isFiniteFloat(d.Value) || d.Value <= 0 {
			continue
		}
		dividendsOnly = append(dividendsOnly, d)
	}

	dividendValues := make([]float64, 0, len(dividendsOnly))
	for _, d := range dividendsOnly {
		dividendValues = append(dividendValues, d.Value)
	}
	dividendTotal := 0.0
	for _, v := range dividendValues {
		dividendTotal += v
	}
	dividendCount := len(dividendValues)
	dividendMean := mean(dividendValues)
	dividendMedian := median(dividendValues)
	dividendMax := 0.0
	dividendMin := 0.0
	if len(dividendValues) > 0 {
		dividendMax = dividendValues[0]
		dividendMin = dividendValues[0]
		for _, v := range dividendValues {
			if v > dividendMax {
				dividendMax = v
			}
			if v < dividendMin {
				dividendMin = v
			}
		}
	}
	dividendStd := stdev(dividendValues)
	dividendCv := 0.0
	if dividendMean > 0 {
		dividendCv = dividendStd / dividendMean
	}

	dividendByMonth := map[string]float64{}
	for _, d := range dividendsOnly {
		mk := toMonthKeyFromBr(d.Date)
		if mk == "" {
			continue
		}
		dividendByMonth[mk] += d.Value
	}

	firstMonth := ""
	lastMonth := ""
	if len(monthKeys) > 0 {
		firstMonth = monthKeys[0]
		lastMonth = monthKeys[len(monthKeys)-1]
	}
	allMonths := monthKeys
	if firstMonth != "" && lastMonth != "" {
		allMonths = listMonthKeysBetweenInclusive(firstMonth, lastMonth)
	}

	expectedMonths := len(allMonths)
	monthsWithPayment := 0
	for _, mk := range allMonths {
		if dividendByMonth[mk] > 0 {
			monthsWithPayment++
		}
	}
	monthsWithoutPayment := expectedMonths - monthsWithPayment
	if monthsWithoutPayment < 0 {
		monthsWithoutPayment = 0
	}
	regularity := 0.0
	if expectedMonths > 0 {
		regularity = float64(monthsWithPayment) / float64(expectedMonths)
	}

	dividendMonthlySeries := make([]xy, 0, len(allMonths))
	for idx, mk := range allMonths {
		dividendMonthlySeries = append(dividendMonthlySeries, xy{X: float64(idx), Y: dividendByMonth[mk]})
	}
	dividendTrendSlope := linearSlope(dividendMonthlySeries)

	paymentIso := make([]string, 0, len(dividendsOnly))
	for _, d := range dividendsOnly {
		if iso := toDateIsoFromBr(d.Payment); iso != "" {
			paymentIso = append(paymentIso, iso)
		}
	}
	sort.Strings(paymentIso)
	intervals := make([]float64, 0, len(paymentIso))
	for i := 1; i < len(paymentIso); i++ {
		a, err1 := time.Parse("2006-01-02", paymentIso[i-1])
		b, err2 := time.Parse("2006-01-02", paymentIso[i])
		if err1 != nil || err2 != nil || !b.After(a) {
			continue
		}
		intervals = append(intervals, math.Round(b.Sub(a).Hours()/24))
	}
	avgPaymentIntervalDays := int(math.Round(mean(intervals)))

	dyByMonth := make([]float64, 0, len(monthKeys))
	for _, mk := range monthKeys {
		div := dividendByMonth[mk]
		price := monthLastPrice[mk]
		if div > 0 && price > 0 {
			dyByMonth = append(dyByMonth, div/price)
		}
	}
	dyMonthly := mean(dyByMonth)

	dyPeriod := 0.0
	if priceMean > 0 {
		dyPeriod = dividendTotal / priceMean
	}
	dyAnnualized := 0.0
	if periodDays > 0 {
		dyAnnualized = dyPeriod * (365.0 / float64(periodDays))
	}

	pvpSeries := computeIndicatorSeries(indicatorSnapshots, "pvp")
	pvpValues := make([]float64, 0, len(pvpSeries))
	pvpCurrent := 0.0
	for _, p := range pvpSeries {
		pvpValues = append(pvpValues, p.Value)
		pvpCurrent = p.Value
	}
	pvpMean := mean(pvpValues)
	pvpMin := 0.0
	pvpMax := 0.0
	if len(pvpValues) > 0 {
		pvpMin = pvpValues[0]
		pvpMax = pvpValues[0]
		for _, v := range pvpValues {
			if v < pvpMin {
				pvpMin = v
			}
			if v > pvpMax {
				pvpMax = v
			}
		}
	}
	pvpStd := stdev(pvpValues)
	pvpPercentile := percentileRank(pvpValues, pvpCurrent)
	pvpTimeAbove1 := 0.0
	if len(pvpValues) > 0 {
		count := 0
		for _, v := range pvpValues {
			if v > 1 {
				count++
			}
		}
		pvpTimeAbove1 = float64(count) / float64(len(pvpValues))
	}
	pvpAmplitude := 0.0
	if pvpMin > 0 {
		pvpAmplitude = pvpMax/pvpMin - 1
	}

	liqSeries := computeIndicatorSeries(indicatorSnapshots, "liquidez_diaria")
	liqValues := make([]float64, 0, len(liqSeries))
	for _, p := range liqSeries {
		liqValues = append(liqValues, p.Value)
	}
	liqMean := mean(liqValues)
	liqMin := 0.0
	liqMax := 0.0
	if len(liqValues) > 0 {
		liqMin = liqValues[0]
		liqMax = liqValues[0]
		for _, v := range liqValues {
			if v < liqMin {
				liqMin = v
			}
			if v > liqMax {
				liqMax = v
			}
		}
	}
	liqSlopePoints := make([]xy, 0, len(liqSeries))
	for i, p := range liqSeries {
		liqSlopePoints = append(liqSlopePoints, xy{X: float64(i), Y: p.Value})
	}
	liqTrendSlope := linearSlope(liqSlopePoints)

	expectedTradingDays := 0
	if len(cotationDatesIso) >= 2 {
		expectedTradingDays = countWeekdaysBetweenIso(cotationDatesIso[0], cotationDatesIso[len(cotationDatesIso)-1])
	} else {
		expectedTradingDays = len(cotationDatesIso)
	}
	tradedDays := len(cotationDatesIso)
	pctDaysTraded := 0.0
	if expectedTradingDays > 0 {
		pctDaysTraded = float64(tradedDays) / float64(expectedTradingDays)
	}
	liqZeroDays := expectedTradingDays - tradedDays
	if liqZeroDays < 0 {
		liqZeroDays = 0
	}

	plSeries := computeIndicatorSeries(indicatorSnapshots, "valor_patrimonial")
	plValues := make([]float64, 0, len(plSeries))
	for _, p := range plSeries {
		plValues = append(plValues, p.Value)
	}
	plGrowth12m := computeGrowth(plSeries, 365)
	plGrowth3m := computeGrowth(plSeries, 90)
	plMin := 0.0
	plMax := 0.0
	if len(plValues) > 0 {
		plMin = plValues[0]
		plMax = plValues[0]
		for _, v := range plValues {
			if v < plMin {
				plMin = v
			}
			if v > plMax {
				plMax = v
			}
		}
	}
	plVol := stdev(plValues)

	cotistasSeries := computeIndicatorSeries(indicatorSnapshots, "numero_de_cotistas")
	cotistasGrowth := computeGrowth(cotistasSeries, 365)

	maxConsecPaid := 0
	maxConsecNoPay := 0
	if len(allMonths) > 0 {
		curPaid := 0
		curNoPay := 0
		for _, mk := range allMonths {
			paid := dividendByMonth[mk] > 0
			if paid {
				curPaid++
				if curPaid > maxConsecPaid {
					maxConsecPaid = curPaid
				}
				curNoPay = 0
			} else {
				curNoPay++
				if curNoPay > maxConsecNoPay {
					maxConsecNoPay = curNoPay
				}
				curPaid = 0
			}
		}
	}
	pctMonthsWithHistory := 0.0
	if expectedMonths > 0 {
		pctMonthsWithHistory = float64(len(monthKeys)) / float64(expectedMonths)
	}
	fundAgeDays := periodDays

	scoreStability := clamp01(regularity * (1 - math.Min(1, dividendCv)))
	scoreVolatility := clamp01(1 - math.Min(1, volatility/0.05))
	scoreLiquidity := clamp01(math.Min(1, pctDaysTraded))
	scoreConsistency := clamp01(regularity)
	scoreComposite := clamp01((scoreStability + scoreVolatility + scoreLiquidity + scoreConsistency) / 4)

	todayPrices := make([]float64, 0, len(cotationsToday))
	for _, it := range cotationsToday {
		if isFiniteFloat(it.Price) && it.Price > 0 {
			todayPrices = append(todayPrices, it.Price)
		}
	}
	todayFirst := 0.0
	todayLast := 0.0
	if len(todayPrices) > 0 {
		todayFirst = todayPrices[0]
		todayLast = todayPrices[len(todayPrices)-1]
	}
	todayReturn := 0.0
	if todayFirst > 0 {
		todayReturn = todayLast/todayFirst - 1
	}

	todayMin := 0.0
	todayMax := 0.0
	if len(todayPrices) > 0 {
		todayMin = todayPrices[0]
		todayMax = todayPrices[0]
		for _, p := range todayPrices {
			if p < todayMin {
				todayMin = p
			}
			if p > todayMax {
				todayMax = p
			}
		}
	}
	todayReturns := make([]float64, 0, len(todayPrices))
	for i := 1; i < len(todayPrices); i++ {
		prev := todayPrices[i-1]
		cur := todayPrices[i]
		if prev > 0 {
			todayReturns = append(todayReturns, cur/prev-1)
		}
	}
	todayMaxTickDrop := 0.0
	todayMaxTickGain := 0.0
	todayPositiveTicks := 0
	todayNegativeTicks := 0
	if len(todayReturns) > 0 {
		todayMaxTickDrop = todayReturns[0]
		todayMaxTickGain = todayReturns[0]
		for _, r := range todayReturns {
			if r < todayMaxTickDrop {
				todayMaxTickDrop = r
			}
			if r > todayMaxTickGain {
				todayMaxTickGain = r
			}
			if r > 0 {
				todayPositiveTicks++
			} else if r < 0 {
				todayNegativeTicks++
			}
		}
	}
	todayPctPositiveTicks := 0.0
	if len(todayReturns) > 0 {
		todayPctPositiveTicks = float64(todayPositiveTicks) / float64(len(todayReturns))
	}
	todayVolatility := stdev(todayReturns)
	todayAmplitude := 0.0
	if todayMin > 0 {
		todayAmplitude = todayMax/todayMin - 1
	}
	todayDirection := "flat"
	if todayReturn > 0.002 {
		todayDirection = "up"
	} else if todayReturn < -0.002 {
		todayDirection = "down"
	}

	out := ExportFundJSON{
		GeneratedAt: time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
		Fund:        details,
		Period: ExportFundPeriod{
			Start:              periodStart,
			End:                periodEnd,
			CotationsDaysLimit: cotationsDays,
		},
		Data: ExportFundData{
			Cotations:      cotationItems,
			Dividends:      dividendsInPeriod,
			CotationsToday: cotationsToday,
		},
	}
	if len(indicatorSnapshots) > 0 {
		out.Data.IndicatorsLatest = indicatorSnapshots[0].Data
	}

	out.Metrics.Price = ExportFundMetricsPrice{
		Initial:               r2(priceInitial),
		Final:                 r2(priceFinal),
		Min:                   r2(priceMin),
		Max:                   r2(priceMax),
		Mean:                  r2(priceMean),
		MeanDailyReturn:       r6(meanDailyReturn),
		SimpleReturn:          r6(simpleReturn),
		CumulativeReturn:      r6(cumulativeReturn),
		CagrAnnualized:        r6(cagrAnnualized),
		Last3dReturn:          r6(last3dReturn),
		AvgMonthlyReturn:      r6(avgMonthlyReturn),
		Volatility:            r6(volatility),
		VolatilityAnnualized:  r6(volatilityAnnualized),
		DownsideVolatility:    r6(downsideVolatility),
		DownsideVolAnnualized: r6(downsideVolAnnualized),
		DrawdownMax:           r6(dd.MaxDrawdown),
		DrawdownDurationDays:  dd.MaxDurationDays,
		RecoveryTimeDays:      dd.MaxRecoveryDays,
		MaxDailyDrop:          r6(maxDown),
		MaxDailyGain:          r6(maxUp),
		PositiveDays:          posDays,
		NegativeDays:          negDays,
		PctPositiveDays:       r6(pctPositiveDays),
		VariationAmplitude:    r6(variationAmplitude),
	}

	out.Metrics.Dividends = ExportFundMetricsDividends{
		Total:                r2(dividendTotal),
		Payments:             dividendCount,
		Mean:                 r4(dividendMean),
		Median:               r4(dividendMedian),
		Max:                  r4(dividendMax),
		Min:                  r4(dividendMin),
		Stdev:                r6(dividendStd),
		CV:                   r6(dividendCv),
		MonthsWithPayment:    monthsWithPayment,
		MonthsWithoutPayment: monthsWithoutPayment,
		Regularity:           r6(regularity),
		AvgIntervalDays:      avgPaymentIntervalDays,
		TrendSlope:           r6(dividendTrendSlope),
	}

	out.Metrics.DividendYield = ExportFundMetricsDividendYield{
		Period:      r6(dyPeriod),
		MonthlyMean: r6(dyMonthly),
		Annualized:  r6(dyAnnualized),
	}

	out.Metrics.Valuation = ExportFundMetricsValuation{
		PVPCurrent:    r4(pvpCurrent),
		PVPMean:       r4(pvpMean),
		PVPMin:        r4(pvpMin),
		PVPMax:        r4(pvpMax),
		PVPStdev:      r6(pvpStd),
		PVPPercentile: r6(pvpPercentile),
		PctDaysPVPGt1: r6(pvpTimeAbove1),
		PVPAmplitude:  r6(pvpAmplitude),
	}

	out.Metrics.Liquidity = ExportFundMetricsLiquidity{
		Mean:          r2(liqMean),
		Min:           r2(liqMin),
		Max:           r2(liqMax),
		ZeroDays:      liqZeroDays,
		PctDaysTraded: r6(pctDaysTraded),
		TrendSlope:    r6(liqTrendSlope),
	}

	out.Metrics.Risk = ExportFundMetricsRisk{
		Volatility:            r6(volatility),
		VolatilityAnnualized:  r6(volatilityAnnualized),
		DownsideVolatility:    r6(downsideVolatility),
		DownsideVolAnnualized: r6(downsideVolAnnualized),
		DrawdownMax:           r6(dd.MaxDrawdown),
		DrawdownDurationDays:  dd.MaxDurationDays,
		RecoveryTimeDays:      dd.MaxRecoveryDays,
		Var95:                 r6(var95),
		Sharpe:                r6(sharpe),
		Sortino:               r6(sortino),
		Calmar:                r6(calmar),
	}

	out.Metrics.Structure = ExportFundMetricsStructure{
		NetWorthSeriesPoints: len(plSeries),
		NetWorthSeries: func() []ExportFundSeriesPoint {
			out := make([]ExportFundSeriesPoint, 0, len(plSeries))
			for _, p := range plSeries {
				out = append(out, ExportFundSeriesPoint{At: p.At, Value: r2(p.Value)})
			}
			return out
		}(),
		NetWorthGrowth12m:    r6(plGrowth12m),
		NetWorthGrowth3m:     r6(plGrowth3m),
		NetWorthMin:          r2(plMin),
		NetWorthMax:          r2(plMax),
		NetWorthVolatility:   r6(plVol),
		CotistasSeriesPoints: len(cotistasSeries),
		CotistasSeries: func() []ExportFundSeriesPointInt {
			out := make([]ExportFundSeriesPointInt, 0, len(cotistasSeries))
			for _, p := range cotistasSeries {
				out = append(out, ExportFundSeriesPointInt{At: p.At, Value: int(rN(p.Value, 0))})
			}
			return out
		}(),
		CotistasGrowth12m: r6(cotistasGrowth),
	}

	out.Metrics.Consistency = ExportFundMetricsConsistency{
		MaxConsecutiveMonthsPaid:   maxConsecPaid,
		MaxConsecutiveMonthsUnpaid: maxConsecNoPay,
		PctMonthsWithHistory:       r6(pctMonthsWithHistory),
		FundAgeDays:                int(rN(float64(fundAgeDays), 0)),
	}

	out.Metrics.Quality = ExportFundMetricsQuality{
		ScoreStability:   r6(scoreStability),
		ScoreVolatility:  r6(scoreVolatility),
		ScoreLiquidity:   r6(scoreLiquidity),
		ScoreConsistency: r6(scoreConsistency),
		ScoreComposite:   r6(scoreComposite),
	}

	out.Metrics.Today = ExportFundMetricsToday{
		First:              r2(todayFirst),
		Last:               r2(todayLast),
		Min:                r2(todayMin),
		Max:                r2(todayMax),
		Return:             r6(todayReturn),
		Direction:          todayDirection,
		Ticks:              len(todayPrices),
		MaxTickDrop:        r6(todayMaxTickDrop),
		MaxTickGain:        r6(todayMaxTickGain),
		PositiveTicks:      todayPositiveTicks,
		NegativeTicks:      todayNegativeTicks,
		PctPositiveTicks:   r6(todayPctPositiveTicks),
		Volatility:         r6(todayVolatility),
		VariationAmplitude: r6(todayAmplitude),
	}

	return out
}

func clampInt(v int, fallback int, min int, max int) int {
	if v <= 0 {
		return fallback
	}
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}

func isFiniteFloat(f float64) bool {
	return !math.IsNaN(f) && !math.IsInf(f, 0)
}

func mean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	acc := 0.0
	for _, v := range values {
		acc += v
	}
	return acc / float64(len(values))
}

func median(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)
	mid := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[mid]
	}
	return (sorted[mid-1] + sorted[mid]) / 2
}

func stdev(values []float64) float64 {
	if len(values) < 2 {
		return 0
	}
	m := mean(values)
	acc := 0.0
	for _, v := range values {
		d := v - m
		acc += d * d
	}
	return math.Sqrt(acc / float64(len(values)-1))
}

func quantile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	q := p
	if q < 0 {
		q = 0
	}
	if q > 1 {
		q = 1
	}
	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)
	idx := float64(len(sorted)-1) * q
	lo := int(math.Floor(idx))
	hi := int(math.Ceil(idx))
	if lo == hi {
		return sorted[lo]
	}
	w := idx - float64(lo)
	return sorted[lo]*(1-w) + sorted[hi]*w
}

type xy struct {
	X float64
	Y float64
}

func linearSlope(values []xy) float64 {
	n := len(values)
	if n < 2 {
		return 0
	}
	sumX := 0.0
	sumY := 0.0
	sumXY := 0.0
	sumXX := 0.0
	for _, p := range values {
		sumX += p.X
		sumY += p.Y
		sumXY += p.X * p.Y
		sumXX += p.X * p.X
	}
	denom := float64(n)*sumXX - sumX*sumX
	if denom == 0 {
		return 0
	}
	return (float64(n)*sumXY - sumX*sumY) / denom
}

func annualizeVolatility(dailyVolatility float64, tradingDays float64) float64 {
	if !isFiniteFloat(dailyVolatility) || dailyVolatility <= 0 {
		return 0
	}
	td := tradingDays
	if !isFiniteFloat(td) || td <= 0 {
		td = 252
	}
	return dailyVolatility * math.Sqrt(td)
}

func annualizeCagr(simpleReturn float64, periodDays int) float64 {
	if !isFiniteFloat(simpleReturn) || simpleReturn <= -1 {
		return 0
	}
	if periodDays <= 0 {
		return 0
	}
	return math.Pow(1+simpleReturn, 365/float64(periodDays)) - 1
}

func sharpeRatio(meanDailyReturn float64, dailyVolatility float64, tradingDays float64) float64 {
	if !isFiniteFloat(meanDailyReturn) {
		return 0
	}
	if !isFiniteFloat(dailyVolatility) || dailyVolatility <= 0 {
		return 0
	}
	td := tradingDays
	if !isFiniteFloat(td) || td <= 0 {
		td = 252
	}
	return (meanDailyReturn / dailyVolatility) * math.Sqrt(td)
}

func sortinoRatio(meanDailyReturn float64, downsideVolatility float64, tradingDays float64) float64 {
	if !isFiniteFloat(meanDailyReturn) {
		return 0
	}
	if !isFiniteFloat(downsideVolatility) || downsideVolatility <= 0 {
		return 0
	}
	td := tradingDays
	if !isFiniteFloat(td) || td <= 0 {
		td = 252
	}
	return (meanDailyReturn / downsideVolatility) * math.Sqrt(td)
}

func calmarRatio(cagrAnnualized float64, maxDrawdown float64) float64 {
	if !isFiniteFloat(cagrAnnualized) {
		return 0
	}
	if !isFiniteFloat(maxDrawdown) {
		return 0
	}
	dd := math.Abs(maxDrawdown)
	if dd <= 0 {
		return 0
	}
	return cagrAnnualized / dd
}

type drawdownResult struct {
	MaxDrawdown     float64
	PeakIndex       int
	TroughIndex     int
	MaxDurationDays int
	MaxRecoveryDays int
}

func computeDrawdown(prices []float64) drawdownResult {
	peak := math.Inf(-1)
	maxDrawdown := 0.0
	peakIndex := 0
	troughIndex := 0
	maxDuration := 0
	maxRecovery := 0

	currentPeakIndex := 0
	currentPeak := 0.0
	if len(prices) > 0 {
		currentPeak = prices[0]
	}
	peak = currentPeak

	inDrawdown := false
	drawdownStartIndex := 0
	drawdownPeakValue := currentPeak

	for i := 0; i < len(prices); i++ {
		price := prices[i]
		if price <= 0 {
			continue
		}

		if price > peak {
			peak = price
			peakIndex = i
		}

		dd := 0.0
		if peak > 0 {
			dd = price/peak - 1
		}
		if dd < maxDrawdown {
			maxDrawdown = dd
			troughIndex = i
		}

		if !inDrawdown {
			if price < currentPeak && currentPeak > 0 {
				inDrawdown = true
				drawdownStartIndex = currentPeakIndex
				drawdownPeakValue = currentPeak
			} else if price >= currentPeak {
				currentPeak = price
				currentPeakIndex = i
			}
			continue
		}

		if price >= drawdownPeakValue {
			duration := i - drawdownStartIndex
			if duration > maxDuration {
				maxDuration = duration
			}
			if duration > maxRecovery {
				maxRecovery = duration
			}
			inDrawdown = false
			currentPeak = price
			currentPeakIndex = i
			continue
		}
	}

	if inDrawdown && len(prices) > 0 {
		duration := (len(prices) - 1) - drawdownStartIndex
		if duration > maxDuration {
			maxDuration = duration
		}
	}

	return drawdownResult{
		MaxDrawdown:     maxDrawdown,
		PeakIndex:       peakIndex,
		TroughIndex:     troughIndex,
		MaxDurationDays: maxDuration,
		MaxRecoveryDays: maxRecovery,
	}
}

type seriesPoint struct {
	At    string
	Value float64
}

func pickIndicatorValue(data model.NormalizedIndicators, key string) (float64, bool) {
	series := data[key]
	if len(series) == 0 {
		return 0, false
	}
	var candidate *float64
	for i := range series {
		if strings.TrimSpace(series[i].Year) == "Atual" {
			candidate = series[i].Value
			break
		}
	}
	if candidate == nil {
		candidate = series[0].Value
	}
	if candidate == nil || !isFiniteFloat(*candidate) {
		return 0, false
	}
	return *candidate, true
}

func parseISOTime(value string) (time.Time, bool) {
	v := strings.TrimSpace(value)
	if v == "" {
		return time.Time{}, false
	}
	if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
		return t, true
	}
	if t, err := time.Parse(time.RFC3339, v); err == nil {
		return t, true
	}
	return time.Time{}, false
}

func computeIndicatorSeries(snapshots []IndicatorsSnapshot, key string) []seriesPoint {
	out := make([]seriesPoint, 0, len(snapshots))
	for _, s := range snapshots {
		v, ok := pickIndicatorValue(s.Data, key)
		if !ok {
			continue
		}
		out = append(out, seriesPoint{At: s.FetchedAt, Value: v})
	}
	sort.Slice(out, func(i, j int) bool {
		a, okA := parseISOTime(out[i].At)
		b, okB := parseISOTime(out[j].At)
		if okA && okB {
			return a.Before(b)
		}
		return out[i].At < out[j].At
	})
	return out
}

func computeGrowth(series []seriesPoint, daysBack int) float64 {
	if len(series) < 2 {
		return 0
	}
	last := series[len(series)-1]
	lastAt, ok := parseISOTime(last.At)
	if !ok || last.Value <= 0 {
		return 0
	}
	target := lastAt.Add(-time.Duration(daysBack) * 24 * time.Hour)
	var base *seriesPoint
	for i := len(series) - 1; i >= 0; i-- {
		at, ok := parseISOTime(series[i].At)
		if ok && (at.Before(target) || at.Equal(target)) {
			base = &series[i]
			break
		}
	}
	if base == nil || base.Value <= 0 {
		return 0
	}
	return last.Value/base.Value - 1
}

func percentileRank(values []float64, current float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)
	count := 0
	for _, v := range sorted {
		if v <= current {
			count++
		}
	}
	return float64(count) / float64(len(sorted))
}

func rN(v float64, decimals int) float64 {
	if !isFiniteFloat(v) {
		return 0
	}
	if decimals <= 0 {
		return math.Round(v)
	}
	pow := math.Pow10(decimals)
	return math.Round(v*pow) / pow
}

func r2(v float64) float64 { return rN(v, 2) }
func r4(v float64) float64 { return rN(v, 4) }
func r6(v float64) float64 { return rN(v, 6) }

func clamp01(v float64) float64 {
	if !isFiniteFloat(v) {
		return 0
	}
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}
