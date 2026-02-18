package fii

import (
	"math"
	"sort"
	"time"

	"github.com/luizfelipeneves/api-fundo/go-api/internal/model"
)

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
		if iso := ToDateISOFromBR(it.Date); iso != "" {
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
		if !isFiniteFloat(it.Price) || it.Price <= 0 {
			continue
		}
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
			iso := ToDateISOFromBR(d.Date)
			if iso == "" {
				iso = ToDateISOFromBR(d.Payment)
			}
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
			mk = toMonthKeyFromBr(d.Payment)
		}
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
		if iso := ToDateISOFromBR(d.Payment); iso != "" {
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
	if pvpCurrent <= 0 && len(pvpValues) > 0 {
		for i := len(pvpValues) - 1; i >= 0; i-- {
			if pvpValues[i] > 0 {
				pvpCurrent = pvpValues[i]
				break
			}
		}
	}
	if pvpCurrent <= 0 && priceFinal > 0 && details != nil && details.ValorPatrimonialCota > 0 {
		pvpCurrent = priceFinal / details.ValorPatrimonialCota
	}
	if len(pvpValues) == 0 && pvpCurrent > 0 {
		pvpValues = append(pvpValues, pvpCurrent)
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
	pvpPercentile := 1.0
	if len(pvpValues) > 0 && pvpCurrent > 0 {
		pvpPercentile = percentileRank(pvpValues, pvpCurrent)
	}
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
