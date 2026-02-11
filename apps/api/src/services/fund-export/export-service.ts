import { toDateIsoFromBr } from '../../db';
import {
  annualizeCagr,
  annualizeVolatility,
  calmarRatio,
  clamp01,
  linearSlope,
  mean,
  median,
  quantile,
  r0,
  r2,
  r4,
  r6,
  sharpeRatio,
  sortinoRatio,
  stdev,
} from './math';
import { computeDrawdown, computeGrowth, computeIndicatorSeries, percentileRank } from './analytics';
import { countWeekdaysBetweenIso, listMonthKeysBetweenInclusive, toMonthKeyFromBr } from './dates';

type CotationItem = { date: string; price: number };

type CotationsData = {
  real?: CotationItem[];
} | null;

type DividendItem = {
  date: string;
  payment: string;
  type: string;
  value: number;
};

type IndicatorsSnapshot = { fetched_at: string; data: Record<string, Array<{ year: string; value: number | null }>> };

type CotationTodayItem = { price: number };

type ExportFundInputs = {
  details: any;
  cotations: CotationsData;
  dividends: DividendItem[] | null;
  indicatorSnapshots: IndicatorsSnapshot[];
  cotationsToday: CotationTodayItem[];
  cotationsDays: number;
};

export function buildFundExportJson({
  details,
  cotations,
  dividends,
  indicatorSnapshots,
  cotationsToday,
  cotationsDays,
}: ExportFundInputs) {
  const cotationItems = cotations?.real ?? [];

  const cotationPrices = cotationItems.map((x) => x.price).filter((p) => Number.isFinite(p) && p > 0);
  const cotationDatesIso = cotationItems.map((x) => toDateIsoFromBr(x.date)).filter(Boolean);
  const periodStart = cotationItems.length ? cotationItems[0].date : '';
  const periodEnd = cotationItems.length ? cotationItems[cotationItems.length - 1].date : '';
  const periodDays =
    cotationDatesIso.length >= 2
      ? Math.max(1, Math.round((Date.parse(cotationDatesIso[cotationDatesIso.length - 1]) - Date.parse(cotationDatesIso[0])) / (24 * 60 * 60 * 1000)))
      : 0;

  const dailyReturns: number[] = [];
  for (let i = 1; i < cotationPrices.length; i++) {
    const prev = cotationPrices[i - 1];
    const cur = cotationPrices[i];
    if (prev > 0) dailyReturns.push(cur / prev - 1);
  }
  const meanDailyReturn = mean(dailyReturns);

  const priceInitial = cotationPrices.length ? cotationPrices[0] : 0;
  const priceFinal = cotationPrices.length ? cotationPrices[cotationPrices.length - 1] : 0;
  const simpleReturn = priceInitial > 0 ? priceFinal / priceInitial - 1 : 0;
  let cumulativeReturn = 0;
  if (dailyReturns.length) {
    let acc = 1;
    for (const r of dailyReturns) acc *= 1 + r;
    cumulativeReturn = acc - 1;
  }
  const cagrAnnualized = annualizeCagr(simpleReturn, periodDays);
  const last3dReturn =
    cotationPrices.length >= 3 && cotationPrices[cotationPrices.length - 3] > 0
      ? cotationPrices[cotationPrices.length - 1] / cotationPrices[cotationPrices.length - 3] - 1
      : 0;

  const priceMin = cotationPrices.length ? Math.min(...cotationPrices) : 0;
  const priceMax = cotationPrices.length ? Math.max(...cotationPrices) : 0;
  const priceMean = mean(cotationPrices);
  const volatility = stdev(dailyReturns);
  const downsideVolatility = stdev(dailyReturns.filter((r) => r < 0));
  const volatilityAnnualized = annualizeVolatility(volatility);
  const downsideVolAnnualized = annualizeVolatility(downsideVolatility);
  const maxDown = dailyReturns.length ? Math.min(...dailyReturns) : 0;
  const maxUp = dailyReturns.length ? Math.max(...dailyReturns) : 0;
  const posDays = dailyReturns.filter((r) => r > 0).length;
  const negDays = dailyReturns.filter((r) => r < 0).length;
  const pctPositiveDays = dailyReturns.length ? posDays / dailyReturns.length : 0;
  const variationAmplitude = priceMin > 0 ? priceMax / priceMin - 1 : 0;
  const var95 = quantile(dailyReturns, 0.05);
  const dd = computeDrawdown(cotationPrices);
  const sharpe = sharpeRatio(meanDailyReturn, volatility);
  const sortino = sortinoRatio(meanDailyReturn, downsideVolatility);
  const calmar = calmarRatio(cagrAnnualized, dd.maxDrawdown);

  const monthLastPrice = new Map<string, number>();
  for (const item of cotationItems) {
    const monthKey = toMonthKeyFromBr(item.date);
    if (!monthKey) continue;
    monthLastPrice.set(monthKey, item.price);
  }
  const monthKeys = Array.from(monthLastPrice.keys()).sort();
  const monthPrices = monthKeys.map((k) => monthLastPrice.get(k) ?? 0).filter((p) => Number.isFinite(p) && p > 0);
  const monthReturns: number[] = [];
  for (let i = 1; i < monthPrices.length; i++) {
    const prev = monthPrices[i - 1];
    const cur = monthPrices[i];
    if (prev > 0) monthReturns.push(cur / prev - 1);
  }
  const avgMonthlyReturn = mean(monthReturns);

  const allDividends = dividends ?? [];
  const dividendsInPeriod = cotationDatesIso.length
    ? allDividends.filter((d) => {
        const iso = toDateIsoFromBr(d.date);
        if (!iso) return false;
        return iso >= cotationDatesIso[0] && iso <= cotationDatesIso[cotationDatesIso.length - 1];
      })
    : allDividends;

  const dividendsOnly = dividendsInPeriod.filter((d) => d.type === 'Dividendos' && Number.isFinite(d.value) && d.value > 0);
  const dividendValues = dividendsOnly.map((d) => d.value);
  const dividendTotal = dividendValues.reduce((a, b) => a + b, 0);
  const dividendCount = dividendValues.length;
  const dividendMean = mean(dividendValues);
  const dividendMedian = median(dividendValues);
  const dividendMax = dividendValues.length ? Math.max(...dividendValues) : 0;
  const dividendMin = dividendValues.length ? Math.min(...dividendValues) : 0;
  const dividendStd = stdev(dividendValues);
  const dividendCv = dividendMean > 0 ? dividendStd / dividendMean : 0;

  const dividendByMonth = new Map<string, number>();
  for (const d of dividendsOnly) {
    const mk = toMonthKeyFromBr(d.date);
    if (!mk) continue;
    dividendByMonth.set(mk, (dividendByMonth.get(mk) ?? 0) + d.value);
  }
  const firstMonth = monthKeys[0] ?? '';
  const lastMonth = monthKeys.length ? monthKeys[monthKeys.length - 1] : '';
  const allMonths = firstMonth && lastMonth ? listMonthKeysBetweenInclusive(firstMonth, lastMonth) : monthKeys;
  const expectedMonths = allMonths.length;
  const monthsWithPayment = allMonths.filter((mk) => (dividendByMonth.get(mk) ?? 0) > 0).length;
  const monthsWithoutPayment = Math.max(0, expectedMonths - monthsWithPayment);
  const regularity = expectedMonths > 0 ? monthsWithPayment / expectedMonths : 0;

  const sortedPaymentIso = dividendsOnly
    .map((d) => toDateIsoFromBr(d.payment))
    .filter(Boolean)
    .sort();
  const paymentIntervalsDays: number[] = [];
  for (let i = 1; i < sortedPaymentIso.length; i++) {
    const a = Date.parse(sortedPaymentIso[i - 1]);
    const b = Date.parse(sortedPaymentIso[i]);
    if (Number.isFinite(a) && Number.isFinite(b) && b > a) {
      paymentIntervalsDays.push(Math.round((b - a) / (24 * 60 * 60 * 1000)));
    }
  }
  const avgPaymentIntervalDays = mean(paymentIntervalsDays);

  const dividendMonthlySeries = allMonths.map((mk, idx) => ({ x: idx, y: dividendByMonth.get(mk) ?? 0 }));
  const dividendTrendSlope = linearSlope(dividendMonthlySeries);

  const dyPeriod = priceMean > 0 ? dividendTotal / priceMean : 0;
  const dyMonthly = (() => {
    const dyByMonth: number[] = [];
    for (const mk of monthKeys) {
      const div = dividendByMonth.get(mk) ?? 0;
      const price = monthLastPrice.get(mk) ?? 0;
      if (div > 0 && price > 0) dyByMonth.push(div / price);
    }
    return mean(dyByMonth);
  })();
  const dyAnnualized = periodDays > 0 ? dyPeriod * (365 / periodDays) : 0;

  const pvpSeries = computeIndicatorSeries(indicatorSnapshots, 'pvp');
  const liquiditySeries = computeIndicatorSeries(indicatorSnapshots, 'liquidez_diaria');
  const plSeries = computeIndicatorSeries(indicatorSnapshots, 'valor_patrimonial');
  const cotistasSeries = computeIndicatorSeries(indicatorSnapshots, 'numero_de_cotistas');

  const pvpValues = pvpSeries.map((p) => p.value);
  const pvpCurrent = pvpSeries.length ? pvpSeries[pvpSeries.length - 1].value : 0;
  const pvpMean = mean(pvpValues);
  const pvpMin = pvpValues.length ? Math.min(...pvpValues) : 0;
  const pvpMax = pvpValues.length ? Math.max(...pvpValues) : 0;
  const pvpStd = stdev(pvpValues);
  const pvpPercentile = percentileRank(pvpValues, pvpCurrent);
  const pvpTimeAbove1 = pvpValues.length ? pvpValues.filter((v) => v > 1).length / pvpValues.length : 0;
  const pvpAmplitude = pvpMin > 0 ? pvpMax / pvpMin - 1 : 0;

  const liqValues = liquiditySeries.map((p) => p.value);
  const liqMean = mean(liqValues);
  const liqMin = liqValues.length ? Math.min(...liqValues) : 0;
  const liqMax = liqValues.length ? Math.max(...liqValues) : 0;
  const liqTrendSlope = linearSlope(liquiditySeries.map((p, idx) => ({ x: idx, y: p.value })));

  const expectedTradingDays =
    cotationDatesIso.length >= 2 ? countWeekdaysBetweenIso(cotationDatesIso[0], cotationDatesIso[cotationDatesIso.length - 1]) : cotationDatesIso.length;
  const tradedDays = cotationDatesIso.length;
  const pctDaysTraded = expectedTradingDays > 0 ? tradedDays / expectedTradingDays : 0;
  const liqZeroDays = Math.max(0, expectedTradingDays - tradedDays);

  const plValues = plSeries.map((p) => p.value);
  const plGrowth12m = computeGrowth(plSeries, 365);
  const plGrowth3m = computeGrowth(plSeries, 90);
  const plMin = plValues.length ? Math.min(...plValues) : 0;
  const plMax = plValues.length ? Math.max(...plValues) : 0;
  const plVol = stdev(plValues);

  const cotistasGrowth = computeGrowth(cotistasSeries, 365);

  const maxConsecPaid = (() => {
    if (!allMonths.length) return 0;
    let best = 0;
    let cur = 0;
    for (const mk of allMonths) {
      const paid = (dividendByMonth.get(mk) ?? 0) > 0;
      if (paid) {
        cur++;
        best = Math.max(best, cur);
      } else {
        cur = 0;
      }
    }
    return best;
  })();

  const maxConsecNoPay = (() => {
    if (!allMonths.length) return 0;
    let best = 0;
    let cur = 0;
    for (const mk of allMonths) {
      const paid = (dividendByMonth.get(mk) ?? 0) > 0;
      if (!paid) {
        cur++;
        best = Math.max(best, cur);
      } else {
        cur = 0;
      }
    }
    return best;
  })();

  const pctMonthsWithHistory = expectedMonths > 0 ? monthKeys.length / expectedMonths : 0;
  const fundAgeDays = periodDays;

  const scoreStability = clamp01(regularity * (1 - Math.min(1, dividendCv)));
  const scoreVolatility = clamp01(1 - Math.min(1, volatility / 0.05));
  const scoreLiquidity = clamp01(Math.min(1, pctDaysTraded));
  const scoreConsistency = clamp01(regularity);
  const scoreComposite = clamp01((scoreStability + scoreVolatility + scoreLiquidity + scoreConsistency) / 4);

  const todayPrices = cotationsToday.map((x) => x.price).filter((p) => Number.isFinite(p) && p > 0);
  const todayFirst = todayPrices.length ? todayPrices[0] : 0;
  const todayLast = todayPrices.length ? todayPrices[todayPrices.length - 1] : 0;
  const todayMin = todayPrices.length ? Math.min(...todayPrices) : 0;
  const todayMax = todayPrices.length ? Math.max(...todayPrices) : 0;
  const todayReturn = todayFirst > 0 ? todayLast / todayFirst - 1 : 0;
  const todayReturns: number[] = [];
  for (let i = 1; i < todayPrices.length; i++) {
    const prev = todayPrices[i - 1];
    const cur = todayPrices[i];
    if (prev > 0) todayReturns.push(cur / prev - 1);
  }
  const todayMaxTickDrop = todayReturns.length ? Math.min(...todayReturns) : 0;
  const todayMaxTickGain = todayReturns.length ? Math.max(...todayReturns) : 0;
  const todayPositiveTicks = todayReturns.filter((r) => r > 0).length;
  const todayNegativeTicks = todayReturns.filter((r) => r < 0).length;
  const todayPctPositiveTicks = todayReturns.length ? todayPositiveTicks / todayReturns.length : 0;
  const todayVolatility = stdev(todayReturns);
  const todayAmplitude = todayMin > 0 ? todayMax / todayMin - 1 : 0;

  return {
    generated_at: new Date().toISOString(),
    fund: details,
    period: { start: periodStart, end: periodEnd, cotations_days_limit: cotationsDays },
    data: {
      cotations: cotations?.real ?? [],
      dividends: dividendsInPeriod,
      indicators_latest: indicatorSnapshots[0]?.data ?? null,
      cotations_today: cotationsToday,
    },
    metrics: {
      price: {
        initial: r2(priceInitial),
        final: r2(priceFinal),
        min: r2(priceMin),
        max: r2(priceMax),
        mean: r2(priceMean),
        mean_daily_return: r6(meanDailyReturn),
        simple_return: r6(simpleReturn),
        cumulative_return: r6(cumulativeReturn),
        cagr_annualized: r6(cagrAnnualized),
        last_3d_return: r6(last3dReturn),
        avg_monthly_return: r6(avgMonthlyReturn),
        volatility: r6(volatility),
        volatility_annualized: r6(volatilityAnnualized),
        downside_volatility: r6(downsideVolatility),
        downside_volatility_annualized: r6(downsideVolAnnualized),
        drawdown_max: r6(dd.maxDrawdown),
        drawdown_duration_days: r0(dd.maxDurationDays),
        recovery_time_days: r0(dd.maxRecoveryDays),
        max_daily_drop: r6(maxDown),
        max_daily_gain: r6(maxUp),
        positive_days: r0(posDays),
        negative_days: r0(negDays),
        pct_positive_days: r6(pctPositiveDays),
        variation_amplitude: r6(variationAmplitude),
      },
      dividends: {
        total: r2(dividendTotal),
        payments: r0(dividendCount),
        mean: r4(dividendMean),
        median: r4(dividendMedian),
        max: r4(dividendMax),
        min: r4(dividendMin),
        stdev: r6(dividendStd),
        cv: r6(dividendCv),
        months_with_payment: r0(monthsWithPayment),
        months_without_payment: r0(monthsWithoutPayment),
        regularity: r6(regularity),
        avg_interval_days: r0(avgPaymentIntervalDays),
        trend_slope: r6(dividendTrendSlope),
      },
      dividend_yield: {
        period: r6(dyPeriod),
        monthly_mean: r6(dyMonthly),
        annualized: r6(dyAnnualized),
      },
      valuation: {
        pvp_current: r4(pvpCurrent),
        pvp_mean: r4(pvpMean),
        pvp_min: r4(pvpMin),
        pvp_max: r4(pvpMax),
        pvp_stdev: r6(pvpStd),
        pvp_percentile: r6(pvpPercentile),
        pct_days_pvp_gt_1: r6(pvpTimeAbove1),
        pvp_amplitude: r6(pvpAmplitude),
      },
      liquidity: {
        mean: r2(liqMean),
        min: r2(liqMin),
        max: r2(liqMax),
        zero_days: r0(liqZeroDays),
        pct_days_traded: r6(pctDaysTraded),
        trend_slope: r6(liqTrendSlope),
      },
      risk: {
        volatility: r6(volatility),
        volatility_annualized: r6(volatilityAnnualized),
        downside_volatility: r6(downsideVolatility),
        downside_volatility_annualized: r6(downsideVolAnnualized),
        drawdown_max: r6(dd.maxDrawdown),
        drawdown_duration_days: r0(dd.maxDurationDays),
        recovery_time_days: r0(dd.maxRecoveryDays),
        var_95: r6(var95),
        sharpe: r6(sharpe),
        sortino: r6(sortino),
        calmar: r6(calmar),
      },
      structure: {
        net_worth_series_points: r0(plSeries.length),
        net_worth_series: plSeries.map((p) => ({ at: p.at, value: r2(p.value) })),
        net_worth_growth_12m: r6(plGrowth12m),
        net_worth_growth_3m: r6(plGrowth3m),
        net_worth_min: r2(plMin),
        net_worth_max: r2(plMax),
        net_worth_volatility: r6(plVol),
        cotistas_series_points: r0(cotistasSeries.length),
        cotistas_series: cotistasSeries.map((p) => ({ at: p.at, value: r0(p.value) })),
        cotistas_growth_12m: r6(cotistasGrowth),
      },
      consistency: {
        max_consecutive_months_paid: r0(maxConsecPaid),
        max_consecutive_months_unpaid: r0(maxConsecNoPay),
        pct_months_with_history: r6(pctMonthsWithHistory),
        fund_age_days: r0(fundAgeDays),
      },
      quality: {
        score_stability: r6(scoreStability),
        score_volatility: r6(scoreVolatility),
        score_liquidity: r6(scoreLiquidity),
        score_consistency: r6(scoreConsistency),
        score_composite: r6(scoreComposite),
      },
      today: {
        first: r2(todayFirst),
        last: r2(todayLast),
        min: r2(todayMin),
        max: r2(todayMax),
        return: r6(todayReturn),
        direction: todayReturn > 0.002 ? 'up' : todayReturn < -0.002 ? 'down' : 'flat',
        ticks: r0(todayPrices.length),
        max_tick_drop: r6(todayMaxTickDrop),
        max_tick_gain: r6(todayMaxTickGain),
        positive_ticks: r0(todayPositiveTicks),
        negative_ticks: r0(todayNegativeTicks),
        pct_positive_ticks: r6(todayPctPositiveTicks),
        volatility: r6(todayVolatility),
        variation_amplitude: r6(todayAmplitude),
      },
    },
  };
}
