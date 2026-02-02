import type Database from 'better-sqlite3';
import { toDateIsoFromBr } from '../db';
import { getCotations, getDividends, getDocuments, getFundDetails, getLatestCotationsToday, getLatestIndicatorsSnapshots } from '../db/repo';

function clamp01(value: number): number {
  if (!Number.isFinite(value)) return 0;
  if (value <= 0) return 0;
  if (value >= 1) return 1;
  return value;
}

function mean(values: number[]): number {
  if (!values.length) return 0;
  let sum = 0;
  for (const v of values) sum += v;
  return sum / values.length;
}

function median(values: number[]): number {
  if (!values.length) return 0;
  const sorted = values.slice().sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) return sorted[mid];
  return (sorted[mid - 1] + sorted[mid]) / 2;
}

function stdev(values: number[]): number {
  if (values.length < 2) return 0;
  const m = mean(values);
  let acc = 0;
  for (const v of values) {
    const d = v - m;
    acc += d * d;
  }
  return Math.sqrt(acc / (values.length - 1));
}

function quantile(values: number[], p: number): number {
  if (!values.length) return 0;
  const q = Math.max(0, Math.min(1, p));
  const sorted = values.slice().sort((a, b) => a - b);
  const idx = (sorted.length - 1) * q;
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  if (lo === hi) return sorted[lo];
  const w = idx - lo;
  return sorted[lo] * (1 - w) + sorted[hi] * w;
}

function linearSlope(values: Array<{ x: number; y: number }>): number {
  const n = values.length;
  if (n < 2) return 0;
  let sumX = 0;
  let sumY = 0;
  let sumXY = 0;
  let sumXX = 0;
  for (const p of values) {
    sumX += p.x;
    sumY += p.y;
    sumXY += p.x * p.y;
    sumXX += p.x * p.x;
  }
  const denom = n * sumXX - sumX * sumX;
  if (denom === 0) return 0;
  return (n * sumXY - sumX * sumY) / denom;
}

function toMonthKeyFromBr(dateBr: string): string {
  const iso = toDateIsoFromBr(dateBr);
  if (!iso) return '';
  return iso.slice(0, 7);
}

function monthKeyToParts(monthKey: string): { y: number; m: number } | null {
  const [ys, ms] = String(monthKey || '').split('-');
  const y = Number.parseInt(ys || '', 10);
  const m = Number.parseInt(ms || '', 10);
  if (!Number.isFinite(y) || !Number.isFinite(m) || m < 1 || m > 12) return null;
  return { y, m };
}

function monthKeyDiff(a: string, b: string): number {
  const ap = monthKeyToParts(a);
  const bp = monthKeyToParts(b);
  if (!ap || !bp) return 0;
  return (bp.y - ap.y) * 12 + (bp.m - ap.m);
}

function monthKeyAdd(monthKey: string, deltaMonths: number): string {
  const p = monthKeyToParts(monthKey);
  if (!p) return '';
  const base = p.y * 12 + (p.m - 1);
  const next = base + deltaMonths;
  const y = Math.floor(next / 12);
  const m = (next % 12) + 1;
  const ys = String(y).padStart(4, '0');
  const ms = String(m).padStart(2, '0');
  return `${ys}-${ms}`;
}

function listMonthKeysBetweenInclusive(startKey: string, endKey: string): string[] {
  const diff = monthKeyDiff(startKey, endKey);
  if (diff < 0) return [];
  const out: string[] = [];
  for (let i = 0; i <= diff; i++) out.push(monthKeyAdd(startKey, i));
  return out.filter(Boolean);
}

function parseIsoMs(iso: string): number {
  const ms = Date.parse(iso);
  return Number.isFinite(ms) ? ms : 0;
}

function countWeekdaysBetweenIso(startIso: string, endIso: string): number {
  const start = new Date(`${startIso}T00:00:00.000Z`);
  const end = new Date(`${endIso}T00:00:00.000Z`);
  const startMs = start.getTime();
  const endMs = end.getTime();
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs < startMs) return 0;
  let count = 0;
  const cursor = new Date(startMs);
  while (cursor.getTime() <= endMs) {
    const d = cursor.getUTCDay();
    if (d >= 1 && d <= 5) count++;
    cursor.setUTCDate(cursor.getUTCDate() + 1);
  }
  return count;
}

function computeDrawdown(prices: number[]) {
  let peak = -Infinity;
  let maxDrawdown = 0;
  let peakIndex = 0;
  let troughIndex = 0;
  let maxDuration = 0;
  let maxRecovery = 0;

  let currentPeakIndex = 0;
  let currentPeak = prices.length ? prices[0] : 0;
  peak = currentPeak;

  let inDrawdown = false;
  let drawdownStartIndex = 0;
  let drawdownPeakValue = currentPeak;

  for (let i = 0; i < prices.length; i++) {
    const price = prices[i];
    if (price <= 0) continue;

    if (price > peak) {
      peak = price;
      peakIndex = i;
    }

    const dd = peak > 0 ? price / peak - 1 : 0;
    if (dd < maxDrawdown) {
      maxDrawdown = dd;
      troughIndex = i;
    }

    if (!inDrawdown) {
      if (price < currentPeak && currentPeak > 0) {
        inDrawdown = true;
        drawdownStartIndex = currentPeakIndex;
        drawdownPeakValue = currentPeak;
      } else if (price >= currentPeak) {
        currentPeak = price;
        currentPeakIndex = i;
      }
      continue;
    }

    if (price >= drawdownPeakValue) {
      const duration = i - drawdownStartIndex;
      maxDuration = Math.max(maxDuration, duration);
      maxRecovery = Math.max(maxRecovery, duration);
      inDrawdown = false;
      currentPeak = price;
      currentPeakIndex = i;
      continue;
    }
  }

  if (inDrawdown) {
    const duration = prices.length - 1 - drawdownStartIndex;
    maxDuration = Math.max(maxDuration, duration);
  }

  return { maxDrawdown, peakIndex, troughIndex, maxDurationDays: maxDuration, maxRecoveryDays: maxRecovery };
}

type IndicatorsSnapshot = { fetched_at: string; data: Record<string, Array<{ year: string; value: number | null }>> };

function pickIndicatorValue(data: IndicatorsSnapshot['data'], key: string): number | null {
  const series = data[key];
  if (!Array.isArray(series) || !series.length) return null;
  const atual = series.find((x) => x?.year === 'Atual') ?? null;
  const candidate = atual?.value ?? series[0]?.value ?? null;
  return Number.isFinite(candidate as number) ? (candidate as number) : null;
}

function computeIndicatorSeries(snapshots: IndicatorsSnapshot[], key: string) {
  const out: Array<{ at: string; value: number }> = [];
  for (const s of snapshots) {
    const v = pickIndicatorValue(s.data, key);
    if (!Number.isFinite(v as number)) continue;
    out.push({ at: s.fetched_at, value: v as number });
  }
  out.sort((a, b) => parseIsoMs(a.at) - parseIsoMs(b.at));
  return out;
}

function computeGrowth(series: Array<{ at: string; value: number }>, daysBack: number): number {
  if (series.length < 2) return 0;
  const last = series[series.length - 1];
  const lastMs = parseIsoMs(last.at);
  if (!lastMs || last.value <= 0) return 0;
  const targetMs = lastMs - daysBack * 24 * 60 * 60 * 1000;
  let base: { at: string; value: number } | null = null;
  for (let i = series.length - 1; i >= 0; i--) {
    const ms = parseIsoMs(series[i].at);
    if (ms && ms <= targetMs) {
      base = series[i];
      break;
    }
  }
  if (!base || base.value <= 0) return 0;
  return last.value / base.value - 1;
}

function percentileRank(values: number[], current: number): number {
  if (!values.length) return 0;
  const sorted = values.slice().sort((a, b) => a - b);
  let count = 0;
  for (const v of sorted) {
    if (v <= current) count++;
  }
  return count / sorted.length;
}

export function exportFundJson(
  db: Database.Database,
  code: string,
  opts?: { cotationsDays?: number; indicatorsSnapshotsLimit?: number; documentsLimit?: number }
) {
  const fundCode = String(code || '').trim().toUpperCase();
  const details = getFundDetails(db, fundCode);
  if (!details) return null;

  const cotationsDays = Number.isFinite(opts?.cotationsDays) && (opts?.cotationsDays as number) > 0 ? Math.min(Math.floor(opts!.cotationsDays!), 5000) : 1825;
  const cotations = getCotations(db, fundCode, cotationsDays);
  const cotationItems = cotations?.real ?? [];

  const cotationPrices = cotationItems.map((x) => x.price).filter((p) => Number.isFinite(p) && p > 0);
  const cotationDatesIso = cotationItems.map((x) => toDateIsoFromBr(x.date)).filter(Boolean);
  const periodStart = cotationItems.length ? cotationItems[0].date : '';
  const periodEnd = cotationItems.length ? cotationItems[cotationItems.length - 1].date : '';
  const periodDays = cotationDatesIso.length >= 2 ? Math.max(1, Math.round((Date.parse(cotationDatesIso[cotationDatesIso.length - 1]) - Date.parse(cotationDatesIso[0])) / (24 * 60 * 60 * 1000))) : 0;

  const dailyReturns: number[] = [];
  for (let i = 1; i < cotationPrices.length; i++) {
    const prev = cotationPrices[i - 1];
    const cur = cotationPrices[i];
    if (prev > 0) dailyReturns.push(cur / prev - 1);
  }

  const priceInitial = cotationPrices.length ? cotationPrices[0] : 0;
  const priceFinal = cotationPrices.length ? cotationPrices[cotationPrices.length - 1] : 0;
  const simpleReturn = priceInitial > 0 ? priceFinal / priceInitial - 1 : 0;
  let cumulativeReturn = 0;
  if (dailyReturns.length) {
    let acc = 1;
    for (const r of dailyReturns) acc *= 1 + r;
    cumulativeReturn = acc - 1;
  }

  const priceMin = cotationPrices.length ? Math.min(...cotationPrices) : 0;
  const priceMax = cotationPrices.length ? Math.max(...cotationPrices) : 0;
  const priceMean = mean(cotationPrices);
  const volatility = stdev(dailyReturns);
  const downsideVolatility = stdev(dailyReturns.filter((r) => r < 0));
  const maxDown = dailyReturns.length ? Math.min(...dailyReturns) : 0;
  const maxUp = dailyReturns.length ? Math.max(...dailyReturns) : 0;
  const posDays = dailyReturns.filter((r) => r > 0).length;
  const negDays = dailyReturns.filter((r) => r < 0).length;
  const pctPositiveDays = dailyReturns.length ? posDays / dailyReturns.length : 0;
  const variationAmplitude = priceMin > 0 ? priceMax / priceMin - 1 : 0;
  const var95 = quantile(dailyReturns, 0.05);
  const dd = computeDrawdown(cotationPrices);

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

  const allDividends = getDividends(db, fundCode) ?? [];
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
  const paidMonths = Array.from(dividendByMonth.keys()).sort();
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

  const snapshotsLimit =
    Number.isFinite(opts?.indicatorsSnapshotsLimit) && (opts?.indicatorsSnapshotsLimit as number) > 0
      ? Math.min(Math.floor(opts!.indicatorsSnapshotsLimit!), 5000)
      : 365;
  const indicatorSnapshots = getLatestIndicatorsSnapshots(db, fundCode, snapshotsLimit);

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

  const cotationsToday = getLatestCotationsToday(db, fundCode) ?? [];
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

  const documentsAll = getDocuments(db, fundCode) ?? [];
  const documentsLimit = Number.isFinite(opts?.documentsLimit) && (opts?.documentsLimit as number) > 0 ? Math.min(Math.floor(opts!.documentsLimit!), 200) : 20;
  const documents = documentsAll.slice(0, documentsLimit);

  return {
    generated_at: new Date().toISOString(),
    fund: details,
    period: { start: periodStart, end: periodEnd, cotations_days_limit: cotationsDays },
    data: {
      cotations: cotations?.real ?? [],
      dividends: dividendsInPeriod,
      indicators_latest: indicatorSnapshots[0]?.data ?? null,
      cotations_today: cotationsToday,
      documents: { total: documentsAll.length, items: documents },
    },
    metrics: {
      price: {
        initial: priceInitial,
        final: priceFinal,
        min: priceMin,
        max: priceMax,
        mean: priceMean,
        simple_return: simpleReturn,
        cumulative_return: cumulativeReturn,
        avg_monthly_return: avgMonthlyReturn,
        volatility: volatility,
        downside_volatility: downsideVolatility,
        drawdown_max: dd.maxDrawdown,
        drawdown_duration_days: dd.maxDurationDays,
        recovery_time_days: dd.maxRecoveryDays,
        max_daily_drop: maxDown,
        max_daily_gain: maxUp,
        positive_days: posDays,
        negative_days: negDays,
        pct_positive_days: pctPositiveDays,
        variation_amplitude: variationAmplitude,
      },
      dividends: {
        total: dividendTotal,
        payments: dividendCount,
        mean: dividendMean,
        median: dividendMedian,
        max: dividendMax,
        min: dividendMin,
        stdev: dividendStd,
        cv: dividendCv,
        months_with_payment: monthsWithPayment,
        months_without_payment: monthsWithoutPayment,
        regularity: regularity,
        avg_interval_days: avgPaymentIntervalDays,
        trend_slope: dividendTrendSlope,
      },
      dividend_yield: {
        period: dyPeriod,
        monthly_mean: dyMonthly,
        annualized: dyAnnualized,
      },
      valuation: {
        pvp_current: pvpCurrent,
        pvp_mean: pvpMean,
        pvp_min: pvpMin,
        pvp_max: pvpMax,
        pvp_stdev: pvpStd,
        pvp_percentile: pvpPercentile,
        pct_days_pvp_gt_1: pvpTimeAbove1,
        pvp_amplitude: pvpAmplitude,
      },
      liquidity: {
        mean: liqMean,
        min: liqMin,
        max: liqMax,
        zero_days: liqZeroDays,
        pct_days_traded: pctDaysTraded,
        trend_slope: liqTrendSlope,
      },
      risk: {
        volatility: volatility,
        downside_volatility: downsideVolatility,
        drawdown_max: dd.maxDrawdown,
        drawdown_duration_days: dd.maxDurationDays,
        recovery_time_days: dd.maxRecoveryDays,
        var_95: var95,
      },
      structure: {
        net_worth_series_points: plSeries.length,
        net_worth_series: plSeries,
        net_worth_growth_12m: plGrowth12m,
        net_worth_growth_3m: plGrowth3m,
        net_worth_min: plMin,
        net_worth_max: plMax,
        net_worth_volatility: plVol,
        cotistas_series_points: cotistasSeries.length,
        cotistas_series: cotistasSeries,
        cotistas_growth_12m: cotistasGrowth,
      },
      consistency: {
        max_consecutive_months_paid: maxConsecPaid,
        max_consecutive_months_unpaid: maxConsecNoPay,
        pct_months_with_history: pctMonthsWithHistory,
        fund_age_days: fundAgeDays,
      },
      quality: {
        score_stability: scoreStability,
        score_volatility: scoreVolatility,
        score_liquidity: scoreLiquidity,
        score_consistency: scoreConsistency,
        score_composite: scoreComposite,
      },
      today: {
        first: todayFirst,
        last: todayLast,
        min: todayMin,
        max: todayMax,
        return: todayReturn,
        ticks: todayPrices.length,
        max_tick_drop: todayMaxTickDrop,
        max_tick_gain: todayMaxTickGain,
        positive_ticks: todayPositiveTicks,
        negative_ticks: todayNegativeTicks,
        pct_positive_ticks: todayPctPositiveTicks,
        volatility: todayVolatility,
        variation_amplitude: todayAmplitude,
      },
    },
  };
}
