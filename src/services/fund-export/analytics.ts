import { parseIsoMs } from './dates';

export function computeDrawdown(prices: number[]) {
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

export function pickIndicatorValue(data: IndicatorsSnapshot['data'], key: string): number | null {
  const series = data[key];
  if (!Array.isArray(series) || !series.length) return null;
  const atual = series.find((x) => x?.year === 'Atual') ?? null;
  const candidate = atual?.value ?? series[0]?.value ?? null;
  return Number.isFinite(candidate as number) ? (candidate as number) : null;
}

export function computeIndicatorSeries(snapshots: IndicatorsSnapshot[], key: string) {
  const out: Array<{ at: string; value: number }> = [];
  for (const s of snapshots) {
    const v = pickIndicatorValue(s.data, key);
    if (!Number.isFinite(v as number)) continue;
    out.push({ at: s.fetched_at, value: v as number });
  }
  out.sort((a, b) => parseIsoMs(a.at) - parseIsoMs(b.at));
  return out;
}

export function computeGrowth(series: Array<{ at: string; value: number }>, daysBack: number): number {
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

export function percentileRank(values: number[], current: number): number {
  if (!values.length) return 0;
  const sorted = values.slice().sort((a, b) => a - b);
  let count = 0;
  for (const v of sorted) {
    if (v <= current) count++;
  }
  return count / sorted.length;
}
