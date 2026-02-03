export function clamp01(value: number): number {
  if (!Number.isFinite(value)) return 0;
  if (value <= 0) return 0;
  if (value >= 1) return 1;
  return value;
}

export function roundTo(value: number, decimals: number): number {
  if (!Number.isFinite(value)) return 0;
  const d = Math.max(0, Math.min(12, Math.floor(decimals)));
  const p = Math.pow(10, d);
  return Math.round(value * p) / p;
}

export function r0(value: number): number {
  return Math.round(Number.isFinite(value) ? value : 0);
}

export function r2(value: number): number {
  return roundTo(value, 2);
}

export function r4(value: number): number {
  return roundTo(value, 4);
}

export function r6(value: number): number {
  return roundTo(value, 6);
}

export function mean(values: number[]): number {
  if (!values.length) return 0;
  let sum = 0;
  for (const v of values) sum += v;
  return sum / values.length;
}

export function median(values: number[]): number {
  if (!values.length) return 0;
  const sorted = values.slice().sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) return sorted[mid];
  return (sorted[mid - 1] + sorted[mid]) / 2;
}

export function stdev(values: number[]): number {
  if (values.length < 2) return 0;
  const m = mean(values);
  let acc = 0;
  for (const v of values) {
    const d = v - m;
    acc += d * d;
  }
  return Math.sqrt(acc / (values.length - 1));
}

export function quantile(values: number[], p: number): number {
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

export function linearSlope(values: Array<{ x: number; y: number }>): number {
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

export function annualizeCagr(simpleReturn: number, periodDays: number): number {
  if (!Number.isFinite(simpleReturn) || simpleReturn <= -1) return 0;
  if (!Number.isFinite(periodDays) || periodDays <= 0) return 0;
  return Math.pow(1 + simpleReturn, 365 / periodDays) - 1;
}

export function annualizeVolatility(dailyVolatility: number, tradingDays = 252): number {
  if (!Number.isFinite(dailyVolatility) || dailyVolatility <= 0) return 0;
  const td = Number.isFinite(tradingDays) && tradingDays > 0 ? tradingDays : 252;
  return dailyVolatility * Math.sqrt(td);
}

export function sharpeRatio(meanDailyReturn: number, dailyVolatility: number, tradingDays = 252): number {
  if (!Number.isFinite(meanDailyReturn)) return 0;
  if (!Number.isFinite(dailyVolatility) || dailyVolatility <= 0) return 0;
  const td = Number.isFinite(tradingDays) && tradingDays > 0 ? tradingDays : 252;
  return (meanDailyReturn / dailyVolatility) * Math.sqrt(td);
}

export function sortinoRatio(meanDailyReturn: number, downsideVolatility: number, tradingDays = 252): number {
  if (!Number.isFinite(meanDailyReturn)) return 0;
  if (!Number.isFinite(downsideVolatility) || downsideVolatility <= 0) return 0;
  const td = Number.isFinite(tradingDays) && tradingDays > 0 ? tradingDays : 252;
  return (meanDailyReturn / downsideVolatility) * Math.sqrt(td);
}

export function calmarRatio(cagrAnnualized: number, maxDrawdown: number): number {
  if (!Number.isFinite(cagrAnnualized)) return 0;
  if (!Number.isFinite(maxDrawdown)) return 0;
  const dd = Math.abs(maxDrawdown);
  if (dd <= 0) return 0;
  return cagrAnnualized / dd;
}
