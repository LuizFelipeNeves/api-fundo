import { toDateIsoFromBr } from '../../db';
import { listFundCodes } from '../../db/repo';
import { exportFundJson } from '../../services/fund-export';
import { listExistingFundCodes, listTelegramUserFunds } from '../storage';
import { formatRankHojeMessage, formatRankVMessage } from '../webhook-messages';
import type { HandlerDeps } from './types';

function medianValue(values: number[]): number {
  if (!values.length) return 0;
  const sorted = values.slice().sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) return sorted[mid];
  return (sorted[mid - 1] + sorted[mid]) / 2;
}

export async function handleRankHoje({ db, telegram, chatIdStr }: HandlerDeps, codes: string[]) {
  const requested = codes.length ? codes.map((c) => c.toUpperCase()) : listTelegramUserFunds(db, chatIdStr);
  if (!requested.length) {
    await telegram.sendText(chatIdStr, 'Sua lista está vazia.');
    return;
  }

  const existing = listExistingFundCodes(db, requested);
  const missing = requested.filter((code) => !existing.includes(code));

  const ranked: Array<{ code: string; pvp: number | null; dividendYieldMonthly: number | null; sharpe: number | null }> = [];
  for (const code of existing) {
    const data = exportFundJson(db, code);
    if (!data) continue;
    const vacancia = Number.isFinite(data.fund?.vacancia) ? (data.fund.vacancia as number) : null;
    const dailyLiquidity = Number.isFinite(data.fund?.daily_liquidity) ? (data.fund.daily_liquidity as number) : null;
    const pvp = Number.isFinite(data.metrics?.valuation?.pvp_current) ? (data.metrics.valuation.pvp_current as number) : null;
    const dyMonthly = Number.isFinite(data.metrics?.dividend_yield?.monthly_mean) ? (data.metrics.dividend_yield.monthly_mean as number) : null;
    const sharpe = Number.isFinite(data.metrics?.risk?.sharpe) ? (data.metrics.risk.sharpe as number) : null;

    if (pvp === null || dyMonthly === null || sharpe === null || vacancia === null || dailyLiquidity === null) continue;
    if (pvp < 0.94 && dyMonthly > 0.011 && vacancia === 0 && dailyLiquidity > 300_000 && sharpe >= 1.7) {
      ranked.push({ code, pvp, dividendYieldMonthly: dyMonthly, sharpe });
    }
  }

  ranked.sort((a, b) => {
    const dy = (b.dividendYieldMonthly ?? 0) - (a.dividendYieldMonthly ?? 0);
    if (dy) return dy;
    const sharpeDiff = (b.sharpe ?? 0) - (a.sharpe ?? 0);
    if (sharpeDiff) return sharpeDiff;
    return (a.pvp ?? 0) - (b.pvp ?? 0);
  });

  await telegram.sendText(chatIdStr, formatRankHojeMessage({ items: ranked, total: existing.length, missing }));
}

export async function handleRankV({ db, telegram, chatIdStr }: HandlerDeps) {
  const allCodes = listFundCodes(db);
  if (!allCodes.length) {
    await telegram.sendText(chatIdStr, 'Não encontrei fundos na base.');
    return;
  }

  const ranked: Array<{ code: string; pvp: number | null; dividendYieldMonthly: number | null; regularity: number | null }> = [];
  for (const code of allCodes) {
    const data = exportFundJson(db, code);
    if (!data) continue;
    const pvp = Number.isFinite(data.metrics?.valuation?.pvp_current) ? (data.metrics.valuation.pvp_current as number) : null;
    const dyMonthly = Number.isFinite(data.metrics?.dividend_yield?.monthly_mean) ? (data.metrics.dividend_yield.monthly_mean as number) : null;
    const regularity = Number.isFinite(data.metrics?.dividends?.regularity) ? (data.metrics.dividends.regularity as number) : null;
    const monthsWithoutPayment =
      Number.isFinite(data.metrics?.dividends?.months_without_payment) ? (data.metrics.dividends.months_without_payment as number) : null;
    const rawDividends = Array.isArray(data.data?.dividends) ? data.data.dividends : [];
    const cutoff = new Date();
    cutoff.setUTCFullYear(cutoff.getUTCFullYear() - 1);
    const cutoffIso = cutoff.toISOString().slice(0, 10);
    const dividendSeries = rawDividends
      .map((d: any) => {
        if (!d || d.type !== 'Dividendos' || !Number.isFinite(d.value) || d.value <= 0) return null;
        const iso = toDateIsoFromBr(d.date || '');
        if (!iso) return null;
        if (iso < cutoffIso) return null;
        return { iso, value: Number(d.value) };
      })
      .filter(Boolean) as Array<{ iso: string; value: number }>;
    dividendSeries.sort((a, b) => a.iso.localeCompare(b.iso));
    const dividendValues = dividendSeries.map((d) => d.value);
    const dividendMax = dividendValues.length ? Math.max(...dividendValues) : null;
    const dividendMin = dividendValues.length ? Math.min(...dividendValues) : null;
    const dividendMedian = dividendValues.length ? medianValue(dividendValues) : null;
    const dividendMean = dividendValues.length ? dividendValues.reduce((a, b) => a + b, 0) / dividendValues.length : null;
    const lastDividend = dividendSeries.length ? dividendSeries[dividendSeries.length - 1].value : null;
    const prevMedian = dividendSeries.length >= 4 ? medianValue(dividendSeries.slice(0, -1).map((d) => d.value)) : null;
    const prevMean =
      dividendSeries.length >= 4
        ? dividendSeries.slice(0, -1).map((d) => d.value).reduce((a, b) => a + b, 0) / (dividendSeries.length - 1)
        : null;

    if (code === 'RBRD11') {
      const lastInfo = dividendSeries.length ? dividendSeries[dividendSeries.length - 1] : null;
      console.log('[rankv][debug]', {
        code,
        pvp,
        dyMonthly,
        regularity,
        monthsWithoutPayment,
        dividendMax,
        dividendMedian,
        dividendMean,
        lastDividend,
        lastDividendIso: lastInfo?.iso ?? null,
        prevMedian,
        prevMean,
        dividendCount: dividendSeries.length,
        dividendSeries: dividendSeries.map((d) => ({ iso: d.iso, value: d.value })),
      });
    }

    const split = dividendSeries.length >= 6 ? Math.floor(dividendSeries.length / 2) : 0;
    const firstHalfMean =
      split >= 3 ? dividendSeries.slice(0, split).reduce((a, b) => a + b.value, 0) / split : null;
    const lastHalfMean =
      split >= 3 ? dividendSeries.slice(split).reduce((a, b) => a + b.value, 0) / (dividendSeries.length - split) : null;

    if (
      pvp === null ||
      dyMonthly === null ||
      regularity === null ||
      monthsWithoutPayment === null ||
      dividendMax === null ||
      dividendMin === null ||
      dividendMedian === null ||
      dividendMean === null ||
      lastDividend === null
    ) {
      continue;
    }
    if (dividendSeries.length < 12) continue;
    const spikeOk = dividendMean > 0 ? dividendMax <= dividendMean * 2.5 : false;
    const lastSpikeOk = prevMean && prevMean > 0 ? lastDividend <= prevMean * 2.2 : false;
    const minOk = dividendMean > 0 ? dividendMin >= dividendMean * 0.4 : false;
    const regimeOk = firstHalfMean && lastHalfMean ? lastHalfMean <= firstHalfMean * 1.8 : true;
    const regularityYear = dividendSeries.length >= 12 ? Math.min(1, dividendSeries.length / 12) : 0;
    if (
      pvp <= 0.7 &&
      dyMonthly > 0.0116 &&
      monthsWithoutPayment === 0 &&
      regularityYear >= 0.999 &&
      spikeOk &&
      lastSpikeOk &&
      minOk &&
      regimeOk
    ) {
      ranked.push({ code, pvp, dividendYieldMonthly: dyMonthly, regularity });
    }
  }

  ranked.sort((a, b) => {
    const dy = (b.dividendYieldMonthly ?? 0) - (a.dividendYieldMonthly ?? 0);
    if (dy) return dy;
    const pvp = (a.pvp ?? 0) - (b.pvp ?? 0);
    if (pvp) return pvp;
    return (b.regularity ?? 0) - (a.regularity ?? 0);
  });

  await telegram.sendText(chatIdStr, formatRankVMessage({ items: ranked, total: allCodes.length }));
}
