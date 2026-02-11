import { toDateIsoFromBr } from '../../utils/date';
import { exportFundJson } from '../../services/fund-export';
import { listExistingFundCodes, listTelegramUserFunds } from '../storage';
import { formatRankHojeMessage, formatRankVMessage } from '../../telegram-bot/webhook-messages';


import type { HandlerDeps } from './types';
import { getWriteDb } from '../../pipeline/db';

function medianValue(values: number[]): number {
  if (!values.length) return 0;
  const sorted = values.slice().sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) return sorted[mid];
  return (sorted[mid - 1] + sorted[mid]) / 2;
}

async function listFundCodes(): Promise<string[]> {
  const sql = getWriteDb();
  const rows = await sql<{ code: string }[]>`
    SELECT code
    FROM fund_master
    ORDER BY code ASC
  `;
  return rows.map((r: any) => r.code.toUpperCase());
}

export async function handleRankHoje({ db, telegram, chatIdStr }: HandlerDeps, codes: string[]) {
  const requested = codes.length ? codes.map((c) => c.toUpperCase()) : await listTelegramUserFunds(db, chatIdStr);
  if (!requested.length) {
    await telegram.sendText(chatIdStr, 'Sua lista está vazia.');
    return;
  }

  const existing = await listExistingFundCodes(db, requested);
  const missing = requested.filter((code) => !existing.includes(code));

  const ranked: Array<{ code: string; pvp: number | null; dividendYieldMonthly: number | null; sharpe: number | null; todayReturn: number | null }> = [];
  for (const code of existing) {
    const data = await exportFundJson(code);
    if (!data) continue;
    const vacancia = Number.isFinite(data.fund?.vacancia) ? (data.fund.vacancia as number) : null;
    const dailyLiquidity = Number.isFinite(data.fund?.daily_liquidity) ? (data.fund.daily_liquidity as number) : null;
    const pvp = Number.isFinite(data.metrics?.valuation?.pvp_current) ? (data.metrics.valuation.pvp_current as number) : null;
    const dyMonthly = Number.isFinite(data.metrics?.dividend_yield?.monthly_mean) ? (data.metrics.dividend_yield.monthly_mean as number) : null;
    const sharpe = Number.isFinite(data.metrics?.risk?.sharpe) ? (data.metrics.risk.sharpe as number) : null;
    const todayReturn = Number.isFinite(data.metrics?.today?.return) ? (data.metrics.today.return as number) : null;
    const last3dReturn = Number.isFinite(data.metrics?.price?.last_3d_return) ? (data.metrics.price.last_3d_return as number) : null;

    if (pvp === null || dyMonthly === null || sharpe === null || vacancia === null || dailyLiquidity === null) continue;
    const notMelting = (todayReturn ?? 0) > -0.02 && (last3dReturn ?? 0) > -0.05;
    if (pvp < 0.94 && dyMonthly > 0.011 && vacancia === 0 && dailyLiquidity > 300_000 && sharpe >= 1.7 && notMelting) {
      ranked.push({ code, pvp, dividendYieldMonthly: dyMonthly, sharpe, todayReturn });
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
  const allCodes = await listFundCodes();
  if (!allCodes.length) {
    await telegram.sendText(chatIdStr, 'Não encontrei fundos na base.');
    return;
  }

  const ranked: Array<{ code: string; pvp: number | null; dividendYieldMonthly: number | null; regularity: number | null; todayReturn: number | null }> = [];
  for (const code of allCodes) {
    const data = await exportFundJson(code);
    if (!data) continue;
    const pvp = Number.isFinite(data.metrics?.valuation?.pvp_current) ? (data.metrics.valuation.pvp_current as number) : null;
    const dyMonthly = Number.isFinite(data.metrics?.dividend_yield?.monthly_mean) ? (data.metrics.dividend_yield.monthly_mean as number) : null;
    const regularity = Number.isFinite(data.metrics?.dividends?.regularity) ? (data.metrics.dividends.regularity as number) : null;
    const monthsWithoutPayment =
      Number.isFinite(data.metrics?.dividends?.months_without_payment) ? (data.metrics.dividends.months_without_payment as number) : null;
    const todayReturn = Number.isFinite(data.metrics?.today?.return) ? (data.metrics.today.return as number) : null;
    const last3dReturn = Number.isFinite(data.metrics?.price?.last_3d_return) ? (data.metrics.price.last_3d_return as number) : null;
    const dividendCv = Number.isFinite(data.metrics?.dividends?.cv) ? (data.metrics.dividends.cv as number) : null;
    const dividendTrend = Number.isFinite(data.metrics?.dividends?.trend_slope) ? (data.metrics.dividends.trend_slope as number) : null;
    const drawdownMax = Number.isFinite(data.metrics?.risk?.drawdown_max) ? (data.metrics.risk.drawdown_max as number) : null;
    const recoveryDays = Number.isFinite(data.metrics?.risk?.recovery_time_days) ? (data.metrics.risk.recovery_time_days as number) : null;
    const volAnnual = Number.isFinite(data.metrics?.risk?.volatility_annualized) ? (data.metrics.risk.volatility_annualized as number) : null;
    const pvpPercentile = Number.isFinite(data.metrics?.valuation?.pvp_percentile) ? (data.metrics.valuation.pvp_percentile as number) : null;
    const liqMean = Number.isFinite(data.metrics?.liquidity?.mean) ? (data.metrics.liquidity.mean as number) : null;
    const pctDaysTraded = Number.isFinite(data.metrics?.liquidity?.pct_days_traded) ? (data.metrics.liquidity.pct_days_traded as number) : null;
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
      dividendCv === null ||
      dividendTrend === null ||
      drawdownMax === null ||
      recoveryDays === null ||
      volAnnual === null ||
      pvpPercentile === null ||
      liqMean === null ||
      pctDaysTraded === null ||
      last3dReturn === null ||
      todayReturn === null ||
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
    const notMelting = todayReturn > -0.01 && last3dReturn >= 0;
    if (
      pvp <= 0.7 &&
      dyMonthly > 0.0116 &&
      monthsWithoutPayment === 0 &&
      regularityYear >= 0.999 &&
      dividendCv <= 0.6 &&
      dividendTrend > 0 &&
      drawdownMax > -0.25 &&
      recoveryDays <= 120 &&
      volAnnual <= 0.3 &&
      pvpPercentile <= 0.25 &&
      liqMean >= 400000 &&
      pctDaysTraded >= 0.95 &&
      spikeOk &&
      lastSpikeOk &&
      minOk &&
      regimeOk &&
      notMelting
    ) {
      ranked.push({ code, pvp, dividendYieldMonthly: dyMonthly, regularity, todayReturn });
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
