import { getWriteDb } from '../db';
import { cotation, fundCotationStats } from '../db/schema';
import { eq, desc } from 'drizzle-orm';

export type CotationStats = {
  fundCode: string;
  asOfDateIso: string;
  lastPrice: number;
  returns: { d7: number | null; d30: number | null; d90: number | null };
  drawdown: { max: number | null };
  volatility: { d30: number | null; d90: number | null };
  samples: { prices: number; returns30: number; returns90: number };
  computedAt: string;
};

function stdev(values: number[]): number | null {
  if (values.length < 2) return null;
  const mean = values.reduce((a, b) => a + b, 0) / values.length;
  let acc = 0;
  for (const v of values) acc += (v - mean) * (v - mean);
  return Math.sqrt(acc / (values.length - 1));
}

function maxDrawdown(prices: number[]): number | null {
  if (prices.length < 2) return null;
  let peak = prices[0];
  let maxDd = 0;
  for (const p of prices) {
    if (p > peak) peak = p;
    if (peak > 0) {
      const dd = p / peak - 1;
      if (dd < maxDd) maxDd = dd;
    }
  }
  return maxDd;
}

function pickReturn(prices: number[], days: number): number | null {
  if (prices.length < days + 1) return null;
  const last = prices[prices.length - 1];
  const prev = prices[prices.length - 1 - days];
  if (!Number.isFinite(last) || !Number.isFinite(prev) || prev <= 0) return null;
  return last / prev - 1;
}

export function computeCotationStatsFromPrices(fundCode: string, asOfDateIso: string, prices: number[], computedAt: string): CotationStats {
  const clean = prices.filter((p) => Number.isFinite(p) && p > 0);
  const lastPrice = clean.length ? clean[clean.length - 1] : 0;

  const dailyReturns: number[] = [];
  for (let i = 1; i < clean.length; i++) {
    const prev = clean[i - 1];
    const cur = clean[i];
    if (prev > 0) dailyReturns.push(cur / prev - 1);
  }

  const returns30 = dailyReturns.slice(-30);
  const returns90 = dailyReturns.slice(-90);
  const vol30 = stdev(returns30);
  const vol90 = stdev(returns90);
  const annual = Math.sqrt(252);

  const windowForDd = clean.slice(-252);
  const dd = maxDrawdown(windowForDd);

  return {
    fundCode,
    asOfDateIso,
    lastPrice,
    returns: { d7: pickReturn(clean, 7), d30: pickReturn(clean, 30), d90: pickReturn(clean, 90) },
    drawdown: { max: dd },
    volatility: { d30: vol30 === null ? null : vol30 * annual, d90: vol90 === null ? null : vol90 * annual },
    samples: { prices: clean.length, returns30: returns30.length, returns90: returns90.length },
    computedAt,
  };
}

export async function getOrComputeCotationStats(fundCode: string): Promise<CotationStats | null> {
  const code = fundCode.toUpperCase();
  const db = getWriteDb();

  const latestRows = await db.select({ maxd: cotation.dateIso })
    .from(cotation)
    .where(eq(cotation.fundCode, code))
    .orderBy(desc(cotation.dateIso))
    .limit(1);
  const latestRow = latestRows[0];

  const latestDateIso = latestRow?.maxd ?? '';
  if (!latestDateIso) return null;

  const cacheRows = await db.select({
    src: fundCotationStats.dateIso,
  })
    .from(fundCotationStats)
    .where(eq(fundCotationStats.fundCode, code))
    .limit(1);
  const cacheRow = cacheRows[0];

  if (cacheRow?.src === latestDateIso) {
    // Get full stats from the stored columns
    const statsRows = await db.select({
      d7: fundCotationStats.d7,
      d30: fundCotationStats.d30,
      d90: fundCotationStats.d90,
      drawdown: fundCotationStats.drawdown,
      volatility30: fundCotationStats.volatility30,
      volatility90: fundCotationStats.volatility90,
      dateIso: fundCotationStats.dateIso,
    })
      .from(fundCotationStats)
      .where(eq(fundCotationStats.fundCode, code));
    const statsRow = statsRows[0];

    if (statsRow) {
      return {
        fundCode: code,
        asOfDateIso: statsRow.dateIso ?? latestDateIso,
        lastPrice: 0, // Not stored, but required by type
        returns: { d7: statsRow.d7, d30: statsRow.d30, d90: statsRow.d90 },
        drawdown: { max: statsRow.drawdown },
        volatility: { d30: statsRow.volatility30, d90: statsRow.volatility90 },
        samples: { prices: 0, returns30: 0, returns90: 0 },
        computedAt: new Date().toISOString(),
      };
    }
  }

  const rows = await db.select({
    dateIso: cotation.dateIso,
    price: cotation.price,
  })
    .from(cotation)
    .where(eq(cotation.fundCode, code))
    .orderBy(desc(cotation.dateIso))
    .limit(400);

  if (!rows.length) return null;

  const ordered = rows.slice().reverse();
  const prices = ordered.map((r) => r.price ?? 0);
  const asOf = ordered[ordered.length - 1]?.dateIso ?? latestDateIso;
  const stats = computeCotationStatsFromPrices(code, asOf, prices, new Date().toISOString());

  await db.insert(fundCotationStats)
    .values({
      fundCode: code,
      dateIso: latestDateIso,
      d7: stats.returns.d7,
      d30: stats.returns.d30,
      d90: stats.returns.d90,
      drawdown: stats.drawdown.max,
      volatility30: stats.volatility.d30,
      volatility90: stats.volatility.d90,
    })
    .onConflictDoUpdate({
      target: fundCotationStats.fundCode,
      set: {
        dateIso: latestDateIso,
        d7: stats.returns.d7,
        d30: stats.returns.d30,
        d90: stats.returns.d90,
        drawdown: stats.drawdown.max,
        volatility30: stats.volatility.d30,
        volatility90: stats.volatility.d90,
      },
    });

  return stats;
}
