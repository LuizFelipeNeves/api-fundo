import { getWriteDb } from '../pipeline/db';

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
  const sql = getWriteDb();

  const latestRows = await sql<{ maxd: string | null }[]>`
    SELECT max(date_iso) AS maxd
    FROM cotation
    WHERE fund_code = ${code}
  `;
  const latestDateIso = String(latestRows[0]?.maxd || '');
  if (!latestDateIso) return null;

  const cacheRows = await sql<{ src: string; json: string }[]>`
    SELECT source_last_date_iso AS src, data_json AS json
    FROM fund_cotation_stats
    WHERE fund_code = ${code}
    LIMIT 1
  `;

  const cacheRow = cacheRows[0];
  if (cacheRow?.src === latestDateIso && cacheRow.json) {
    try {
      return JSON.parse(cacheRow.json) as CotationStats;
    } catch {
      // fallthrough
    }
  }

  const rows = await sql<{ date_iso: string; price: number }[]>`
    SELECT date_iso, price
    FROM cotation
    WHERE fund_code = ${code}
    ORDER BY date_iso DESC
    LIMIT 400
  `;
  if (!rows.length) return null;

  const ordered = rows.slice().reverse();
  const prices = ordered.map((r) => r.price);
  const asOf = ordered[ordered.length - 1]?.date_iso || latestDateIso;
  const computedAt = new Date().toISOString();
  const stats = computeCotationStatsFromPrices(code, asOf, prices, computedAt);

  await sql`
    INSERT INTO fund_cotation_stats (fund_code, source_last_date_iso, computed_at, data_json)
    VALUES (${code}, ${latestDateIso}, ${computedAt}, ${JSON.stringify(stats)})
    ON CONFLICT (fund_code) DO UPDATE SET
      source_last_date_iso = EXCLUDED.source_last_date_iso,
      computed_at = EXCLUDED.computed_at,
      data_json = EXCLUDED.data_json
  `;

  return stats;
}
