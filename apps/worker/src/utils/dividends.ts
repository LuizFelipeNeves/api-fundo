import { getWriteDb } from '../db';
import type { Dividend, PersistDividend } from '../pipeline/messages';

type ExistingYield = { date_iso: string; type: number; yield: number | null };

export async function enrichDividendYields(items: Dividend[]): Promise<PersistDividend[]> {
  if (items.length === 0) return [];

  const fundCode = items[0]!.fund_code.toUpperCase();
  const uniqueDates = Array.from(new Set(items.map((i) => i.date_iso)));
  const uniqueTypes = Array.from(new Set(items.map((i) => i.type)));

  const sql = getWriteDb();

  const existingMap = new Map<string, number | null>();
  if (uniqueDates.length && uniqueTypes.length) {
    const existingRows = await sql<ExistingYield[]>`
      SELECT date_iso, type, yield
      FROM dividend
      WHERE fund_code = ${fundCode}
        AND date_iso = ANY(${sql.array(uniqueDates, 'date')})
        AND type = ANY(${sql.array(uniqueTypes, 'int4')})
    `;
    for (const row of existingRows) {
      existingMap.set(`${row.date_iso}|${row.type}`, row.yield ?? null);
    }
  }

  const priceMap = new Map<string, number>();
  if (uniqueDates.length > 0) {
    const priceRows = await sql<{ date_iso: string; price: number | null }[]>`
      SELECT date_iso, price
      FROM cotation
      WHERE fund_code = ${fundCode}
        AND date_iso = ANY(${sql.array(uniqueDates, 'date')})
    `;
    for (const row of priceRows) {
      if (row.price !== null) priceMap.set(row.date_iso, row.price);
    }
  }

  return items.map((item) => {
    const key = `${item.date_iso}|${item.type}`;
    if (existingMap.has(key)) {
      const existingYield = existingMap.get(key);
      return { ...item, yield: Number.isFinite(existingYield) ? Number(existingYield) : 0 };
    }

    const price = priceMap.get(item.date_iso);
    if (price && price > 0) {
      return { ...item, yield: (item.value / price) * 100 };
    }

    return { ...item, yield: 0 };
  });
}
