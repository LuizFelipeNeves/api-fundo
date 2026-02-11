import { getWriteDb } from '../db';
import { cotation } from '../db/schema';
import { Dividend, PersistDividend } from '../pipeline/messages';
import { eq, and, inArray } from 'drizzle-orm';

export async function enrichDividendYields(items: Dividend[]): Promise<PersistDividend[]> {
  if (items.length === 0) return [];

  const fundCode = items[0]!.fund_code.toUpperCase();

  // Get unique dates for price lookup
  const uniqueDates = [...new Set(items.map((i) => i.date_iso))];

  // Fetch prices
  const priceMap = new Map<string, number>();
  if (uniqueDates.length > 0) {
    const priceRows = await getWriteDb()
      .select({ dateIso: cotation.dateIso, price: cotation.price })
      .from(cotation)
      .where(and(eq(cotation.fundCode, fundCode), inArray(cotation.dateIso, uniqueDates)));
    for (const r of priceRows) {
      if (r.price !== null) {
        priceMap.set(r.dateIso, r.price);
      }
    }
  }

  // Enrich items - always calculate yield from price
  return items.map((item) => {
    const price = priceMap.get(item.date_iso);
    if (price && price > 0) {
      return { ...item, yield: (item.value / price) * 100 };
    }

    // Return with 0 yield if no price available
    return { ...item, yield: 0 };
  });
}
