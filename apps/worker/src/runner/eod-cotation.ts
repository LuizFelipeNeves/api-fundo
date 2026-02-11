import { getWriteDb } from '../db';
import { cotation, cotationsTodaySnapshot } from '../db/schema';
import { eq, desc } from 'drizzle-orm';
import { listAllFundCodes } from '../db/queries';

export async function runEodCotationRoutine() {
  const db = getWriteDb();

  // Get all fund codes
  const codes = await listAllFundCodes(Number.MAX_SAFE_INTEGER);

  let processedCount = 0;

  for (const code of codes) {
    // Get latest cotation from snapshot
    const latestSnapshot = await db
      .select({
        dateIso: cotationsTodaySnapshot.dateIso,
        dataJson: cotationsTodaySnapshot.dataJson,
      })
      .from(cotationsTodaySnapshot)
      .where(eq(cotationsTodaySnapshot.fundCode, code))
      .orderBy(desc(cotationsTodaySnapshot.fetchedAt))
      .limit(1);

    // Skip if no valid snapshot/price
    if (latestSnapshot.length === 0) {
      continue;
    }
    const snapshot = latestSnapshot[0]!;
    const dataJson = snapshot.dataJson as { price?: number } | null;
    if (!dataJson || typeof dataJson.price !== 'number' || !snapshot.dateIso) {
      continue;
    }

    const price = dataJson.price;
    const dateIso = snapshot.dateIso;

    // Upsert into cotation table (write side)
    await db
      .insert(cotation)
      .values({
        fundCode: code,
        dateIso,
        price,
      })
      .onConflictDoUpdate({
        target: [cotation.fundCode, cotation.dateIso],
        set: { price },
      });

    processedCount++;
  }

  return processedCount;
}
