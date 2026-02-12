import { getRawSql } from '../db';
import { listAllFundCodes } from '../db/queries';

export async function runEodCotationRoutine() {
  const sql = getRawSql();
  const lockKey = 4419270101;
  const lockRows = await sql<{ locked: boolean }[]>`SELECT pg_try_advisory_lock(${lockKey}) AS locked`;
  if (!lockRows[0]?.locked) return 0;

  try {
    const codes = await listAllFundCodes(Number.MAX_SAFE_INTEGER);
    let processedCount = 0;

    for (const code of codes) {
      const rows = await sql<{
        date_iso: string | null;
        data_json: string | null;
      }[]>`
        SELECT date_iso, data_json
        FROM cotations_today_snapshot
        WHERE fund_code = ${code}
        ORDER BY fetched_at DESC
        LIMIT 1
      `;

      const snapshot = rows[0];
      if (!snapshot?.date_iso || !snapshot.data_json) continue;
      let parsed: any = null;
      try {
        parsed = JSON.parse(snapshot.data_json);
      } catch {
        continue;
      }

      const price = Array.isArray(parsed)
        ? parsed[parsed.length - 1]?.price
        : typeof parsed?.price === 'number'
          ? parsed.price
          : null;

      if (!Number.isFinite(price)) continue;

      await sql`
        INSERT INTO cotation (fund_code, date_iso, price)
        VALUES (${code}, ${snapshot.date_iso}, ${price})
        ON CONFLICT (fund_code, date_iso) DO UPDATE SET
          price = EXCLUDED.price
      `;

      processedCount++;
    }

    return processedCount;
  } finally {
    await sql`SELECT pg_advisory_unlock(${lockKey})`;
  }
}
