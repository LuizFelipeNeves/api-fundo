import { listAllFundCodes } from '../db/queries';
import { withTryAdvisoryXactLock } from '../utils/pg-lock';

const EOD_LOCK_KEY_DEFAULT = 4419270101;

export async function runEodCotationRoutineWithSql(sql: any): Promise<number> {
  const codes = await listAllFundCodes(Number.MAX_SAFE_INTEGER, sql);
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
}

export async function runEodCotationRoutine(): Promise<number> {
  const lockKeyRaw = Number.parseInt(process.env.EOD_COTATION_LOCK_KEY || String(EOD_LOCK_KEY_DEFAULT), 10);
  const lockKey = Number.isFinite(lockKeyRaw) ? lockKeyRaw : EOD_LOCK_KEY_DEFAULT;
  const result = await withTryAdvisoryXactLock(lockKey, async (tx) => runEodCotationRoutineWithSql(tx));
  return result ?? 0;
}
