import { withTryAdvisoryXactLock } from '../utils/pg-lock';

const EOD_LOCK_KEY_DEFAULT = 4419270101;

export async function runEodCotationRoutineWithSql(sql: any): Promise<number> {
  const result = await sql<{ count: number }[]>`

    WITH latest AS (
      SELECT DISTINCT ON (fund_code)
        fund_code,
        date_iso,
        data_json
      FROM cotations_today_snapshot
      WHERE date_iso IS NOT NULL
        AND data_json IS NOT NULL
      ORDER BY fund_code, fetched_at DESC
    ),

    extracted AS (
      SELECT
        fund_code,
        date_iso,
        CASE
          WHEN jsonb_typeof(data_json::jsonb) = 'array'
            THEN (data_json::jsonb -> -1 ->> 'price')::numeric
          ELSE
            (data_json::jsonb ->> 'price')::numeric
        END AS price
      FROM latest
    ),

    upserted AS (
      INSERT INTO cotation (fund_code, date_iso, price)
      SELECT fund_code, date_iso, price
      FROM extracted
      WHERE price IS NOT NULL
      ON CONFLICT (fund_code, date_iso)
      DO UPDATE SET price = EXCLUDED.price
      RETURNING 1
    )

    SELECT COUNT(*)::int as count FROM upserted;

  `;

  return result[0]?.count ?? 0;
}


export async function runEodCotationRoutine(): Promise<number> {
  const lockKeyRaw = Number.parseInt(process.env.EOD_COTATION_LOCK_KEY || String(EOD_LOCK_KEY_DEFAULT), 10);
  const lockKey = Number.isFinite(lockKeyRaw) ? lockKeyRaw : EOD_LOCK_KEY_DEFAULT;
  const result = await withTryAdvisoryXactLock(lockKey, async (tx) => runEodCotationRoutineWithSql(tx));
  return result ?? 0;
}
