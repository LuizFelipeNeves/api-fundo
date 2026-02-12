import { getRawSql } from '../db';

export async function withTryAdvisoryXactLock<T>(
  key: number,
  fn: (sql: any) => Promise<T>
): Promise<T | null> {
  const sql = getRawSql();
  return (await (sql as any).begin(async (tx: any) => {
    const rows = (await tx.unsafe('SELECT pg_try_advisory_xact_lock($1) AS locked', [key])) as Array<{ locked: boolean }>;
    if (!rows[0]?.locked) return null;
    return (await fn(tx)) as T;
  })) as T | null;
}
