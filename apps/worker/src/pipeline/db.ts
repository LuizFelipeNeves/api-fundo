import postgres from 'postgres';

// Re-export from db/index for backwards compatibility
export { getRawSql } from '../db';

export type Sql = ReturnType<typeof postgres>;

let sqlSingleton: Sql | null = null;

export function getWriteDb(): Sql {
  if (sqlSingleton) return sqlSingleton;
  const url = String(process.env.DATABASE_URL || '').trim();
  if (!url) throw new Error('DATABASE_URL is required for write-side pipeline');

  const maxRaw = Number.parseInt(process.env.PG_POOL_MAX || '10', 10);
  const max = Number.isFinite(maxRaw) && maxRaw > 0 ? Math.min(maxRaw, 50) : 10;

  sqlSingleton = postgres(url, {
    max,
    idle_timeout: 30,
    connect_timeout: 10,
  });

  return sqlSingleton;
}
