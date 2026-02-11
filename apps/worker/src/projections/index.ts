import postgres from 'postgres';
import { createReadModelWriter } from './read-models';

export type ReadModelWriter = ReturnType<typeof createReadModelWriter>;

let sqlSingleton: ReturnType<typeof postgres> | null = null;

export function getReadModelWriter(): ReadModelWriter {
  if (!sqlSingleton) {
    const url = String(process.env.DATABASE_URL || '').trim();
    if (!url) throw new Error('DATABASE_URL is required for read model projections');

    const maxRaw = Number.parseInt(process.env.PG_POOL_MAX || '10', 10);
    const max = Number.isFinite(maxRaw) && maxRaw > 0 ? Math.min(maxRaw, 50) : 10;

    sqlSingleton = postgres(url, {
      max,
      idle_timeout: 30,
      connect_timeout: 10,
    });
  }

  return createReadModelWriter(sqlSingleton);
}
