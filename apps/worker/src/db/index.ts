import postgres from 'postgres';
import { drizzle } from 'drizzle-orm/postgres-js';
import * as schema from './schema';

let sqlSingleton: postgres.Sql | null = null;
let dbSingleton: ReturnType<typeof drizzle> | null = null;

export function getWriteDb(): ReturnType<typeof drizzle> {
  if (dbSingleton) return dbSingleton;
  const url = String(process.env.DATABASE_URL || '').trim();
  if (!url) throw new Error('DATABASE_URL is required');
  sqlSingleton = postgres(url, { max: 10, idle_timeout: 30, connect_timeout: 10 });
  dbSingleton = drizzle(sqlSingleton, { schema });
  return dbSingleton;
}

export function getRawSql(): postgres.Sql {
  if (!sqlSingleton) getWriteDb(); // ensure singleton is created
  return sqlSingleton!;
}

export type WriteDb = ReturnType<typeof getWriteDb>;
