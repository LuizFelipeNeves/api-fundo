import postgres from 'postgres';

export type Sql = ReturnType<typeof postgres>;

let sqlSingleton: Sql | null = null;

export function getDb(): Sql {
  if (sqlSingleton) return sqlSingleton;

  const url = String(process.env.DATABASE_URL || '').trim();
  if (!url) {
    throw new Error('DATABASE_URL is required for the read API');
  }

  const maxRaw = Number.parseInt(process.env.PG_POOL_MAX || '10', 10);
  const max = Number.isFinite(maxRaw) && maxRaw > 0 ? Math.min(maxRaw, 50) : 10;

  sqlSingleton = postgres(url, {
    max,
    idle_timeout: 30,
    connect_timeout: 10,
  });

  return sqlSingleton;
}

export function toDateBrFromIso(dateIso: string): string {
  const str = String(dateIso || '').trim();
  const match = str.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (!match) return '';
  return `${match[3]}/${match[2]}/${match[1]}`;
}

export function toDateIsoFromBr(dateStr: string): string {
  const str = String(dateStr || '').trim();
  const match = str.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (!match) return '';
  const day = Number(match[1]);
  const month = Number(match[2]);
  const year = Number(match[3]);
  if (!day || !month || !year) return '';
  const iso = new Date(Date.UTC(year, month - 1, day, 0, 0, 0, 0)).toISOString().slice(0, 10);
  return iso;
}
