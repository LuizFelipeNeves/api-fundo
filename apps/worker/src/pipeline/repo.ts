import { getWriteDb } from './db';

export async function getFundIdByCode(code: string): Promise<string | null> {
  const sql = getWriteDb();
  const rows = await sql<{ id: string | null }[]>`
    SELECT id
    FROM fund_master
    WHERE code = ${code.toUpperCase()}
    LIMIT 1
  `;
  return rows[0]?.id ?? null;
}

export async function getFundCnpjByCode(code: string): Promise<string | null> {
  const sql = getWriteDb();
  const rows = await sql<{ cnpj: string | null }[]>`
    SELECT cnpj
    FROM fund_master
    WHERE code = ${code.toUpperCase()}
    LIMIT 1
  `;
  return rows[0]?.cnpj ?? null;
}
