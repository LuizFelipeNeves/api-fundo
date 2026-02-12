import { getRawSql } from '../db';

type FundRow = { code: string };

type FundWithCnpjRow = { code: string; cnpj: string | null };

export type StateField =
  | 'last_details_sync_at'
  | 'last_indicators_at'
  | 'last_cotations_today_at'
  | 'last_historical_cotations_at'
  | 'last_documents_at';

function toIsoCutoff(minIntervalMs: number): string {
  if (!Number.isFinite(minIntervalMs) || minIntervalMs < 0) {
    throw new Error('Invalid minIntervalMs');
  }
  return new Date(Date.now() - minIntervalMs).toISOString();
}

export async function listCandidatesByState(
  field: StateField,
  limit: number,
  minIntervalMs: number,
  opts?: { requireId?: boolean; requireCnpj?: boolean }
): Promise<string[]> {
  const sqlDb = getRawSql();
  const requireId = opts?.requireId === true;
  const requireCnpj = opts?.requireCnpj === true;
  const cutoff = toIsoCutoff(minIntervalMs);
  const fieldSql = `"${field}"`;

  const rows = await sqlDb.unsafe<FundRow[]>(
    `SELECT fm.code
     FROM fund_master fm
     LEFT JOIN fund_state fs ON fs.fund_code = fm.code
     WHERE ($1 IS FALSE OR fm.id IS NOT NULL)
       AND ($2 IS FALSE OR fm.cnpj IS NOT NULL)
       AND (fs.${fieldSql} IS NULL OR fs.${fieldSql} < $3)
     ORDER BY fs.${fieldSql} NULLS FIRST, fm.code ASC
     LIMIT $4`,
    [requireId, requireCnpj, cutoff, limit]
  );

  return rows.map((r) => r.code.toUpperCase());
}

export async function listDocumentsCandidates(
  limit: number,
  minIntervalMs: number
): Promise<Array<{ code: string; cnpj: string }>> {
  const sqlDb = getRawSql();
  const cutoff = toIsoCutoff(minIntervalMs);

  const rows = await sqlDb<FundWithCnpjRow[]>`
    SELECT fm.code, fm.cnpj
    FROM fund_master fm
    LEFT JOIN fund_state fs ON fs.fund_code = fm.code
    WHERE fm.cnpj IS NOT NULL
      AND (fs.last_documents_at IS NULL OR fs.last_documents_at < ${cutoff})
    ORDER BY fs.last_documents_at NULLS FIRST, fm.code ASC
    LIMIT ${limit}
  `;

  return rows
    .filter((r) => r.cnpj)
    .map((r) => ({ code: r.code.toUpperCase(), cnpj: r.cnpj! }));
}

export async function listAllFundCodes(limit: number, sqlDb: any = getRawSql()): Promise<string[]> {
  const rows = (await sqlDb<FundRow[]>`
    SELECT code
    FROM fund_master
    ORDER BY code ASC
    LIMIT ${limit}
  `) as FundRow[];
  return rows.map((r: FundRow) => r.code.toUpperCase());
}
