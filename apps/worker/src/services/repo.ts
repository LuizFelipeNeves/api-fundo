import { getRawSql } from '../db';
import { toDateIsoFromBr } from '../utils/date';

export type NormalizedIndicators = Record<string, Array<{ year: string; value: number | null }>>;
export type CotationItem = { date: string; price: number };
export type NormalizedCotations = { real: CotationItem[]; dolar: CotationItem[]; euro: CotationItem[] };
export type DividendData = { value: number; yield: number; date: string; payment: string; type: 'Dividendos' | 'Amortização' };
export type CotationTodayItem = { price: number; hour: string };
export type CotationsTodayData = CotationTodayItem[];

function toDateBrFromIso(dateIso: string): string {
  const str = String(dateIso || '').trim();
  const match = str.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (!match) return '';
  return `${match[3]}/${match[2]}/${match[1]}`;
}

export async function getFundDetails(code: string) {
  const sql = getRawSql();
  const rows = await sql.unsafe<any[]>(
    `SELECT id, code, razao_social, cnpj, publicly_alvo, mandato, segmento, tipo_fundo, prazo_duracao, tipo_gestao,
           taxa_adminstracao, daily_liquidity, vacancia, numero_cotistas, cotas_emitidas, valor_patrimonial_cota,
           valor_patrimonial, ultimo_rendimento
    FROM fund_details_read
    WHERE code = $1
    LIMIT 1`,
    [code.toUpperCase()]
  );
  const row = rows[0];
  if (!row || !row.id || !row.cnpj) return null;
  return {
    id: String(row.id),
    code: row.code,
    razao_social: row.razao_social ?? '',
    cnpj: row.cnpj ?? '',
    publico_alvo: row.publico_alvo ?? '',
    mandato: row.mandato ?? '',
    segmento: row.segmento ?? '',
    tipo_fundo: row.tipo_fundo ?? '',
    prazo_duracao: row.prazo_duracao ?? '',
    tipo_gestao: row.tipo_gestao ?? '',
    taxa_adminstracao: row.taxa_adminstracao ?? '',
    daily_liquidity: row.daily_liquidity ?? null,
    vacancia: row.vacancia ?? 0,
    numero_cotistas: row.numero_cotistas ?? 0,
    cotas_emitidas: row.cotas_emitidas ?? 0,
    valor_patrimonial_cota: row.valor_patrimonial_cota ?? 0,
    valor_patrimonial: row.valor_patrimonial ?? 0,
    ultimo_rendimento: row.ultimo_rendimento ?? 0,
  };
}

export async function getCotations(code: string, days: number): Promise<NormalizedCotations | null> {
  const limit = Number.isFinite(days) && days > 0 ? Math.min(days, 5000) : 1825;
  const sql = getRawSql();
  const rows = await sql.unsafe<{ date_iso: string; price: number }[]>(
    `SELECT date_iso, price
    FROM cotations_read
    WHERE fund_code = $1
    ORDER BY date_iso DESC
    LIMIT $2`,
    [code.toUpperCase(), limit]
  );
  if (rows.length === 0) return null;
  return {
    real: rows.slice().reverse().map((r) => ({ date: toDateBrFromIso(r.date_iso), price: r.price })),
    dolar: [],
    euro: [],
  };
}

export async function getLatestIndicatorsSnapshots(
  code: string,
  limit: number
): Promise<Array<{ fetched_at: string; data: NormalizedIndicators }>> {
  const safeLimit = Number.isFinite(limit) && limit > 0 ? Math.min(Math.floor(limit), 5000) : 365;
  const sql = getRawSql();
  const rows = await sql.unsafe<{ fetched_at: string; data_json: string }[]>(
    `SELECT fetched_at, data_json
    FROM indicators_snapshot_read
    WHERE fund_code = $1
    ORDER BY fetched_at DESC
    LIMIT $2`,
    [code.toUpperCase(), safeLimit]
  );
  return rows.map((r) => ({ fetched_at: r.fetched_at, data: JSON.parse(r.data_json) as NormalizedIndicators }));
}

export async function getDividends(code: string): Promise<DividendData[] | null> {
  const sql = getRawSql();
  const rows = await sql.unsafe<{ date_iso: string; payment: string; type: number; value: number; yield: number }[]>(
    `SELECT date_iso, payment, type, value, yield
    FROM dividends_read
    WHERE fund_code = $1
    ORDER BY date_iso DESC`,
    [code.toUpperCase()]
  );
  if (rows.length === 0) return null;
  return rows.map((r) => ({
    value: r.value,
    yield: r.yield,
    date: toDateBrFromIso(r.date_iso),
    payment: toDateBrFromIso(r.payment),
    type: r.type === 1 ? 'Dividendos' : 'Amortização',
  }));
}

export async function getLatestCotationsToday(code: string): Promise<CotationsTodayData | null> {
  const sql = getRawSql();
  const rows = await sql.unsafe<{ data_json: string }[]>(
    `SELECT data_json
    FROM cotations_today_read
    WHERE fund_code = $1
    ORDER BY fetched_at DESC
    LIMIT 1`,
    [code.toUpperCase()]
  );
  const row = rows[0];
  if (!row) return null;
  const parsed: any = JSON.parse(row.data_json);
  if (Array.isArray(parsed)) return parsed;
  if (Array.isArray(parsed?.real)) return parsed.real;
  return [];
}
