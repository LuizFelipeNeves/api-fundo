import { getDb, toDateBrFromIso, toDateIsoFromBr } from './index';
import { dividendTypeFromCode, type DividendType } from './dividend-type';
import type { FIIResponse, FIIDetails } from '../types';

export type NormalizedIndicators = Record<string, Array<{ year: string; value: number | null }>>;
export type CotationItem = { date: string; price: number };
export type NormalizedCotations = { real: CotationItem[]; dolar: CotationItem[]; euro: CotationItem[] };
export type DividendData = { value: number; yield: number; date: string; payment: string; type: DividendType };
export type CotationTodayItem = { price: number; hour: string };
export type CotationsTodayData = CotationTodayItem[];
export type DocumentData = { id: number; title: string; category: string; type: string; date: string; dateUpload: string; url: string; status: string; version: number };

function canonicalizeCotationsToday(items: CotationsTodayData): CotationsTodayData {
  if (!items.length) return [];
  const byHour = new Map<string, CotationTodayItem>();
  for (const item of items) {
    const price = typeof item?.price === 'number' ? item.price : Number(item?.price);
    if (!Number.isFinite(price)) continue;
    const hour = formatHour(item?.hour);
    byHour.set(hour, { price, hour });
  }
  const out = Array.from(byHour.values());
  out.sort((a, b) => a.hour.localeCompare(b.hour));
  return out;
}

function formatHour(dateValue: unknown): string {
  if (typeof dateValue === 'string') {
    const trimmed = dateValue.trim();
    const hhmm = trimmed.match(/\b(\d{2}:\d{2})\b/);
    if (hhmm) return hhmm[1]!;

    if (trimmed.length >= 16) {
      const sep = trimmed.charCodeAt(10);
      if ((sep === 84 || sep === 32) && trimmed.charCodeAt(13) === 58) {
        const tail = trimmed.slice(16);
        if (!tail.includes('Z') && !tail.includes('+') && !tail.includes('-')) {
          return trimmed.slice(11, 16);
        }
      }
    }
  }

  const date = new Date(typeof dateValue === 'number' ? dateValue : String(dateValue ?? ''));
  const hours = date.getHours();
  const minutes = date.getMinutes();
  if (!Number.isFinite(hours) || !Number.isFinite(minutes)) return '00:00';
  return `${hours < 10 ? '0' : ''}${hours}:${minutes < 10 ? '0' : ''}${minutes}`;
}

export async function listFunds(): Promise<FIIResponse> {
  const sql = getDb();
  const rows = await sql<{ code: string; sector: string | null; p_vp: number | null; dividend_yield: number | null; dividend_yield_last_5_years: number | null; daily_liquidity: number | null; net_worth: number | null; type: string | null }[]>`
    SELECT code, sector, p_vp, dividend_yield, dividend_yield_last_5_years, daily_liquidity, net_worth, type
    FROM fund_list_read
    ORDER BY code ASC
  `;

  return {
    total: rows.length,
    data: rows.map((r) => ({
      code: r.code,
      sector: r.sector ?? '',
      p_vp: r.p_vp ?? 0,
      dividend_yield: r.dividend_yield ?? 0,
      dividend_yield_last_5_years: r.dividend_yield_last_5_years ?? 0,
      daily_liquidity: r.daily_liquidity ?? 0,
      net_worth: r.net_worth ?? 0,
      type: r.type ?? '',
    })),
  };
}

export async function getFundDetails(code: string): Promise<FIIDetails | null> {
  const sql = getDb();
  const rows = await sql<{ id: string | null; code: string; razao_social: string | null; cnpj: string | null; publico_alvo: string | null; mandato: string | null; segmento: string | null; tipo_fundo: string | null; prazo_duracao: string | null; tipo_gestao: string | null; taxa_adminstracao: string | null; daily_liquidity: number | null; vacancia: number | null; numero_cotistas: number | null; cotas_emitidas: number | null; valor_patrimonial_cota: number | null; valor_patrimonial: number | null; ultimo_rendimento: number | null }[]>`
    SELECT id, code, razao_social, cnpj, publico_alvo, mandato, segmento, tipo_fundo, prazo_duracao, tipo_gestao,
           taxa_adminstracao, daily_liquidity, vacancia, numero_cotistas, cotas_emitidas, valor_patrimonial_cota,
           valor_patrimonial, ultimo_rendimento
    FROM fund_details_read
    WHERE code = ${code.toUpperCase()}
    LIMIT 1
  `;

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

export async function getLatestIndicators(code: string): Promise<NormalizedIndicators | null> {
  const sql = getDb();
  const rows = await sql<{ data_json: string }[]>`
    SELECT data_json
    FROM indicators_read
    WHERE fund_code = ${code.toUpperCase()}
    LIMIT 1
  `;
  const row = rows[0];
  if (!row) return null;
  return JSON.parse(row.data_json) as NormalizedIndicators;
}

export async function getLatestIndicatorsSnapshots(
  code: string,
  limit: number
): Promise<Array<{ fetched_at: string; data: NormalizedIndicators }>> {
  const safeLimit = Number.isFinite(limit) && limit > 0 ? Math.min(Math.floor(limit), 5000) : 365;
  const sql = getDb();
  const rows = await sql<{ fetched_at: string; data_json: string }[]>`
    SELECT fetched_at, data_json
    FROM indicators_snapshot_read
    WHERE fund_code = ${code.toUpperCase()}
    ORDER BY fetched_at DESC
    LIMIT ${safeLimit}
  `;

  return rows.map((r) => ({ fetched_at: r.fetched_at, data: JSON.parse(r.data_json) as NormalizedIndicators }));
}

export async function getCotations(code: string, days: number): Promise<NormalizedCotations | null> {
  const limit = Number.isFinite(days) && days > 0 ? Math.min(days, 5000) : 1825;
  const sql = getDb();
  const rows = await sql<{ date_iso: string; price: number }[]>`
    SELECT date_iso, price
    FROM cotations_read
    WHERE fund_code = ${code.toUpperCase()}
    ORDER BY date_iso DESC
    LIMIT ${limit}
  `;

  if (rows.length === 0) return null;

  const real = rows.slice().reverse().map((r) => ({ date: toDateBrFromIso(r.date_iso), price: r.price }));
  return { real, dolar: [], euro: [] };
}

export async function getDividends(code: string): Promise<DividendData[] | null> {
  const sql = getDb();
  const rows = await sql<{ date_iso: string; payment: string; type: number; value: number; yield: number }[]>`
    SELECT date_iso, payment, type, value, yield
    FROM dividends_read
    WHERE fund_code = ${code.toUpperCase()}
    ORDER BY date_iso DESC
  `;

  if (rows.length === 0) return null;

  return rows
    .map((r) => {
      const type = dividendTypeFromCode(r.type);
      if (!type) return null;
      return {
        value: r.value,
        yield: r.yield,
        date: toDateBrFromIso(r.date_iso),
        payment: toDateBrFromIso(r.payment),
        type,
      };
    })
    .filter((v): v is DividendData => v !== null);
}

export async function getLatestCotationsToday(code: string): Promise<CotationsTodayData | null> {
  const sql = getDb();
  const rows = await sql<{ data_json: string }[]>`
    SELECT data_json
    FROM cotations_today_read
    WHERE fund_code = ${code.toUpperCase()}
    ORDER BY fetched_at DESC
    LIMIT 1
  `;

  const row = rows[0];
  if (!row) return null;
  const parsed: any = JSON.parse(row.data_json);
  if (Array.isArray(parsed)) return canonicalizeCotationsToday(parsed);
  if (Array.isArray(parsed?.real)) return canonicalizeCotationsToday(parsed.real);
  return [];
}

export async function getDocuments(code: string): Promise<DocumentData[] | null> {
  const sql = getDb();
  const rows = await sql<{ id: number; title: string; category: string; type: string; date: string; dateupload: string; url: string; status: string; version: number }[]>`
    SELECT document_id AS id, title, category, type, date, "dateUpload" AS dateupload, url, status, version
    FROM documents_read
    WHERE fund_code = ${code.toUpperCase()}
    ORDER BY date_upload_iso DESC, document_id DESC
  `;

  if (rows.length === 0) return null;

  return rows.map((r) => ({
    id: r.id,
    title: r.title,
    category: r.category,
    type: r.type,
    date: r.date,
    dateUpload: r.dateupload,
    url: r.url,
    status: r.status,
    version: r.version,
  }));
}

export async function getDividendYieldFromCotations(code: string, dateIso: string): Promise<number | null> {
  const sql = getDb();
  const rows = await sql<{ price: number }[]>`
    SELECT price
    FROM cotations_read
    WHERE fund_code = ${code.toUpperCase()} AND date_iso = ${dateIso}
    LIMIT 1
  `;
  const row = rows[0];
  if (!row || !Number.isFinite(row.price) || row.price <= 0) return null;
  return row.price;
}

export function getDividendYieldForValue(dividendValue: number, price: number): number {
  if (!Number.isFinite(dividendValue) || !Number.isFinite(price) || price <= 0) return 0;
  return dividendValue / price;
}

export function buildDividendYieldDateIso(dividendDateBr: string): string {
  return toDateIsoFromBr(dividendDateBr);
}
