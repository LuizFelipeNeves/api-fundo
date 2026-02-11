import type Database from 'better-sqlite3';
import { nowIso, sha256 } from './index';
import type { FIIResponse, FIIDetails } from '../types';
import type { NormalizedIndicators } from '../parsers/indicators';
import type { CotationsTodayData } from '../parsers/today';
import { canonicalizeCotationsToday } from '../parsers/today';
import type { DocumentData } from '../parsers/documents';
import type { DividendData } from '../parsers/dividends';
import type { NormalizedCotations } from '../parsers/cotations';
import { toDateIsoFromBr } from './index';
import { drizzle } from 'drizzle-orm/better-sqlite3';
import { and, asc, desc, eq, gt, isNotNull, isNull, or, sql } from 'drizzle-orm';
import { cotation, cotationsTodaySnapshot, dividend, document, fundMaster, fundState, indicatorsSnapshot } from './schema';

type Orm = ReturnType<typeof drizzle>;
const ormCache = new WeakMap<Database.Database, Orm>();

function getOrm(db: Database.Database): Orm {
  const cached = ormCache.get(db);
  if (cached) return cached;
  const orm = drizzle(db);
  ormCache.set(db, orm);
  return orm;
}

export function upsertFundList(db: Database.Database, data: FIIResponse) {
  const now = nowIso();
  const orm = getOrm(db);

  // Batch insert para fundMaster
  const fundMasterValues = data.data.map((item) => ({
    code: item.code.toUpperCase(),
    sector: item.sector,
    p_vp: item.p_vp,
    dividend_yield: item.dividend_yield,
    dividend_yield_last_5_years: item.dividend_yield_last_5_years,
    daily_liquidity: item.daily_liquidity,
    net_worth: item.net_worth,
    type: item.type,
    created_at: now,
    updated_at: now,
  }));

  // Upsert batch fundMaster
  orm.insert(fundMaster)
    .values(fundMasterValues)
    .onConflictDoUpdate({
      target: fundMaster.code,
      set: {
        sector: sql`excluded.sector`,
        p_vp: sql`excluded.p_vp`,
        dividend_yield: sql`excluded.dividend_yield`,
        dividend_yield_last_5_years: sql`excluded.dividend_yield_last_5_years`,
        daily_liquidity: sql`excluded.daily_liquidity`,
        net_worth: sql`excluded.net_worth`,
        type: sql`excluded.type`,
        updated_at: now,
      },
    })
    .run();

  // Batch insert fundState (apenas os que não existem)
  const fundStateValues = data.data.map((item) => ({
    fund_code: item.code.toUpperCase(),
    created_at: now,
    updated_at: now,
  }));

  orm.insert(fundState)
    .values(fundStateValues)
    .onConflictDoNothing()
    .run();
}

export function updateFundDetails(db: Database.Database, details: FIIDetails) {
  const now = nowIso();
  const orm = getOrm(db);
  const code = details.code.toUpperCase();

  orm
    .insert(fundState)
    .values({ fund_code: code, created_at: now, updated_at: now })
    .onConflictDoNothing()
    .run();

  orm.update(fundMaster)
    .set({
      id: details.id,
      cnpj: details.cnpj,
      razao_social: details.razao_social,
      publico_alvo: details.publico_alvo,
      mandato: details.mandato,
      segmento: details.segmento,
      tipo_fundo: details.tipo_fundo,
      prazo_duracao: details.prazo_duracao,
      tipo_gestao: details.tipo_gestao,
      taxa_adminstracao: details.taxa_adminstracao,
      vacancia: details.vacancia,
      numero_cotistas: details.numero_cotistas,
      cotas_emitidas: details.cotas_emitidas,
      valor_patrimonial_cota: details.valor_patrimonial_cota,
      valor_patrimonial: details.valor_patrimonial,
      ultimo_rendimento: details.ultimo_rendimento,
      updated_at: now,
    })
    .where(eq(fundMaster.code, code))
    .run();

  orm.update(fundState)
    .set({ last_details_sync_at: now, updated_at: now })
    .where(eq(fundState.fund_code, code))
    .run();
}

export function listFunds(db: Database.Database): FIIResponse {
  const orm = getOrm(db);
  const rows = orm
    .select({
      code: fundMaster.code,
      sector: fundMaster.sector,
      p_vp: fundMaster.p_vp,
      dividend_yield: fundMaster.dividend_yield,
      dividend_yield_last_5_years: fundMaster.dividend_yield_last_5_years,
      daily_liquidity: fundMaster.daily_liquidity,
      net_worth: fundMaster.net_worth,
      type: fundMaster.type,
    })
    .from(fundMaster)
    .orderBy(asc(fundMaster.code))
    .all();

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

export function getFundDetails(db: Database.Database, code: string): FIIDetails | null {
  const orm = getOrm(db);
  const row = orm
    .select({
      id: fundMaster.id,
      code: fundMaster.code,
      razao_social: fundMaster.razao_social,
      cnpj: fundMaster.cnpj,
      publico_alvo: fundMaster.publico_alvo,
      mandato: fundMaster.mandato,
      segmento: fundMaster.segmento,
      tipo_fundo: fundMaster.tipo_fundo,
      prazo_duracao: fundMaster.prazo_duracao,
      tipo_gestao: fundMaster.tipo_gestao,
      taxa_adminstracao: fundMaster.taxa_adminstracao,
      daily_liquidity: fundMaster.daily_liquidity,
      vacancia: fundMaster.vacancia,
      numero_cotistas: fundMaster.numero_cotistas,
      cotas_emitidas: fundMaster.cotas_emitidas,
      valor_patrimonial_cota: fundMaster.valor_patrimonial_cota,
      valor_patrimonial: fundMaster.valor_patrimonial,
      ultimo_rendimento: fundMaster.ultimo_rendimento,
    })
    .from(fundMaster)
    .where(eq(fundMaster.code, code.toUpperCase()))
    .get();

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

export function listFundCodes(db: Database.Database): string[] {
  const orm = getOrm(db);
  const rows = orm.select({ code: fundMaster.code }).from(fundMaster).orderBy(asc(fundMaster.code)).all();
  return rows.map((r) => r.code);
}

export function listFundCodesWithId(db: Database.Database): string[] {
  const orm = getOrm(db);
  const rows = orm
    .select({ code: fundMaster.code })
    .from(fundMaster)
    .where(isNotNull(fundMaster.id))
    .orderBy(asc(fundMaster.code))
    .all();
  return rows.map((r) => r.code);
}

export function listFundCodesWithCnpj(db: Database.Database): string[] {
  const orm = getOrm(db);
  const rows = orm
    .select({ code: fundMaster.code })
    .from(fundMaster)
    .where(isNotNull(fundMaster.cnpj))
    .orderBy(asc(fundMaster.code))
    .all();
  return rows.map((r) => r.code);
}

export function listFundCodesForIndicatorsBatch(db: Database.Database, limit: number): string[] {
  const orm = getOrm(db);
  const safeLimit = Number.isFinite(limit) && limit > 0 ? Math.min(Math.floor(limit), 5000) : 100;
  const rows = orm
    .select({ code: fundMaster.code })
    .from(fundMaster)
    .leftJoin(fundState, eq(fundState.fund_code, fundMaster.code))
    .where(isNotNull(fundMaster.id))
    .orderBy(asc(fundState.last_indicators_at), asc(fundMaster.code))
    .limit(safeLimit)
    .all();
  return rows.map((r) => r.code);
}

export function listFundCodesForDetailsSyncBatch(db: Database.Database, limit: number): string[] {
  const orm = getOrm(db);
  const safeLimit = Number.isFinite(limit) && limit > 0 ? Math.min(Math.floor(limit), 5000) : 100;
  const rows = orm
    .select({ code: fundMaster.code })
    .from(fundMaster)
    .leftJoin(fundState, eq(fundState.fund_code, fundMaster.code))
    .orderBy(asc(fundState.last_details_sync_at), asc(fundMaster.code))
    .limit(safeLimit)
    .all();
  return rows.map((r) => r.code);
}

export function listFundCodesForCotationsTodayBatch(db: Database.Database, limit: number): string[] {
  const orm = getOrm(db);
  const safeLimit = Number.isFinite(limit) && limit > 0 ? Math.min(Math.floor(limit), 5000) : 100;
  const rows = orm
    .select({ code: fundMaster.code })
    .from(fundMaster)
    .leftJoin(fundState, eq(fundState.fund_code, fundMaster.code))
    .orderBy(asc(fundState.last_cotations_today_at), asc(fundMaster.code))
    .limit(safeLimit)
    .all();
  return rows.map((r) => r.code);
}

export function listFundCodesForDocumentsBatch(db: Database.Database, limit: number): string[] {
  const orm = getOrm(db);
  const safeLimit = Number.isFinite(limit) && limit > 0 ? Math.min(Math.floor(limit), 5000) : 100;
  const rows = orm
    .select({ code: fundMaster.code })
    .from(fundMaster)
    .leftJoin(fundState, eq(fundState.fund_code, fundMaster.code))
    .where(isNotNull(fundMaster.cnpj))
    .orderBy(asc(fundState.last_documents_at), asc(fundMaster.code))
    .limit(safeLimit)
    .all();
  return rows.map((r) => r.code);
}

export function getDividendCount(db: Database.Database, fundCode: string): number {
  const fundCodeUpper = fundCode.toUpperCase();
  const row = db.prepare('select count(*) as c from dividend where fund_code = ?').get(fundCodeUpper) as { c?: number } | undefined;
  return Number(row?.c ?? 0);
}

export function hasDividend(db: Database.Database, fundCode: string, dateIso: string, type: 'Dividendos' | 'Amortização'): boolean {
  const fundCodeUpper = fundCode.toUpperCase();
  const safeDateIso = String(dateIso || '').trim();
  if (!safeDateIso) return false;
  const row = db
    .prepare('select 1 as ok from dividend where fund_code = ? and date_iso = ? and type = ? limit 1')
    .get(fundCodeUpper, safeDateIso, type) as { ok?: number } | undefined;
  return Boolean(row?.ok);
}

export function listDividendKeys(db: Database.Database, fundCode: string, limit?: number): Array<{ date_iso: string; type: 'Dividendos' | 'Amortização' }> {
  const safeLimit = Number.isFinite(limit) && (limit as number) > 0 ? Math.min(Math.floor(limit as number), 5000) : 200;
  const fundCodeUpper = fundCode.toUpperCase();
  const rows = db
    .prepare('select date_iso, type from dividend where fund_code = ? order by date_iso desc limit ?')
    .all(fundCodeUpper, safeLimit) as Array<{ date_iso?: string; type?: string }>;

  return rows
    .map((r) => {
      const date_iso = String(r.date_iso ?? '').trim();
      const type = r.type === 'Dividendos' || r.type === 'Amortização' ? r.type : null;
      if (!date_iso || !type) return null;
      return { date_iso, type };
    })
    .filter((v): v is { date_iso: string; type: 'Dividendos' | 'Amortização' } => Boolean(v));
}

export function getDividendsTotalCount(db: Database.Database): number {
  const row = db.prepare('select count(*) as c from dividend').get() as { c?: number } | undefined;
  return Number(row?.c ?? 0);
}

export function listFundCodesMissingDetails(db: Database.Database): string[] {
  const orm = getOrm(db);
  const rows = orm
    .select({ code: fundMaster.code })
    .from(fundMaster)
    .where(or(isNull(fundMaster.id), isNull(fundMaster.cnpj)))
    .orderBy(asc(fundMaster.code))
    .all();
  return rows.map((r) => r.code);
}

export function getFundIdAndCnpj(db: Database.Database, code: string): { id: string | null; cnpj: string | null } | null {
  const orm = getOrm(db);
  const row = orm
    .select({ id: fundMaster.id, cnpj: fundMaster.cnpj })
    .from(fundMaster)
    .where(eq(fundMaster.code, code.toUpperCase()))
    .get();
  return row ?? null;
}

export function getFundState(
  db: Database.Database,
  code: string
): { last_documents_max_id: number | null; last_historical_cotations_at: string | null } | null {
  const orm = getOrm(db);
  const row = orm
    .select({
      last_documents_max_id: fundState.last_documents_max_id,
      last_historical_cotations_at: fundState.last_historical_cotations_at,
    })
    .from(fundState)
    .where(eq(fundState.fund_code, code.toUpperCase()))
    .get();
  return row ?? null;
}

export function getFundIndicatorsState(
  db: Database.Database,
  code: string
): { last_indicators_at: string | null } | null {
  const orm = getOrm(db);
  const row = orm
    .select({
      last_indicators_at: fundState.last_indicators_at,
    })
    .from(fundState)
    .where(eq(fundState.fund_code, code.toUpperCase()))
    .get();
  return row ?? null;
}

export function getFundDetailsSyncState(
  db: Database.Database,
  code: string
): { last_details_sync_at: string | null } | null {
  const orm = getOrm(db);
  const row = orm
    .select({
      last_details_sync_at: fundState.last_details_sync_at,
    })
    .from(fundState)
    .where(eq(fundState.fund_code, code.toUpperCase()))
    .get();
  return row ?? null;
}

export function getFundDocumentsState(
  db: Database.Database,
  code: string
): { last_documents_at: string | null } | null {
  const orm = getOrm(db);
  const row = orm
    .select({
      last_documents_at: fundState.last_documents_at,
    })
    .from(fundState)
    .where(eq(fundState.fund_code, code.toUpperCase()))
    .get();
  return row ?? null;
}

export function updateFundDocumentsAt(db: Database.Database, fundCode: string, atIso: string) {
  const orm = getOrm(db);
  const fundCodeUpper = fundCode.toUpperCase();

  orm.insert(fundState).values({ fund_code: fundCodeUpper, created_at: atIso, updated_at: atIso }).onConflictDoNothing().run();
  orm
    .update(fundState)
    .set({
      last_documents_at: atIso,
      updated_at: atIso,
    })
    .where(eq(fundState.fund_code, fundCodeUpper))
    .run();
}

export function getFundCotationsTodayState(
  db: Database.Database,
  code: string
): { last_cotations_today_at: string | null } | null {
  const orm = getOrm(db);
  const row = orm
    .select({
      last_cotations_today_at: fundState.last_cotations_today_at,
    })
    .from(fundState)
    .where(eq(fundState.fund_code, code.toUpperCase()))
    .get();
  return row ?? null;
}

export function upsertIndicatorsSnapshot(db: Database.Database, fundCode: string, fetchedAt: string, dataHash: string, data: NormalizedIndicators) {
  const orm = getOrm(db);
  const fundCodeUpper = fundCode.toUpperCase();
  const json = JSON.stringify(data);
  const existing = orm
    .select({ data_hash: indicatorsSnapshot.data_hash })
    .from(indicatorsSnapshot)
    .where(eq(indicatorsSnapshot.fund_code, fundCodeUpper))
    .get();

  const changed = !existing || existing.data_hash !== dataHash;

  orm
    .insert(indicatorsSnapshot)
    .values({
      fund_code: fundCodeUpper,
      fetched_at: fetchedAt,
      data_hash: dataHash,
      data_json: json,
    })
    .onConflictDoUpdate({
      target: indicatorsSnapshot.fund_code,
      set: { fetched_at: fetchedAt, data_hash: dataHash, data_json: json },
    })
    .run();

  orm
    .insert(fundState)
    .values({ fund_code: fundCodeUpper, created_at: fetchedAt, updated_at: fetchedAt })
    .onConflictDoNothing()
    .run();

  if (changed) {
    orm
      .update(fundState)
      .set({
        last_indicators_hash: dataHash,
        last_indicators_at: fetchedAt,
        updated_at: fetchedAt,
      })
      .where(eq(fundState.fund_code, fundCodeUpper))
      .run();
  } else {
    orm
      .update(fundState)
      .set({
        last_indicators_at: fetchedAt,
        updated_at: fetchedAt,
      })
      .where(eq(fundState.fund_code, fundCodeUpper))
      .run();
  }

  return changed;
}

export function getLatestIndicators(db: Database.Database, fundCode: string): NormalizedIndicators | null {
  const orm = getOrm(db);
  const row = orm
    .select({ data_json: indicatorsSnapshot.data_json })
    .from(indicatorsSnapshot)
    .where(eq(indicatorsSnapshot.fund_code, fundCode.toUpperCase()))
    .orderBy(desc(indicatorsSnapshot.fetched_at))
    .get();
  if (!row) return null;
  return JSON.parse(row.data_json) as NormalizedIndicators;
}

export function getLatestIndicatorsSnapshots(
  db: Database.Database,
  fundCode: string,
  limit: number
): Array<{ fetched_at: string; data: NormalizedIndicators }> {
  const safeLimit = Number.isFinite(limit) && limit > 0 ? Math.min(Math.floor(limit), 5000) : 365;
  const orm = getOrm(db);
  const rows = orm
    .select({ fetched_at: indicatorsSnapshot.fetched_at, data_json: indicatorsSnapshot.data_json })
    .from(indicatorsSnapshot)
    .where(eq(indicatorsSnapshot.fund_code, fundCode.toUpperCase()))
    .orderBy(desc(indicatorsSnapshot.fetched_at))
    .limit(safeLimit)
    .all();

  return rows.map((r) => ({ fetched_at: r.fetched_at, data: JSON.parse(r.data_json) as NormalizedIndicators }));
}

export function upsertCotationsTodaySnapshot(db: Database.Database, fundCode: string, fetchedAt: string, dataHash: string, data: CotationsTodayData) {
  const orm = getOrm(db);
  const fundCodeUpper = fundCode.toUpperCase();
  const dateIso = String(fetchedAt || '').slice(0, 10);
  const incoming = canonicalizeCotationsToday(data);

  const existing = orm
    .select({ data_hash: cotationsTodaySnapshot.data_hash, data_json: cotationsTodaySnapshot.data_json })
    .from(cotationsTodaySnapshot)
    .where(and(eq(cotationsTodaySnapshot.fund_code, fundCodeUpper), eq(cotationsTodaySnapshot.date_iso, dateIso)))
    .get();

  const existingItems = (() => {
    if (!existing?.data_json) return [] as CotationsTodayData;
    try {
      const parsed: any = JSON.parse(existing.data_json);
      return Array.isArray(parsed) ? (parsed as CotationsTodayData) : [];
    } catch {
      return [] as CotationsTodayData;
    }
  })();

  const existingCanonical = canonicalizeCotationsToday(existingItems);
  const existingByHour = new Map<string, { price: number; hour: string }>();
  for (const item of existingCanonical) {
    existingByHour.set(item.hour, item);
  }

  let hasNew = false;
  for (const item of incoming) {
    if (!existingByHour.has(item.hour)) {
      existingByHour.set(item.hour, item);
      hasNew = true;
    }
  }

  const nextItems = hasNew ? canonicalizeCotationsToday(Array.from(existingByHour.values())) : existingItems;
  const nextJson = existing ? (hasNew ? JSON.stringify(nextItems) : existing.data_json) : JSON.stringify(nextItems);
  const nextHash = existing ? (hasNew ? sha256(nextJson) : existing.data_hash) : sha256(nextJson);

  orm
    .insert(cotationsTodaySnapshot)
    .values({
      fund_code: fundCodeUpper,
      date_iso: dateIso,
      fetched_at: fetchedAt,
      data_hash: nextHash,
      data_json: nextJson,
    })
    .onConflictDoUpdate({
      target: [cotationsTodaySnapshot.fund_code, cotationsTodaySnapshot.date_iso],
      set: { fetched_at: fetchedAt, data_hash: nextHash, data_json: nextJson },
    })
    .run();

  orm
    .insert(fundState)
    .values({ fund_code: fundCodeUpper, created_at: fetchedAt, updated_at: fetchedAt })
    .onConflictDoNothing()
    .run();

  orm
    .update(fundState)
    .set({
      last_cotations_today_at: fetchedAt,
      updated_at: fetchedAt,
    })
    .where(eq(fundState.fund_code, fundCodeUpper))
    .run();

  if (hasNew || !existing) {
    orm
      .update(fundState)
      .set({
        last_cotations_today_hash: nextHash,
        updated_at: fetchedAt,
      })
      .where(eq(fundState.fund_code, fundCodeUpper))
      .run();
  }

  return hasNew || !existing;
}

export function getLatestCotationsToday(db: Database.Database, fundCode: string): CotationsTodayData | null {
  const orm = getOrm(db);
  const row = orm
    .select({ data_json: cotationsTodaySnapshot.data_json })
    .from(cotationsTodaySnapshot)
    .where(eq(cotationsTodaySnapshot.fund_code, fundCode.toUpperCase()))
    .orderBy(desc(cotationsTodaySnapshot.fetched_at))
    .get();
  if (!row) return null;
  const parsed: any = JSON.parse(row.data_json);
  if (Array.isArray(parsed)) return canonicalizeCotationsToday(parsed);
  if (Array.isArray(parsed?.real)) return canonicalizeCotationsToday(parsed.real);
  return [];
}

export function upsertCotationBrl(
  db: Database.Database,
  fundCode: string,
  dateIso: string,
  dateBr: string,
  price: number
): number {
  const orm = getOrm(db);
  const fundCodeUpper = fundCode.toUpperCase();
  return orm
    .insert(cotation)
    .values({
      fund_code: fundCodeUpper,
      date_iso: dateIso,
      date: dateBr,
      price,
    })
    .onConflictDoUpdate({
      target: [cotation.fund_code, cotation.date_iso],
      set: { price, date: dateBr },
    })
    .run().changes;
}

export function upsertCotationsHistoricalBrl(db: Database.Database, fundCode: string, data: NormalizedCotations) {
  const now = nowIso();
  const items = data.real || [];
  if (items.length === 0) return 0;
  const orm = getOrm(db);
  const fundCodeUpper = fundCode.toUpperCase();

  // Batch values com date_iso válido
  const validItems = items
    .map((item) => {
      const dateIso = toDateIsoFromBr(item.date);
      return dateIso ? { ...item, dateIso } : null;
    })
    .filter((v): v is typeof v & { dateIso: string } => v !== null);

  if (validItems.length === 0) return 0;

  const cotationValues = validItems.map((item) => ({
    fund_code: fundCodeUpper,
    date_iso: item.dateIso,
    date: item.date,
    price: item.price,
  }));

  // Batch insert com upsert
  const result = orm
    .insert(cotation)
    .values(cotationValues)
    .onConflictDoUpdate({
      target: [cotation.fund_code, cotation.date_iso],
      set: {
        price: sql`excluded.price`,
        date: sql`excluded.date`,
      },
    })
    .run();

  orm.insert(fundState).values({ fund_code: fundCodeUpper, created_at: now, updated_at: now }).onConflictDoNothing().run();
  orm.update(fundState).set({ last_historical_cotations_at: now, updated_at: now }).where(eq(fundState.fund_code, fundCodeUpper)).run();

  return result.changes;
}

export function getCotations(db: Database.Database, fundCode: string, days: number): NormalizedCotations | null {
  const limit = Number.isFinite(days) && days > 0 ? Math.min(days, 5000) : 1825;
  const orm = getOrm(db);
  const rows = orm
    .select({ date: cotation.date, price: cotation.price })
    .from(cotation)
    .where(eq(cotation.fund_code, fundCode.toUpperCase()))
    .orderBy(desc(cotation.date_iso))
    .limit(limit)
    .all();

  if (rows.length === 0) return null;

  return {
    real: rows
      .slice()
      .reverse()
      .map((r) => ({ date: r.date, price: r.price })),
    dolar: [],
    euro: [],
  };
}

export function upsertDocuments(db: Database.Database, fundCode: string, docs: DocumentData[]): { inserted: number; maxId: number } {
  const now = nowIso();
  const orm = getOrm(db);
  const fundCodeUpper = fundCode.toUpperCase();

  if (docs.length === 0) return { inserted: 0, maxId: 0 };

  // Batch values
  const documentValues = docs.map((d) => ({
    fund_code: fundCodeUpper,
    document_id: d.id,
    title: d.title,
    category: d.category,
    type: d.type,
    date: d.date,
    date_upload_iso: toDateIsoFromBr(d.dateUpload) || toDateIsoFromBr(d.date) || now.slice(0, 10),
    dateUpload: d.dateUpload,
    url: d.url,
    status: d.status,
    version: d.version,
    created_at: now,
  }));

  const maxId = docs.reduce((max, d) => Math.max(max, d.id), 0);

  // Batch insert com upsert
  const result = orm
    .insert(document)
    .values(documentValues)
    .onConflictDoUpdate({
      target: [document.fund_code, document.document_id],
      set: {
        title: sql`excluded.title`,
        category: sql`excluded.category`,
        type: sql`excluded.type`,
        date: sql`excluded.date`,
        date_upload_iso: sql`excluded.date_upload_iso`,
        dateUpload: sql`excluded.dateUpload`,
        url: sql`excluded.url`,
        status: sql`excluded.status`,
        version: sql`excluded.version`,
      },
    })
    .run();

  return { inserted: result.changes, maxId };
}

export function updateDocumentsMaxId(db: Database.Database, fundCode: string, maxId: number) {
  const now = nowIso();
  const orm = getOrm(db);
  orm.insert(fundState).values({ fund_code: fundCode.toUpperCase(), created_at: now, updated_at: now }).onConflictDoNothing().run();
  orm
    .update(fundState)
    .set({ last_documents_max_id: maxId, updated_at: now })
    .where(eq(fundState.fund_code, fundCode.toUpperCase()))
    .run();
}

export function getDocuments(db: Database.Database, fundCode: string): DocumentData[] | null {
  const orm = getOrm(db);
  const rows = orm
    .select({
      id: document.document_id,
      title: document.title,
      category: document.category,
      type: document.type,
      date: document.date,
      dateUpload: document.dateUpload,
      url: document.url,
      status: document.status,
      version: document.version,
    })
    .from(document)
    .where(eq(document.fund_code, fundCode.toUpperCase()))
    .orderBy(desc(document.date_upload_iso), desc(document.document_id))
    .all();

  if (rows.length === 0) return null;
  return rows.map((r) => ({
    id: r.id,
    title: r.title,
    category: r.category,
    type: r.type,
    date: r.date,
    dateUpload: r.dateUpload,
    url: r.url,
    status: r.status,
    version: r.version,
  }));
}

export function listDocumentsSinceId(db: Database.Database, fundCode: string, minDocumentId: number, limit?: number): DocumentData[] {
  const safeLimit = Number.isFinite(limit) && (limit as number) > 0 ? Math.min(Math.floor(limit as number), 100) : 20;
  const orm = getOrm(db);
  const rows = orm
    .select({
      id: document.document_id,
      title: document.title,
      category: document.category,
      type: document.type,
      date: document.date,
      dateUpload: document.dateUpload,
      url: document.url,
      status: document.status,
      version: document.version,
    })
    .from(document)
    .where(and(eq(document.fund_code, fundCode.toUpperCase()), gt(document.document_id, Math.max(0, Math.floor(minDocumentId)))))
    .orderBy(desc(document.date_upload_iso), desc(document.document_id))
    .limit(safeLimit)
    .all();
  return rows.map((r) => ({
    id: r.id,
    title: r.title,
    category: r.category,
    type: r.type,
    date: r.date,
    dateUpload: r.dateUpload,
    url: r.url,
    status: r.status,
    version: r.version,
  }));
}

export function upsertDividends(db: Database.Database, fundCode: string, dividends: DividendData[]): number {
  const orm = getOrm(db);
  const fundCodeUpper = fundCode.toUpperCase();

  // Batch values com date_iso válido
  const validDividends = dividends
    .map((d) => {
      const dateIso = toDateIsoFromBr(d.date);
      return dateIso ? { ...d, dateIso } : null;
    })
    .filter((v): v is typeof v & { dateIso: string } => v !== null);

  if (validDividends.length === 0) return 0;

  const dividendValues = validDividends.map((d) => ({
    fund_code: fundCodeUpper,
    date_iso: d.dateIso,
    date: d.date,
    payment: d.payment,
    type: d.type,
    value: d.value,
    yield: d.yield,
  }));

  // Batch insert com upsert
  const result = orm
    .insert(dividend)
    .values(dividendValues)
    .onConflictDoUpdate({
      target: [dividend.fund_code, dividend.date_iso, dividend.type],
      set: {
        date: sql`excluded.date`,
        payment: sql`excluded.payment`,
        value: sql`excluded.value`,
        yield: sql`excluded.yield`,
      },
    })
    .run();

  return result.changes;
}

export function getDividends(db: Database.Database, fundCode: string): DividendData[] | null {
  const orm = getOrm(db);
  const rows = orm
    .select({
      date: dividend.date,
      payment: dividend.payment,
      type: dividend.type,
      value: dividend.value,
      yield: dividend.yield,
    })
    .from(dividend)
    .where(eq(dividend.fund_code, fundCode.toUpperCase()))
    .orderBy(desc(dividend.date_iso))
    .all();

  if (rows.length === 0) return null;
  return rows.map((r) => ({
    value: r.value,
    yield: r.yield,
    date: r.date,
    payment: r.payment,
    type: r.type,
  }));
}
