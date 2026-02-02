import type Database from 'better-sqlite3';
import { nowIso } from './index';
import type { FIIResponse, FIIDetails } from '../types';
import type { NormalizedIndicators } from '../parsers/indicators';
import type { CotationsTodayData } from '../parsers/today';
import type { DocumentData } from '../parsers/documents';
import type { DividendData } from '../parsers/dividends';
import type { NormalizedCotations } from '../parsers/cotations';
import { toDateIsoFromBr } from './index';
import { drizzle } from 'drizzle-orm/better-sqlite3';
import { and, asc, desc, eq, gt, isNotNull, isNull, or } from 'drizzle-orm';
import { cotation, cotationsTodaySnapshot, dividend, document, fundMaster, fundState, indicatorsSnapshot } from './schema';

export function upsertFundList(db: Database.Database, data: FIIResponse) {
  const now = nowIso();
  const orm = drizzle(db);

  orm.transaction((tx) => {
    for (const item of data.data) {
      const code = item.code.toUpperCase();

      tx.insert(fundMaster)
        .values({
          code,
          sector: item.sector,
          p_vp: item.p_vp,
          dividend_yield: item.dividend_yield,
          dividend_yield_last_5_years: item.dividend_yield_last_5_years,
          daily_liquidity: item.daily_liquidity,
          net_worth: item.net_worth,
          type: item.type,
          created_at: now,
          updated_at: now,
        })
        .onConflictDoUpdate({
          target: fundMaster.code,
          set: {
            sector: item.sector,
            p_vp: item.p_vp,
            dividend_yield: item.dividend_yield,
            dividend_yield_last_5_years: item.dividend_yield_last_5_years,
            daily_liquidity: item.daily_liquidity,
            net_worth: item.net_worth,
            type: item.type,
            updated_at: now,
          },
        })
        .run();

      tx.insert(fundState)
        .values({
          fund_code: code,
          created_at: now,
          updated_at: now,
        })
        .onConflictDoNothing()
        .run();
    }
  });
}

export function updateFundDetails(db: Database.Database, details: FIIDetails) {
  const now = nowIso();
  const orm = drizzle(db);
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
  const orm = drizzle(db);
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
  const orm = drizzle(db);
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
    vacancia: row.vacancia ?? 0,
    numero_cotistas: row.numero_cotistas ?? 0,
    cotas_emitidas: row.cotas_emitidas ?? 0,
    valor_patrimonial_cota: row.valor_patrimonial_cota ?? 0,
    valor_patrimonial: row.valor_patrimonial ?? 0,
    ultimo_rendimento: row.ultimo_rendimento ?? 0,
  };
}

export function listFundCodes(db: Database.Database): string[] {
  const orm = drizzle(db);
  const rows = orm.select({ code: fundMaster.code }).from(fundMaster).orderBy(asc(fundMaster.code)).all();
  return rows.map((r) => r.code);
}

export function listFundCodesWithId(db: Database.Database): string[] {
  const orm = drizzle(db);
  const rows = orm
    .select({ code: fundMaster.code })
    .from(fundMaster)
    .where(isNotNull(fundMaster.id))
    .orderBy(asc(fundMaster.code))
    .all();
  return rows.map((r) => r.code);
}

export function listFundCodesWithCnpj(db: Database.Database): string[] {
  const orm = drizzle(db);
  const rows = orm
    .select({ code: fundMaster.code })
    .from(fundMaster)
    .where(isNotNull(fundMaster.cnpj))
    .orderBy(asc(fundMaster.code))
    .all();
  return rows.map((r) => r.code);
}

export function listFundCodesForIndicatorsBatch(db: Database.Database, limit: number): string[] {
  const orm = drizzle(db);
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
  const orm = drizzle(db);
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
  const orm = drizzle(db);
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
  const orm = drizzle(db);
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
  const orm = drizzle(db);
  const rows = orm
    .select({ code: fundMaster.code })
    .from(fundMaster)
    .where(or(isNull(fundMaster.id), isNull(fundMaster.cnpj)))
    .orderBy(asc(fundMaster.code))
    .all();
  return rows.map((r) => r.code);
}

export function getFundIdAndCnpj(db: Database.Database, code: string): { id: string | null; cnpj: string | null } | null {
  const orm = drizzle(db);
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
  const orm = drizzle(db);
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
  const orm = drizzle(db);
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
  const orm = drizzle(db);
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
  const orm = drizzle(db);
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
  const orm = drizzle(db);
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
  const orm = drizzle(db);
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
  const orm = drizzle(db);
  const fundCodeUpper = fundCode.toUpperCase();
  const json = JSON.stringify(data);
  const inserted = orm
    .insert(indicatorsSnapshot)
    .values({
      fund_code: fundCodeUpper,
      fetched_at: fetchedAt,
      data_hash: dataHash,
      data_json: json,
    })
    .onConflictDoNothing({ target: [indicatorsSnapshot.fund_code, indicatorsSnapshot.data_hash] })
    .run().changes;

  orm
    .insert(fundState)
    .values({ fund_code: fundCodeUpper, created_at: fetchedAt, updated_at: fetchedAt })
    .onConflictDoNothing()
    .run();

  if (inserted > 0) {
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

  return inserted > 0;
}

export function getLatestIndicators(db: Database.Database, fundCode: string): NormalizedIndicators | null {
  const orm = drizzle(db);
  const row = orm
    .select({ data_json: indicatorsSnapshot.data_json })
    .from(indicatorsSnapshot)
    .where(eq(indicatorsSnapshot.fund_code, fundCode.toUpperCase()))
    .orderBy(desc(indicatorsSnapshot.fetched_at))
    .limit(1)
    .all()[0];
  if (!row) return null;
  return JSON.parse(row.data_json) as NormalizedIndicators;
}

export function upsertCotationsTodaySnapshot(db: Database.Database, fundCode: string, fetchedAt: string, dataHash: string, data: CotationsTodayData) {
  const orm = drizzle(db);
  const fundCodeUpper = fundCode.toUpperCase();
  const json = JSON.stringify(data);
  const inserted = orm
    .insert(cotationsTodaySnapshot)
    .values({
      fund_code: fundCodeUpper,
      fetched_at: fetchedAt,
      data_hash: dataHash,
      data_json: json,
    })
    .onConflictDoNothing({ target: [cotationsTodaySnapshot.fund_code, cotationsTodaySnapshot.data_hash] })
    .run().changes;

  orm
    .insert(fundState)
    .values({ fund_code: fundCodeUpper, created_at: fetchedAt, updated_at: fetchedAt })
    .onConflictDoNothing()
    .run();

  if (inserted > 0) {
    orm
      .update(fundState)
      .set({
        last_cotations_today_hash: dataHash,
        last_cotations_today_at: fetchedAt,
        updated_at: fetchedAt,
      })
      .where(eq(fundState.fund_code, fundCodeUpper))
      .run();
  } else {
    orm
      .update(fundState)
      .set({
        last_cotations_today_at: fetchedAt,
        updated_at: fetchedAt,
      })
      .where(eq(fundState.fund_code, fundCodeUpper))
      .run();
  }

  return inserted > 0;
}

export function getLatestCotationsToday(db: Database.Database, fundCode: string): CotationsTodayData | null {
  const orm = drizzle(db);
  const row = orm
    .select({ data_json: cotationsTodaySnapshot.data_json })
    .from(cotationsTodaySnapshot)
    .where(eq(cotationsTodaySnapshot.fund_code, fundCode.toUpperCase()))
    .orderBy(desc(cotationsTodaySnapshot.fetched_at))
    .limit(1)
    .all()[0];
  if (!row) return null;
  const parsed: any = JSON.parse(row.data_json);
  if (Array.isArray(parsed)) return parsed;
  if (Array.isArray(parsed?.real)) return parsed.real;
  return [];
}

export function upsertCotationBrl(
  db: Database.Database,
  fundCode: string,
  dateIso: string,
  dateBr: string,
  price: number
): number {
  const orm = drizzle(db);
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
  const orm = drizzle(db);
  const fundCodeUpper = fundCode.toUpperCase();

  return orm.transaction((tx) => {
    let changes = 0;
    for (const item of items) {
      const dateIso = toDateIsoFromBr(item.date);
      if (!dateIso) continue;
      changes += tx
        .insert(cotation)
        .values({
          fund_code: fundCodeUpper,
          date_iso: dateIso,
          date: item.date,
          price: item.price,
        })
        .onConflictDoUpdate({
          target: [cotation.fund_code, cotation.date_iso],
          set: { price: item.price, date: item.date },
        })
        .run().changes;
    }

    tx.insert(fundState).values({ fund_code: fundCodeUpper, created_at: now, updated_at: now }).onConflictDoNothing().run();
    tx.update(fundState).set({ last_historical_cotations_at: now, updated_at: now }).where(eq(fundState.fund_code, fundCodeUpper)).run();

    return changes;
  });
}

export function getCotations(db: Database.Database, fundCode: string, days: number): NormalizedCotations | null {
  const limit = Number.isFinite(days) && days > 0 ? Math.min(days, 5000) : 1825;
  const orm = drizzle(db);
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
  const orm = drizzle(db);
  const fundCodeUpper = fundCode.toUpperCase();

  return orm.transaction((tx) => {
    let inserted = 0;
    let maxId = 0;
    for (const d of docs) {
      maxId = Math.max(maxId, d.id);
      const uploadIso = toDateIsoFromBr(d.dateUpload) || toDateIsoFromBr(d.date) || now.slice(0, 10);

      inserted += tx
        .insert(document)
        .values({
          fund_code: fundCodeUpper,
          document_id: d.id,
          title: d.title,
          category: d.category,
          type: d.type,
          date: d.date,
          date_upload_iso: uploadIso,
          dateUpload: d.dateUpload,
          url: d.url,
          status: d.status,
          version: d.version,
          created_at: now,
        })
        .onConflictDoUpdate({
          target: [document.fund_code, document.document_id],
          set: {
            title: d.title,
            category: d.category,
            type: d.type,
            date: d.date,
            date_upload_iso: uploadIso,
            dateUpload: d.dateUpload,
            url: d.url,
            status: d.status,
            version: d.version,
          },
        })
        .run().changes;
    }

    return { inserted, maxId };
  });
}

export function updateDocumentsMaxId(db: Database.Database, fundCode: string, maxId: number) {
  const now = nowIso();
  const orm = drizzle(db);
  orm.insert(fundState).values({ fund_code: fundCode.toUpperCase(), created_at: now, updated_at: now }).onConflictDoNothing().run();
  orm
    .update(fundState)
    .set({ last_documents_max_id: maxId, updated_at: now })
    .where(eq(fundState.fund_code, fundCode.toUpperCase()))
    .run();
}

export function getDocuments(db: Database.Database, fundCode: string): DocumentData[] | null {
  const orm = drizzle(db);
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
  const orm = drizzle(db);
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
  const orm = drizzle(db);
  const fundCodeUpper = fundCode.toUpperCase();

  return orm.transaction((tx) => {
    let changes = 0;
    for (const d of dividends) {
      const dateIso = toDateIsoFromBr(d.date);
      if (!dateIso) continue;
      changes += tx
        .insert(dividend)
        .values({
          fund_code: fundCodeUpper,
          date_iso: dateIso,
          date: d.date,
          payment: d.payment,
          type: d.type,
          value: d.value,
          yield: d.yield,
        })
        .onConflictDoUpdate({
          target: [dividend.fund_code, dividend.date_iso, dividend.type],
          set: {
            date: d.date,
            payment: d.payment,
            value: d.value,
            yield: d.yield,
          },
        })
        .run().changes;
    }
    return changes;
  });
}

export function getDividends(db: Database.Database, fundCode: string): DividendData[] | null {
  const orm = drizzle(db);
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
