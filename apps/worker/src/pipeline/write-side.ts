import { getRawSql } from '../db';
import type {
  PersistFundListItem,
  PersistFundDetailsItem,
  PersistIndicators,
  PersistCotation,
  PersistCotationsToday,
  PersistDividend,
  PersistDocument,
} from './messages';

export function createWriteSide() {
  const sql = getRawSql();

  async function touchFundState(fundCode: string, fields: Record<string, string | number | boolean | Date | null>) {
    const now = new Date();
    const code = fundCode.toUpperCase();

    await sql`
      INSERT INTO fund_state (fund_code, created_at, updated_at)
      VALUES (${code}, ${now}, ${now})
      ON CONFLICT (fund_code) DO UPDATE SET
        updated_at = EXCLUDED.updated_at
    `;

    const keys = Object.keys(fields);
    if (keys.length === 0) return;

    const fieldValues = keys.map((k) => fields[k]);
    const paramPlaceholders = keys.map((_, i) => `"${keys[i]}" = $${i + 1}`).join(', ');
    const params = [...fieldValues, now, code] as Array<string | number | boolean | Date | null>;
    await sql.unsafe(
      `UPDATE fund_state SET ${paramPlaceholders}, updated_at = $${keys.length + 1} WHERE fund_code = $${keys.length + 2}`,
      params as any
    );
  }

  async function upsertFundList(items: PersistFundListItem[]) {
    if (items.length === 0) return;
    const now = new Date();
    const uniqueByCode = new Map<string, PersistFundListItem>();
    for (const item of items) uniqueByCode.set(item.code.toUpperCase(), item);
    const uniqueItems = Array.from(uniqueByCode.entries()).map(([code, item]) => [
      code,
      item.sector ?? null,
      item.p_vp ?? null,
      item.dividend_yield ?? null,
      item.dividend_yield_last_5_years ?? null,
      item.daily_liquidity ?? null,
      item.net_worth ?? null,
      item.type ?? null,
      now,
      now,
    ]);

    await sql`
      INSERT INTO fund_master (code, sector, p_vp, dividend_yield, dividend_yield_last_5_years, daily_liquidity, net_worth, type, created_at, updated_at)
      VALUES ${sql(uniqueItems as any)}
      ON CONFLICT (code) DO UPDATE SET
        sector = EXCLUDED.sector,
        p_vp = EXCLUDED.p_vp,
        dividend_yield = EXCLUDED.dividend_yield,
        dividend_yield_last_5_years = EXCLUDED.dividend_yield_last_5_years,
        daily_liquidity = EXCLUDED.daily_liquidity,
        net_worth = EXCLUDED.net_worth,
        type = EXCLUDED.type,
        updated_at = EXCLUDED.updated_at
      WHERE
        fund_master.sector IS DISTINCT FROM EXCLUDED.sector OR
        fund_master.p_vp IS DISTINCT FROM EXCLUDED.p_vp OR
        fund_master.dividend_yield IS DISTINCT FROM EXCLUDED.dividend_yield OR
        fund_master.dividend_yield_last_5_years IS DISTINCT FROM EXCLUDED.dividend_yield_last_5_years OR
        fund_master.daily_liquidity IS DISTINCT FROM EXCLUDED.daily_liquidity OR
        fund_master.net_worth IS DISTINCT FROM EXCLUDED.net_worth OR
        fund_master.type IS DISTINCT FROM EXCLUDED.type
    `;
  }

  async function upsertFundDetails(item: PersistFundDetailsItem) {
    const now = new Date();
    await sql`
      INSERT INTO fund_master (
        code, id, cnpj, razao_social, publico_alvo, mandato, segmento, tipo_fundo, prazo_duracao, tipo_gestao,
        taxa_adminstracao, daily_liquidity, vacancia, numero_cotistas, cotas_emitidas, valor_patrimonial_cota,
        valor_patrimonial, ultimo_rendimento, updated_at
      ) VALUES (
        ${item.code.toUpperCase()},
        ${item.id ?? null},
        ${item.cnpj ?? null},
        ${item.razao_social ?? null},
        ${item.publico_alvo ?? null},
        ${item.mandato ?? null},
        ${item.segmento ?? null},
        ${item.tipo_fundo ?? null},
        ${item.prazo_duracao ?? null},
        ${item.tipo_gestao ?? null},
        ${item.taxa_adminstracao ?? null},
        ${item.daily_liquidity ?? null},
        ${item.vacancia ?? null},
        ${item.numero_cotistas ?? null},
        ${item.cotas_emitidas ?? null},
        ${item.valor_patrimonial_cota ?? null},
        ${item.valor_patrimonial ?? null},
        ${item.ultimo_rendimento ?? null},
        ${now}
      )
      ON CONFLICT (code) DO UPDATE SET
        id = EXCLUDED.id,
        cnpj = EXCLUDED.cnpj,
        razao_social = EXCLUDED.razao_social,
        publico_alvo = EXCLUDED.publico_alvo,
        mandato = EXCLUDED.mandato,
        segmento = EXCLUDED.segmento,
        tipo_fundo = EXCLUDED.tipo_fundo,
        prazo_duracao = EXCLUDED.prazo_duracao,
        tipo_gestao = EXCLUDED.tipo_gestao,
        taxa_adminstracao = EXCLUDED.taxa_adminstracao,
        daily_liquidity = EXCLUDED.daily_liquidity,
        vacancia = EXCLUDED.vacancia,
        numero_cotistas = EXCLUDED.numero_cotistas,
        cotas_emitidas = EXCLUDED.cotas_emitidas,
        valor_patrimonial_cota = EXCLUDED.valor_patrimonial_cota,
        valor_patrimonial = EXCLUDED.valor_patrimonial,
        ultimo_rendimento = EXCLUDED.ultimo_rendimento,
        updated_at = EXCLUDED.updated_at
    `;

    await touchFundState(item.code, { last_details_sync_at: now });
  }

  async function upsertIndicators(item: PersistIndicators) {
    const fetchedAt = new Date(item.fetched_at);
    const dataJson = typeof item.data_json === 'string' ? item.data_json : JSON.stringify(item.data_json);
    await sql`
      INSERT INTO indicators_snapshot (fund_code, fetched_at, data_hash, data_json)
      VALUES (${item.fund_code.toUpperCase()}, ${fetchedAt}, ${item.data_hash}, ${dataJson}::jsonb)
      ON CONFLICT (fund_code) DO UPDATE SET
        fetched_at = EXCLUDED.fetched_at,
        data_hash = EXCLUDED.data_hash,
        data_json = EXCLUDED.data_json
      WHERE
        indicators_snapshot.data_hash IS DISTINCT FROM EXCLUDED.data_hash OR
        indicators_snapshot.fetched_at IS DISTINCT FROM EXCLUDED.fetched_at
    `;

    await touchFundState(item.fund_code, {
      last_indicators_at: fetchedAt,
      last_indicators_hash: item.data_hash,
    });
  }

  async function upsertCotations(items: PersistCotation[]) {
    if (items.length === 0) return;
    const now = new Date();
    const uniqueByKey = new Map<string, [fund_code: string, date_iso: string, price: number]>();
    const codesSet = new Set<string>();
    for (const item of items) {
      const fundCode = item.fund_code.toUpperCase();
      codesSet.add(fundCode);
      uniqueByKey.set(`${fundCode}|${item.date_iso}`, [fundCode, item.date_iso, item.price]);
    }
    const uniqueRows = Array.from(uniqueByKey.values());

    await sql`
      INSERT INTO cotation (fund_code, date_iso, price)
      VALUES ${sql(uniqueRows as any)}
      ON CONFLICT (fund_code, date_iso) DO UPDATE SET
        price = EXCLUDED.price
      WHERE
        cotation.price IS DISTINCT FROM EXCLUDED.price
    `;

    const codes = Array.from(codesSet);
    await sql`
      INSERT INTO fund_state (fund_code, last_historical_cotations_at, created_at, updated_at)
      VALUES ${sql(codes.map((code) => [code, now, now, now]) as any)}
      ON CONFLICT (fund_code) DO UPDATE SET
        last_historical_cotations_at = EXCLUDED.last_historical_cotations_at,
        updated_at = EXCLUDED.updated_at
    `;
  }

  async function upsertCotationsToday(item: PersistCotationsToday) {
    const dataJson = typeof item.data_json === 'string' ? item.data_json : JSON.stringify(item.data_json);
    await sql`
      INSERT INTO cotations_today_snapshot (fund_code, date_iso, fetched_at, data_json)
      VALUES (${item.fund_code.toUpperCase()}, ${item.date_iso}, ${new Date(item.fetched_at)}, ${dataJson}::jsonb)
      ON CONFLICT (fund_code, date_iso) DO UPDATE SET
        fetched_at = EXCLUDED.fetched_at,
        data_json = EXCLUDED.data_json
      WHERE
        cotations_today_snapshot.fetched_at IS DISTINCT FROM EXCLUDED.fetched_at OR
        cotations_today_snapshot.data_json IS DISTINCT FROM EXCLUDED.data_json
    `;

    await touchFundState(item.fund_code, { last_cotations_today_at: new Date(item.fetched_at) });
  }

  async function upsertDividends(items: PersistDividend[]) {
    if (items.length === 0) return;
    const uniqueByKey = new Map<string, [fund_code: string, date_iso: string, payment: string, type: number, value: number, yieldValue: number]>();
    for (const item of items) {
      const fundCode = item.fund_code.toUpperCase();
      uniqueByKey.set(`${fundCode}|${item.date_iso}|${item.type}`, [
        fundCode,
        item.date_iso,
        item.payment,
        item.type,
        item.value,
        item.yield,
      ]);
    }

    const uniqueRows = Array.from(uniqueByKey.values());
    await sql`
      INSERT INTO dividend (fund_code, date_iso, payment, type, value, yield)
      VALUES ${sql(uniqueRows as any)}
      ON CONFLICT (fund_code, date_iso, type) DO UPDATE SET
        payment = EXCLUDED.payment,
        value = EXCLUDED.value,
        yield = EXCLUDED.yield
      WHERE
        dividend.payment IS DISTINCT FROM EXCLUDED.payment OR
        dividend.value IS DISTINCT FROM EXCLUDED.value OR
        dividend.yield IS DISTINCT FROM EXCLUDED.yield
    `;
  }

  async function upsertDocuments(items: PersistDocument[]) {
    if (items.length === 0) return;
    const now = new Date();
    const uniqueByKey = new Map<
      string,
      [
        fund_code: string,
        document_id: number,
        title: string,
        category: string,
        type: string,
        date: string,
        date_upload_iso: string,
        dateUpload: string,
        url: string,
        status: string,
        version: number,
      ]
    >();
    for (const item of items) {
      const fundCode = item.fund_code.toUpperCase();
      uniqueByKey.set(`${fundCode}|${item.document_id}`, [
        fundCode,
        item.document_id,
        item.title,
        item.category,
        item.type,
        item.date,
        item.date_upload_iso,
        item.dateUpload,
        item.url,
        item.status,
        item.version,
      ]);
    }
    const uniqueRows = Array.from(uniqueByKey.values());

    await sql`
      INSERT INTO document (fund_code, document_id, title, category, type, date, date_upload_iso, "dateUpload", url, status, version, created_at)
      VALUES ${sql(uniqueRows.map((r) => [...r, now]) as any)}
      ON CONFLICT (fund_code, document_id) DO UPDATE SET
        title = EXCLUDED.title,
        category = EXCLUDED.category,
        type = EXCLUDED.type,
        date = EXCLUDED.date,
        date_upload_iso = EXCLUDED.date_upload_iso,
        "dateUpload" = EXCLUDED."dateUpload",
        url = EXCLUDED.url,
        status = EXCLUDED.status,
        version = EXCLUDED.version
      WHERE
        document.title IS DISTINCT FROM EXCLUDED.title OR
        document.category IS DISTINCT FROM EXCLUDED.category OR
        document.type IS DISTINCT FROM EXCLUDED.type OR
        document.date IS DISTINCT FROM EXCLUDED.date OR
        document.date_upload_iso IS DISTINCT FROM EXCLUDED.date_upload_iso OR
        document."dateUpload" IS DISTINCT FROM EXCLUDED."dateUpload" OR
        document.url IS DISTINCT FROM EXCLUDED.url OR
        document.status IS DISTINCT FROM EXCLUDED.status OR
        document.version IS DISTINCT FROM EXCLUDED.version
    `;

    const byFund = new Map<string, number>();
    for (const r of uniqueRows) {
      const fundCode = r[0];
      const documentId = r[1];
      const current = byFund.get(fundCode) ?? 0;
      if (documentId > current) byFund.set(fundCode, documentId);
    }
    const stateRows = Array.from(byFund.entries()).map(([code, maxId]) => [code, now, maxId, now, now]);
    await sql`
      INSERT INTO fund_state (fund_code, last_documents_at, last_documents_max_id, created_at, updated_at)
      VALUES ${sql(stateRows as any)}
      ON CONFLICT (fund_code) DO UPDATE SET
        last_documents_at = EXCLUDED.last_documents_at,
        last_documents_max_id = GREATEST(COALESCE(fund_state.last_documents_max_id, 0), COALESCE(EXCLUDED.last_documents_max_id, 0)),
        updated_at = EXCLUDED.updated_at
    `;
  }

  return {
    upsertFundList,
    upsertFundDetails,
    upsertIndicators,
    upsertCotations,
    upsertCotationsToday,
    upsertDividends,
    upsertDocuments,
  };
}
