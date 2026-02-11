import type { Sql } from './db';
import type {
  PersistFundListItem,
  PersistFundDetailsItem,
  PersistIndicators,
  PersistCotation,
  PersistCotationsToday,
  PersistDividend,
  PersistDocument,
} from './messages';

export function createWriteSide(sql: Sql) {
  async function touchFundState(fundCode: string, fields: Record<string, unknown>) {
    const now = new Date();
    await sql`
      INSERT INTO fund_state (fund_code, created_at, updated_at)
      VALUES (${fundCode.toUpperCase()}, ${now}, ${now})
      ON CONFLICT (fund_code) DO UPDATE SET
        updated_at = EXCLUDED.updated_at
    `;

    const keys = Object.keys(fields);
    if (keys.length === 0) return;
    const assignments = keys.map((k) => sql`${sql(k)} = ${fields[k]}`);
    await sql`
      UPDATE fund_state
      SET ${sql(assignments)}, updated_at = ${now}
      WHERE fund_code = ${fundCode.toUpperCase()}
    `;
  }
  async function upsertFundList(items: PersistFundListItem[]) {
    if (items.length === 0) return;
    const now = new Date();
    await sql`
      INSERT INTO fund_master (code, sector, p_vp, dividend_yield, dividend_yield_last_5_years, daily_liquidity, net_worth, type, created_at, updated_at)
      VALUES ${sql(items.map((i) => [
        i.code.toUpperCase(),
        i.sector ?? null,
        i.p_vp ?? null,
        i.dividend_yield ?? null,
        i.dividend_yield_last_5_years ?? null,
        i.daily_liquidity ?? null,
        i.net_worth ?? null,
        i.type ?? null,
        now,
        now,
      ]))}
      ON CONFLICT (code) DO UPDATE SET
        sector = EXCLUDED.sector,
        p_vp = EXCLUDED.p_vp,
        dividend_yield = EXCLUDED.dividend_yield,
        dividend_yield_last_5_years = EXCLUDED.dividend_yield_last_5_years,
        daily_liquidity = EXCLUDED.daily_liquidity,
        net_worth = EXCLUDED.net_worth,
        type = EXCLUDED.type,
        updated_at = EXCLUDED.updated_at
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
    await sql`
      INSERT INTO indicators_snapshot (fund_code, fetched_at, data_hash, data_json)
      VALUES (${item.fund_code.toUpperCase()}, ${fetchedAt}, ${item.data_hash}, ${sql.json(item.data_json)})
      ON CONFLICT (fund_code) DO UPDATE SET
        fetched_at = EXCLUDED.fetched_at,
        data_hash = EXCLUDED.data_hash,
        data_json = EXCLUDED.data_json
    `;

    await touchFundState(item.fund_code, {
      last_indicators_at: fetchedAt,
      last_indicators_hash: item.data_hash,
    });
  }

  async function upsertCotations(items: PersistCotation[]) {
    if (items.length === 0) return;
    await sql`
      INSERT INTO cotation (fund_code, date_iso, price)
      VALUES ${sql(items.map((i) => [i.fund_code.toUpperCase(), i.date_iso, i.price]))}
      ON CONFLICT (fund_code, date_iso) DO UPDATE SET
        price = EXCLUDED.price
    `;

    const now = new Date();
    const codes = Array.from(new Set(items.map((i) => i.fund_code.toUpperCase())));
    for (const code of codes) {
      await touchFundState(code, { last_historical_cotations_at: now });
    }
  }

  async function upsertCotationsToday(item: PersistCotationsToday) {
    await sql`
      INSERT INTO cotations_today_snapshot (fund_code, date_iso, fetched_at, data_json)
      VALUES (${item.fund_code.toUpperCase()}, ${item.date_iso}, ${new Date(item.fetched_at)}, ${sql.json(item.data_json)})
      ON CONFLICT (fund_code, date_iso) DO UPDATE SET
        fetched_at = EXCLUDED.fetched_at,
        data_json = EXCLUDED.data_json
    `;

    await touchFundState(item.fund_code, { last_cotations_today_at: new Date(item.fetched_at) });
  }

  async function upsertDividends(items: PersistDividend[]) {
    if (items.length === 0) return;
    await sql`
      INSERT INTO dividend (fund_code, date_iso, payment, type, value, yield)
      VALUES ${sql(items.map((i) => [
        i.fund_code.toUpperCase(),
        i.date_iso,
        i.payment,
        i.type,
        i.value,
        i.yield,
      ]))}
      ON CONFLICT (fund_code, date_iso, type) DO UPDATE SET
        payment = EXCLUDED.payment,
        value = EXCLUDED.value,
        yield = EXCLUDED.yield
    `;
  }

  async function upsertDocuments(items: PersistDocument[]) {
    if (items.length === 0) return;
    await sql`
      INSERT INTO document (fund_code, document_id, title, category, type, date, date_upload_iso, "dateUpload", url, status, version, created_at)
      VALUES ${sql(items.map((i) => [
        i.fund_code.toUpperCase(),
        i.document_id,
        i.title,
        i.category,
        i.type,
        i.date,
        i.date_upload_iso,
        i.dateUpload,
        i.url,
        i.status,
        i.version,
        new Date(),
      ]))}
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
    `;

    const now = new Date();
    const byFund = new Map<string, number>();
    for (const item of items) {
      const key = item.fund_code.toUpperCase();
      const current = byFund.get(key) ?? 0;
      if (item.document_id > current) byFund.set(key, item.document_id);
    }
    for (const [code, maxId] of byFund.entries()) {
      await touchFundState(code, { last_documents_at: now, last_documents_max_id: maxId });
    }
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
