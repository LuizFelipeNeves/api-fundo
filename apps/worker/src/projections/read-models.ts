import postgres from 'postgres';

type Sql = ReturnType<typeof postgres>;

export function createReadModelWriter(sql: Sql) {
  async function upsertFundList(items: Array<{ code: string; sector?: string | null; p_vp?: number | null; dividend_yield?: number | null; dividend_yield_last_5_years?: number | null; daily_liquidity?: number | null; net_worth?: number | null; type?: string | null }>) {
    if (items.length === 0) return;
    await sql`
      INSERT INTO fund_list_read (code, sector, p_vp, dividend_yield, dividend_yield_last_5_years, daily_liquidity, net_worth, type, updated_at)
      VALUES ${sql(items.map((i) => [
        i.code.toUpperCase(),
        i.sector ?? null,
        i.p_vp ?? null,
        i.dividend_yield ?? null,
        i.dividend_yield_last_5_years ?? null,
        i.daily_liquidity ?? null,
        i.net_worth ?? null,
        i.type ?? null,
        new Date(),
      ]) as any)}
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

  async function upsertFundDetails(item: {
    code: string;
    id?: string | null;
    razao_social?: string | null;
    cnpj?: string | null;
    publico_alvo?: string | null;
    mandato?: string | null;
    segmento?: string | null;
    tipo_fundo?: string | null;
    prazo_duracao?: string | null;
    tipo_gestao?: string | null;
    taxa_adminstracao?: string | null;
    daily_liquidity?: number | null;
    vacancia?: number | null;
    numero_cotistas?: number | null;
    cotas_emitidas?: number | null;
    valor_patrimonial_cota?: number | null;
    valor_patrimonial?: number | null;
    ultimo_rendimento?: number | null;
  }) {
    await sql`
      INSERT INTO fund_details_read (
        code, id, razao_social, cnpj, publico_alvo, mandato, segmento, tipo_fundo, prazo_duracao, tipo_gestao,
        taxa_adminstracao, daily_liquidity, vacancia, numero_cotistas, cotas_emitidas, valor_patrimonial_cota,
        valor_patrimonial, ultimo_rendimento, updated_at
      ) VALUES (
        ${item.code.toUpperCase()},
        ${item.id ?? null},
        ${item.razao_social ?? null},
        ${item.cnpj ?? null},
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
        ${new Date()}
      )
      ON CONFLICT (code) DO UPDATE SET
        id = EXCLUDED.id,
        razao_social = EXCLUDED.razao_social,
        cnpj = EXCLUDED.cnpj,
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
  }

  async function upsertIndicatorsLatest(fundCode: string, fetchedAt: Date, dataJson: unknown) {
    await sql`
      INSERT INTO indicators_read (fund_code, fetched_at, data_json)
      VALUES (${fundCode.toUpperCase()}, ${fetchedAt}, ${sql.json(dataJson as any)})
      ON CONFLICT (fund_code) DO UPDATE SET
        fetched_at = EXCLUDED.fetched_at,
        data_json = EXCLUDED.data_json
    `;
  }

  async function insertIndicatorsSnapshot(fundCode: string, fetchedAt: Date, dataJson: unknown) {
    await sql`
      INSERT INTO indicators_snapshot_read (fund_code, fetched_at, data_json)
      VALUES (${fundCode.toUpperCase()}, ${fetchedAt}, ${sql.json(dataJson as any)})
      ON CONFLICT (fund_code, fetched_at) DO NOTHING
    `;
  }

  async function upsertCotations(items: Array<{ fund_code: string; date_iso: string; price: number }>) {
    if (items.length === 0) return;
    const uniqueByKey = new Map<string, { fund_code: string; date_iso: string; price: number }>();
    for (const item of items) {
      const fundCode = item.fund_code.toUpperCase();
      const key = `${fundCode}|${item.date_iso}`;
      uniqueByKey.set(key, { ...item, fund_code: fundCode });
    }
    const uniqueItems = Array.from(uniqueByKey.values());
    await sql`
      INSERT INTO cotations_read (fund_code, date_iso, price)
      VALUES ${sql(uniqueItems.map((i) => [i.fund_code, i.date_iso, i.price]) as any)}
      ON CONFLICT (fund_code, date_iso) DO UPDATE SET
        price = EXCLUDED.price
    `;
  }

  async function insertCotationsToday(fundCode: string, dateIso: string, fetchedAt: Date, dataJson: unknown) {
    await sql`
      INSERT INTO cotations_today_read (fund_code, date_iso, fetched_at, data_json)
      VALUES (${fundCode.toUpperCase()}, ${dateIso}, ${fetchedAt}, ${sql.json(dataJson as any)})
      ON CONFLICT (fund_code, date_iso, fetched_at) DO UPDATE SET
        data_json = EXCLUDED.data_json
    `;
  }

  async function upsertDividends(items: Array<{ fund_code: string; date_iso: string; payment: string; type: number; value: number; yield: number }>) {
    if (items.length === 0) return;
    const uniqueByKey = new Map<string, { fund_code: string; date_iso: string; payment: string; type: number; value: number; yield: number }>();
    for (const item of items) {
      const fundCode = item.fund_code.toUpperCase();
      const key = `${fundCode}|${item.date_iso}|${item.type}`;
      uniqueByKey.set(key, { ...item, fund_code: fundCode });
    }
    const uniqueItems = Array.from(uniqueByKey.values());
    await sql`
      INSERT INTO dividends_read (fund_code, date_iso, payment, type, value, yield)
      VALUES ${sql(uniqueItems.map((i) => [
        i.fund_code,
        i.date_iso,
        i.payment,
        i.type,
        i.value,
        i.yield,
      ]) as any)}
      ON CONFLICT (fund_code, date_iso, type) DO UPDATE SET
        payment = EXCLUDED.payment,
        value = EXCLUDED.value,
        yield = EXCLUDED.yield
    `;
  }

  async function upsertDocuments(items: Array<{ fund_code: string; document_id: number; title: string; category: string; type: string; date: string; date_upload_iso: string; dateUpload: string; url: string; status: string; version: number }>) {
    if (items.length === 0) return;
    await sql`
      INSERT INTO documents_read (fund_code, document_id, title, category, type, date, date_upload_iso, "dateUpload", url, status, version)
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
      ]) as any)}
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
  }

  return {
    upsertFundList,
    upsertFundDetails,
    upsertIndicatorsLatest,
    insertIndicatorsSnapshot,
    upsertCotations,
    insertCotationsToday,
    upsertDividends,
    upsertDocuments,
  };
}
