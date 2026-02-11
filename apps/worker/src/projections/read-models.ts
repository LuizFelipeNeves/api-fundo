import { getWriteDb } from '../db';
import { fundListRead, fundDetailsRead, indicatorsRead, indicatorsSnapshotRead, cotationsRead, cotationsTodayRead, dividendsRead, documentsRead } from '../db/schema';

export function createReadModelWriter() {
  const db = getWriteDb();

  async function upsertFundList(items: Array<{ code: string; sector?: string | null; p_vp?: number | null; dividend_yield?: number | null; dividend_yield_last_5_years?: number | null; daily_liquidity?: number | null; net_worth?: number | null; type?: string | null }>) {
    if (items.length === 0) return;
    const now = new Date();
    for (const item of items) {
      await db.insert(fundListRead)
        .values({
          code: item.code.toUpperCase(),
          sector: item.sector ?? null,
          pVp: item.p_vp ?? null,
          dividendYield: item.dividend_yield ?? null,
          dividendYieldLast5Years: item.dividend_yield_last_5_years ?? null,
          dailyLiquidity: item.daily_liquidity ?? null,
          netWorth: item.net_worth ?? null,
          type: item.type ?? null,
          updatedAt: now,
        })
        .onConflictDoUpdate({
          target: fundListRead.code,
          set: {
            sector: item.sector ?? null,
            pVp: item.p_vp ?? null,
            dividendYield: item.dividend_yield ?? null,
            dividendYieldLast5Years: item.dividend_yield_last_5_years ?? null,
            dailyLiquidity: item.daily_liquidity ?? null,
            netWorth: item.net_worth ?? null,
            type: item.type ?? null,
            updatedAt: now,
          },
        });
    }
  }

  async function upsertFundDetails(item: {
    code: string;
    id?: string | null;
    razao_social?: string | null;
    cnpj?: string | null;
    público_alvo?: string | null;
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
    const now = new Date();
    await db.insert(fundDetailsRead)
      .values({
        code: item.code.toUpperCase(),
        id: item.id ?? null,
        razaoSocial: item.razao_social ?? null,
        cnpj: item.cnpj ?? null,
        publicAlvo: item.público_alvo ?? null,
        mandato: item.mandato ?? null,
        segmento: item.segmento ?? null,
        tipoFundo: item.tipo_fundo ?? null,
        prazoDuracao: item.prazo_duracao ?? null,
        tipoGestao: item.tipo_gestao ?? null,
        taxaAdministracao: item.taxa_adminstracao ?? null,
        dailyLiquidity: item.daily_liquidity ?? null,
        vacancia: item.vacancia ?? null,
        numeroCotistas: item.numero_cotistas ?? null,
        cotasEmitidas: item.cotas_emitidas != null ? BigInt(item.cotas_emitidas) : null,
        valorPatrimonialCota: item.valor_patrimonial_cota ?? null,
        valorPatrimonial: item.valor_patrimonial ?? null,
        ultimoRendimento: item.ultimo_rendimento ?? null,
        updatedAt: now,
      })
      .onConflictDoUpdate({
        target: fundDetailsRead.code,
        set: {
          id: item.id ?? null,
          razaoSocial: item.razao_social ?? null,
          cnpj: item.cnpj ?? null,
          publicAlvo: item.público_alvo ?? null,
          mandato: item.mandato ?? null,
          segmento: item.segmento ?? null,
          tipoFundo: item.tipo_fundo ?? null,
          prazoDuracao: item.prazo_duracao ?? null,
          tipoGestao: item.tipo_gestao ?? null,
          taxaAdministracao: item.taxa_adminstracao ?? null,
          dailyLiquidity: item.daily_liquidity ?? null,
          vacancia: item.vacancia ?? null,
          numeroCotistas: item.numero_cotistas ?? null,
          cotasEmitidas: item.cotas_emitidas != null ? BigInt(item.cotas_emitidas) : null,
          valorPatrimonialCota: item.valor_patrimonial_cota ?? null,
          valorPatrimonial: item.valor_patrimonial ?? null,
          ultimoRendimento: item.ultimo_rendimento ?? null,
          updatedAt: now,
        },
      });
  }

  async function upsertIndicatorsLatest(fundCode: string, fetchedAt: Date, dataJson: unknown) {
    await db.insert(indicatorsRead)
      .values({
        fundCode: fundCode.toUpperCase(),
        fetchedAt,
        dataJson: dataJson as any,
      })
      .onConflictDoUpdate({
        target: indicatorsRead.fundCode,
        set: {
          fetchedAt,
          dataJson: dataJson as any,
        },
      });
  }

  async function insertIndicatorsSnapshot(fundCode: string, fetchedAt: Date, dataJson: unknown) {
    await db.insert(indicatorsSnapshotRead)
      .values({
        fundCode: fundCode.toUpperCase(),
        fetchedAt,
        dataJson: dataJson as any,
      })
      .onConflictDoNothing();
  }

  async function upsertCotations(items: Array<{ fund_code: string; date_iso: string; price: number }>) {
    if (items.length === 0) return;
    for (const item of items) {
      await db.insert(cotationsRead)
        .values({
          fundCode: item.fund_code.toUpperCase(),
          dateIso: item.date_iso,
          price: item.price,
        })
        .onConflictDoUpdate({
          target: [cotationsRead.fundCode, cotationsRead.dateIso],
          set: { price: item.price },
        });
    }
  }

  async function insertCotationsToday(fundCode: string, dateIso: string, fetchedAt: Date, dataJson: unknown) {
    await db.insert(cotationsTodayRead)
      .values({
        fundCode: fundCode.toUpperCase(),
        dateIso,
        fetchedAt,
        dataJson: dataJson as any,
      })
      .onConflictDoUpdate({
        target: [cotationsTodayRead.fundCode, cotationsTodayRead.dateIso],
        set: {
          fetchedAt,
          dataJson: dataJson as any,
        },
      });
  }

  async function upsertDividends(items: Array<{ fund_code: string; date_iso: string; payment: string; type: number; value: number; yield: number }>) {
    if (items.length === 0) return;
    for (const item of items) {
      await db.insert(dividendsRead)
        .values({
          fundCode: item.fund_code.toUpperCase(),
          dateIso: item.date_iso,
          payment: item.payment,
          type: item.type,
          value: item.value,
          yield: item.yield ?? null,
        })
        .onConflictDoUpdate({
          target: [dividendsRead.fundCode, dividendsRead.dateIso, dividendsRead.type],
          set: {
            payment: item.payment,
            value: item.value,
            yield: item.yield ?? null,
          },
        });
    }
  }

  async function upsertDocuments(items: Array<{ fund_code: string; document_id: number; title: string; category: string; type: string; date: string; date_upload_iso: string; dateUpload: string; url: string; status: string; version: number }>) {
    if (items.length === 0) return;
    for (const item of items) {
      await db.insert(documentsRead)
        .values({
          fundCode: item.fund_code.toUpperCase(),
          id: item.document_id,
          title: item.title,
          category: item.category,
          type: item.type,
          date: item.date,
          dateUpload: item.date_upload_iso,
          url: item.url,
          status: item.status,
          version: item.version,
        })
        .onConflictDoUpdate({
          target: [documentsRead.fundCode, documentsRead.id],
          set: {
            title: item.title,
            category: item.category,
            type: item.type,
            date: item.date,
            dateUpload: item.date_upload_iso,
            url: item.url,
            status: item.status,
            version: item.version,
          },
        });
    }
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
