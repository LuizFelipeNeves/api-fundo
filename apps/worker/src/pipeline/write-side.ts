import { getWriteDb } from '../db';
import type {
  PersistFundListItem,
  PersistFundDetailsItem,
  PersistIndicators,
  PersistCotation,
  PersistCotationsToday,
  PersistDividend,
  PersistDocument,
} from './messages';
import { fundMaster, fundState, cotation, dividend, document, indicatorsSnapshot, cotationsTodaySnapshot } from '../db/schema';

export function createWriteSide() {
  const db = getWriteDb();

  async function touchFundState(fundCode: string, fields: Partial<typeof fundState.$inferInsert>) {
    const now = new Date();
    await db.insert(fundState)
      .values({
        fundCode: fundCode.toUpperCase(),
        createdAt: now,
        updatedAt: now,
      })
      .onConflictDoUpdate({
        target: fundState.fundCode,
        set: { updatedAt: now, ...fields },
      });
  }

  async function upsertFundList(items: PersistFundListItem[]) {
    if (items.length === 0) return;
    const now = new Date();
    for (const item of items) {
      await db.insert(fundMaster)
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
          target: fundMaster.code,
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

  async function upsertFundDetails(item: PersistFundDetailsItem) {
    const now = new Date();
    await db.insert(fundMaster)
      .values({
        code: item.code.toUpperCase(),
        id: item.id ?? null,
        cnpj: item.cnpj ?? null,
        razaoSocial: item.razao_social ?? null,
        publicAlvo: item.publico_alvo ?? null,
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
        target: fundMaster.code,
        set: {
          id: item.id ?? null,
          cnpj: item.cnpj ?? null,
          razaoSocial: item.razao_social ?? null,
          publicAlvo: item.publico_alvo ?? null,
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

    await touchFundState(item.code, { lastDetailsSyncAt: now });
  }

  async function upsertIndicators(item: PersistIndicators) {
    const fetchedAt = new Date(item.fetched_at);
    await db.insert(indicatorsSnapshot)
      .values({
        fundCode: item.fund_code.toUpperCase(),
        fetchedAt,
        dataJson: item.data_json as any,
      })
      .onConflictDoUpdate({
        target: [indicatorsSnapshot.fundCode],
        set: {
          fetchedAt,
          dataJson: item.data_json as any,
        },
      });

    await touchFundState(item.fund_code, {
      lastIndicatorsAt: fetchedAt,
      lastIndicatorsHash: item.data_hash ?? null,
    });
  }

  async function upsertCotations(items: PersistCotation[]) {
    if (items.length === 0) return;
    for (const item of items) {
      await db.insert(cotation)
        .values({
          fundCode: item.fund_code.toUpperCase(),
          dateIso: item.date_iso,
          price: item.price,
        })
        .onConflictDoUpdate({
          target: [cotation.fundCode, cotation.dateIso],
          set: { price: item.price },
        });
    }

    const now = new Date();
    const codes = Array.from(new Set(items.map((i) => i.fund_code.toUpperCase())));
    for (const code of codes) {
      await touchFundState(code, { lastHistoricalCotationsAt: now });
    }
  }

  async function upsertCotationsToday(item: PersistCotationsToday) {
    await db.insert(cotationsTodaySnapshot)
      .values({
        fundCode: item.fund_code.toUpperCase(),
        dateIso: item.date_iso,
        fetchedAt: new Date(item.fetched_at),
        dataJson: item.data_json as any,
      })
      .onConflictDoUpdate({
        target: [cotationsTodaySnapshot.fundCode, cotationsTodaySnapshot.dateIso],
        set: {
          fetchedAt: new Date(item.fetched_at),
          dataJson: item.data_json as any,
        },
      });

    await touchFundState(item.fund_code, { lastCotationsTodayAt: new Date(item.fetched_at) });
  }

  async function upsertDividends(items: PersistDividend[]) {
    if (items.length === 0) return;
    for (const item of items) {
      await db.insert(dividend)
        .values({
          fundCode: item.fund_code.toUpperCase(),
          dateIso: item.date_iso,
          payment: item.payment,
          type: item.type,
          value: item.value,
          yield: item.yield ?? null,
        })
        .onConflictDoUpdate({
          target: [dividend.fundCode, dividend.dateIso, dividend.type],
          set: {
            payment: item.payment,
            value: item.value,
            yield: item.yield ?? null,
          },
        });
    }
  }

  async function upsertDocuments(items: PersistDocument[]) {
    if (items.length === 0) return;
    for (const item of items) {
      await db.insert(document)
        .values({
          fundCode: item.fund_code.toUpperCase(),
          category: item.category ?? null,
          type: item.type ?? null,
          title: item.title ?? null,
          dateUpload: item.date_upload_iso ?? null,
          date: item.date ?? null,
          status: item.status ?? null,
          version: item.version ?? null,
          url: item.url ?? null,
        })
        .onConflictDoUpdate({
          target: [document.fundCode, document.id],
          set: {
            category: item.category ?? null,
            type: item.type ?? null,
            title: item.title ?? null,
            dateUpload: item.date_upload_iso ?? null,
            date: item.date ?? null,
            status: item.status ?? null,
            version: item.version ?? null,
            url: item.url ?? null,
          },
        });
    }

    const now = new Date();
    const byFund = new Map<string, number>();
    for (const item of items) {
      const key = item.fund_code.toUpperCase();
      const current = byFund.get(key) ?? 0;
      if (item.document_id > current) byFund.set(key, item.document_id);
    }
    for (const [code, maxId] of byFund.entries()) {
      await touchFundState(code, { lastDocumentsAt: now, lastDocumentsMaxId: maxId });
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
