import { eq } from 'drizzle-orm';
import { drizzle } from 'drizzle-orm/better-sqlite3';
import { fundMaster } from '../../db/schema';
import { getOrComputeCotationStats } from '../cotation-stats';
import { listExistingFundCodes } from '../storage';
import { formatCotationMessage, formatPesquisaMessage } from '../webhook-messages';
import type { HandlerDeps } from './types';

export async function handlePesquisa({ db, telegram, chatIdStr }: HandlerDeps, code: string) {
  const fundCode = code.toUpperCase();
  const existing = listExistingFundCodes(db, [fundCode]);
  if (!existing.length) {
    await telegram.sendText(chatIdStr, `Fundo não encontrado: ${fundCode}`);
    return;
  }

  const orm = drizzle(db);
  const fund = orm
    .select({
      code: fundMaster.code,
      sector: fundMaster.sector,
      type: fundMaster.type,
      segmento: fundMaster.segmento,
      tipo_fundo: fundMaster.tipo_fundo,
      p_vp: fundMaster.p_vp,
      dividend_yield: fundMaster.dividend_yield,
      dividend_yield_last_5_years: fundMaster.dividend_yield_last_5_years,
      daily_liquidity: fundMaster.daily_liquidity,
      net_worth: fundMaster.net_worth,
      razao_social: fundMaster.razao_social,
      cnpj: fundMaster.cnpj,
      publico_alvo: fundMaster.publico_alvo,
      mandato: fundMaster.mandato,
      prazo_duracao: fundMaster.prazo_duracao,
      tipo_gestao: fundMaster.tipo_gestao,
      taxa_adminstracao: fundMaster.taxa_adminstracao,
      vacancia: fundMaster.vacancia,
      numero_cotistas: fundMaster.numero_cotistas,
      cotas_emitidas: fundMaster.cotas_emitidas,
      valor_patrimonial_cota: fundMaster.valor_patrimonial_cota,
      valor_patrimonial: fundMaster.valor_patrimonial,
      ultimo_rendimento: fundMaster.ultimo_rendimento,
      updated_at: fundMaster.updated_at,
    })
    .from(fundMaster)
    .where(eq(fundMaster.code, fundCode))
    .get();

  if (!fund) {
    await telegram.sendText(chatIdStr, `Fundo não encontrado: ${fundCode}`);
    return;
  }

  await telegram.sendText(chatIdStr, formatPesquisaMessage({ fund }));
}

export async function handleCotation({ db, telegram, chatIdStr }: HandlerDeps, code: string) {
  const fundCode = code.toUpperCase();
  const existing = listExistingFundCodes(db, [fundCode]);
  if (!existing.length) {
    await telegram.sendText(chatIdStr, `Fundo não encontrado: ${fundCode}`);
    return;
  }

  const stats = getOrComputeCotationStats(db, fundCode);
  if (!stats) {
    await telegram.sendText(chatIdStr, `Sem cotações históricas para ${fundCode}.`);
    return;
  }
  await telegram.sendText(chatIdStr, formatCotationMessage(stats));
}
