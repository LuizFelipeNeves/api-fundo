import { getWriteDb } from '../../db';
import { listExistingFundCodes } from '../storage';
import { formatCotationMessage, formatPesquisaMessage } from '../../telegram-bot/webhook-messages';
import type { HandlerDeps } from './types';
import { getOrComputeCotationStats } from '../cotation-stats';

export async function handlePesquisa({ db, telegram, chatIdStr }: HandlerDeps, code: string) {
  const fundCode = code.toUpperCase();
  const existing = await listExistingFundCodes(db, [fundCode]);
  if (!existing.length) {
    await telegram.sendText(chatIdStr, `Fundo não encontrado: ${fundCode}`);
    return;
  }

  const dbWrite = getWriteDb();
  const rows = await dbWrite<{
    code: string;
    sector: string | null;
    type: string | null;
    segmento: string | null;
    tipo_fundo: string | null;
    p_vp: number | null;
    dividend_yield: number | null;
    dividend_yield_last_5_years: number | null;
    daily_liquidity: number | null;
    net_worth: number | null;
    razao_social: string | null;
    cnpj: string | null;
    publico_alvo: string | null;
    mandato: string | null;
    prazo_duracao: string | null;
    tipo_gestao: string | null;
    taxa_adminstracao: string | null;
    vacancia: number | null;
    numero_cotistas: number | null;
    cotas_emitidas: string | number | null;
    valor_patrimonial_cota: number | null;
    valor_patrimonial: number | null;
    ultimo_rendimento: number | null;
    updated_at: string | null;
  }[]>`
    SELECT code,
           sector,
           type,
           segmento,
           tipo_fundo,
           p_vp,
           dividend_yield,
           dividend_yield_last_5_years,
           daily_liquidity,
           net_worth,
           razao_social,
           cnpj,
           publico_alvo,
           mandato,
           prazo_duracao,
           tipo_gestao,
           taxa_adminstracao,
           vacancia,
           numero_cotistas,
           cotas_emitidas,
           valor_patrimonial_cota,
           valor_patrimonial,
           ultimo_rendimento,
           updated_at
    FROM fund_master
    WHERE code = ${fundCode}
    LIMIT 1
  `;

  const fundRaw = rows[0];
  const cotasEmitidasRaw = fundRaw?.cotas_emitidas === null || fundRaw?.cotas_emitidas === undefined ? null : Number(fundRaw.cotas_emitidas);
  const cotasEmitidas = cotasEmitidasRaw !== null && Number.isFinite(cotasEmitidasRaw) ? cotasEmitidasRaw : null;
  const fund = fundRaw
    ? {
        ...fundRaw,
        cotas_emitidas: cotasEmitidas,
      }
    : null;
  if (!fund) {
    await telegram.sendText(chatIdStr, `Fundo não encontrado: ${fundCode}`);
    return;
  }

  await telegram.sendText(chatIdStr, formatPesquisaMessage({ fund }));
}

export async function handleCotation({ db, telegram, chatIdStr }: HandlerDeps, code: string) {
  const fundCode = code.toUpperCase();
  const existing = await listExistingFundCodes(db, [fundCode]);
  if (!existing.length) {
    await telegram.sendText(chatIdStr, `Fundo não encontrado: ${fundCode}`);
    return;
  }

  const stats = await getOrComputeCotationStats(fundCode);
  if (!stats) {
    await telegram.sendText(chatIdStr, `Sem cotações históricas para ${fundCode}.`);
    return;
  }
  await telegram.sendText(chatIdStr, formatCotationMessage(stats));
}
