import { getWriteDb } from '../../pipeline/db';
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

  const sql = getWriteDb();
  const rows = await sql<{
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
    cotas_emitidas: number | null;
    valor_patrimonial_cota: number | null;
    valor_patrimonial: number | null;
    ultimo_rendimento: number | null;
    updated_at: string | null;
  }[]>`
    SELECT d.code,
           l.sector,
           l.type,
           d.segmento,
           d.tipo_fundo,
           l.p_vp,
           l.dividend_yield,
           l.dividend_yield_last_5_years,
           l.daily_liquidity,
           l.net_worth,
           d.razao_social,
           d.cnpj,
           d.publico_alvo,
           d.mandato,
           d.prazo_duracao,
           d.tipo_gestao,
           d.taxa_adminstracao,
           d.vacancia,
           d.numero_cotistas,
           d.cotas_emitidas,
           d.valor_patrimonial_cota,
           d.valor_patrimonial,
           d.ultimo_rendimento,
           d.updated_at
    FROM fund_details_read d
    LEFT JOIN fund_list_read l ON l.code = d.code
    WHERE d.code = ${fundCode}
    LIMIT 1
  `;

  const fund = rows[0];
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
