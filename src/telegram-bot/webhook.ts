import { OpenAPIHono } from '@hono/zod-openapi';
import { getDb } from '../db';
import { promises as fs } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { desc, eq, inArray } from 'drizzle-orm';
import { drizzle } from 'drizzle-orm/better-sqlite3';
import { document, fundMaster } from '../db/schema';
import { formatHelp, parseBotCommand, type BotCommand } from './commands';
import { getOrComputeCotationStats } from './cotation-stats';
import { createTelegramService, type TelegramUpdate } from './telegram-api';
import { exportFundJson } from '../services/fund-export';
import { pickIndicatorValue } from '../services/fund-export/analytics';
import {
  addTelegramUserFunds,
  clearTelegramPendingAction,
  getTelegramPendingAction,
  listExistingFundCodes,
  listFundCategoryInfoByCodes,
  listTelegramUserFunds,
  removeTelegramUserFunds,
  setTelegramUserFunds,
  upsertTelegramPendingAction,
  upsertTelegramUser,
} from './storage';
import {
  formatAddMessage,
  formatCategoriesMessage,
  formatConfirmRemoveMessage,
  formatConfirmSetMessage,
  formatCotationMessage,
  formatDocumentsMessage,
  formatFundsListMessage,
  formatPesquisaMessage,
  formatRankHojeMessage,
  formatRemoveMessage,
  formatSetMessage,
} from './webhook-messages';
import { handleResumoDocumentoCommand } from './resumo-documento';

const app = new OpenAPIHono();

function pickLimit(value: number | undefined, fallback: number): number {
  if (!Number.isFinite(value) || (value as number) <= 0) return fallback;
  return Math.min(Math.max(1, Math.floor(value as number)), 50);
}

function isExpired(iso: string, ttlMs: number): boolean {
  const t = new Date(iso).getTime();
  if (!Number.isFinite(t)) return true;
  return Date.now() - t > ttlMs;
}

function listLatestDocuments(db: ReturnType<typeof getDb>, fundCodes: string[], limit: number) {
  const codes = Array.from(new Set(fundCodes.map((c) => c.toUpperCase()))).filter(Boolean);
  if (!codes.length) return [];
  const orm = drizzle(db);
  return orm
    .select({
      fund_code: document.fund_code,
      title: document.title,
      category: document.category,
      type: document.type,
      dateUpload: document.dateUpload,
      url: document.url,
    })
    .from(document)
    .where(inArray(document.fund_code, codes))
    .orderBy(desc(document.date_upload_iso), desc(document.document_id))
    .limit(limit)
    .all();
}

app.post('/webhook', async (c) => {
  const secret = (process.env.TELEGRAM_WEBHOOK_SECRET || '').trim();
  if (secret) {
    const header = (c.req.header('x-telegram-bot-api-secret-token') || '').trim();
    if (header !== secret) return c.json({ ok: true });
  }

  const token = (process.env.TELEGRAM_BOT_TOKEN || '').trim();
  if (!token) return c.json({ ok: true });

  const update = (await c.req.json()) as TelegramUpdate;
  const callback = update?.callback_query;
  const callbackData = String(callback?.data || '').trim();
  const msg = update?.message ?? callback?.message;
  const text = msg?.text || '';
  if (!msg || (!text && !callbackData)) return c.json({ ok: true });

  const chatIdStr = String(msg.chat.id);
  const db = getDb();
  upsertTelegramUser(db, {
    chatId: chatIdStr,
    username: msg.chat.username,
    firstName: msg.chat.first_name,
    lastName: msg.chat.last_name,
  });

  const telegram = createTelegramService(token);
  await telegram.registerDefaultCommandsOnce();
  if (callback?.id) {
    await telegram.ackCallbackQuery(callback.id);
  }
  let callbackKind: 'confirm' | 'cancel' | null = null;
  let callbackToken: string | null = null;
  if (callbackData) {
    const m = callbackData.match(/^(confirm|cancel)(?::(.+))?$/);
    if (!m) return c.json({ ok: true });
    callbackKind = m[1] as 'confirm' | 'cancel';
    callbackToken = m[2] ?? null;
  }

  const cmd: BotCommand =
    callbackKind === 'confirm'
      ? { kind: 'confirm' }
      : callbackKind === 'cancel'
        ? { kind: 'cancel' }
        : parseBotCommand(text);

  if (cmd.kind === 'help') {
    await telegram.sendText(chatIdStr, formatHelp());
    return c.json({ ok: true });
  }

  if (cmd.kind === 'resumo-documento') {
    await handleResumoDocumentoCommand({ db, chatId: chatIdStr, telegram, codes: cmd.codes });
    return c.json({ ok: true });
  }

  if (cmd.kind === 'cancel') {
    if (callbackToken) {
      const pending = getTelegramPendingAction(db, chatIdStr);
      if (!pending) {
        await telegram.sendText(chatIdStr, 'Não há nenhuma ação pendente para cancelar.');
        return c.json({ ok: true });
      }
      if (pending.createdAt !== callbackToken) {
        await telegram.sendText(chatIdStr, 'Essa ação não é mais válida. Envie o comando novamente.');
        return c.json({ ok: true });
      }
    }
    clearTelegramPendingAction(db, chatIdStr);
    await telegram.sendText(chatIdStr, '✅ Ok, ação cancelada.');
    return c.json({ ok: true });
  }

  if (cmd.kind === 'confirm') {
    const pending = getTelegramPendingAction(db, chatIdStr);
    if (!pending) {
      await telegram.sendText(chatIdStr, 'Não há nenhuma ação pendente para confirmar.');
      return c.json({ ok: true });
    }
    if (callbackToken && pending.createdAt !== callbackToken) {
      await telegram.sendText(chatIdStr, 'Essa confirmação não é mais válida. Envie o comando novamente.');
      return c.json({ ok: true });
    }
    if (isExpired(pending.createdAt, 10 * 60 * 1000)) {
      clearTelegramPendingAction(db, chatIdStr);
      await telegram.sendText(chatIdStr, 'Essa confirmação expirou. Envie o comando novamente.');
      return c.json({ ok: true });
    }

    if (pending.action.kind === 'set') {
      const codes = pending.action.codes;
      const existing = listExistingFundCodes(db, codes);
      const missing = codes.map((c) => c.toUpperCase()).filter((code) => !existing.includes(code));
      const before = listTelegramUserFunds(db, chatIdStr);
      setTelegramUserFunds(db, chatIdStr, existing);
      clearTelegramPendingAction(db, chatIdStr);
      const removed = before.filter((code) => !existing.includes(code));
      const added = existing.filter((code) => !before.includes(code));
      await telegram.sendText(chatIdStr, `✅ Confirmado\n\n${formatSetMessage({ existing, added, removed, missing })}`);
      return c.json({ ok: true });
    }

    if (pending.action.kind === 'remove') {
      const codes = pending.action.codes;
      const existing = listExistingFundCodes(db, codes);
      const missing = codes.map((c) => c.toUpperCase()).filter((code) => !existing.includes(code));
      const removedCount = removeTelegramUserFunds(db, chatIdStr, existing);
      clearTelegramPendingAction(db, chatIdStr);
      const nowList = listTelegramUserFunds(db, chatIdStr);
      await telegram.sendText(chatIdStr, `✅ Confirmado\n\n${formatRemoveMessage({ removedCount, nowList, missing })}`);
      return c.json({ ok: true });
    }

    clearTelegramPendingAction(db, chatIdStr);
    await telegram.sendText(chatIdStr, 'Não consegui interpretar a ação pendente. Envie o comando novamente.');
    return c.json({ ok: true });
  }

  if (cmd.kind === 'list') {
    const funds = listTelegramUserFunds(db, chatIdStr);
    await telegram.sendText(chatIdStr, formatFundsListMessage(funds));
    return c.json({ ok: true });
  }

  if (cmd.kind === 'categories') {
    const funds = listTelegramUserFunds(db, chatIdStr);
    if (!funds.length) {
      await telegram.sendText(chatIdStr, 'Sua lista está vazia.');
      return c.json({ ok: true });
    }
    const info = listFundCategoryInfoByCodes(db, funds);
    await telegram.sendText(chatIdStr, formatCategoriesMessage(funds, info));
    return c.json({ ok: true });
  }

  if (cmd.kind === 'export') {
    const requested = cmd.codes.length ? cmd.codes.map((c) => c.toUpperCase()) : listTelegramUserFunds(db, chatIdStr);
    if (!requested.length) {
      await telegram.sendText(chatIdStr, 'Sua lista está vazia.');
      return c.json({ ok: true });
    }

    const existing = listExistingFundCodes(db, requested);
    const missing = requested.filter((code) => !existing.includes(code));

    const funds: Array<{ code: string; ok: boolean; data?: any; error?: string }> = [];
    for (const code of existing) {
      const data = exportFundJson(db, code);
      if (!data) {
        funds.push({ code, ok: false, error: 'FII não encontrado' });
      } else {
        funds.push({ code, ok: true, data });
      }
    }

    const payload = {
      generated_at: new Date().toISOString(),
      source: 'telegram',
      chat_id: chatIdStr,
      requested_codes: requested,
      exported_codes: existing,
      missing_codes: missing,
      funds,
    };

    const safeStamp = new Date().toISOString().replace(/[:.]/g, '-');
    const filename = `fii-export-${chatIdStr}-${safeStamp}.json`;
    const filePath = path.join(os.tmpdir(), filename);
    await fs.writeFile(filePath, JSON.stringify(payload), 'utf8');
    try {
      const caption = `Export JSON: ${existing.length} fundo(s)${missing.length ? ` (${missing.length} não encontrado(s))` : ''}`;
      const res = await telegram.sendDocument(chatIdStr, { filePath, filename, caption, contentType: 'application/json' });
      if (!res.ok) {
        await telegram.sendText(chatIdStr, 'Não consegui enviar o arquivo agora. Tente novamente.');
      }
    } finally {
      await fs.unlink(filePath).catch(() => {});
    }

    return c.json({ ok: true });
  }

  if (cmd.kind === 'rank-hoje') {
    const requested = cmd.codes.length ? cmd.codes.map((c) => c.toUpperCase()) : listTelegramUserFunds(db, chatIdStr);
    if (!requested.length) {
      await telegram.sendText(chatIdStr, 'Sua lista está vazia.');
      return c.json({ ok: true });
    }

    const existing = listExistingFundCodes(db, requested);
    const missing = requested.filter((code) => !existing.includes(code));

    const ranked: Array<{ code: string; pvp: number | null; dividendYield12m: number | null; liquidity: number | null }> = [];
    for (const code of existing) {
      const data = exportFundJson(db, code);
      if (!data?.data?.indicators_latest) continue;
      const indicators = data.data.indicators_latest as Record<string, Array<{ year: string; value: number | null }>>;
      const pvp = pickIndicatorValue(indicators, 'pvp');
      const dy = pickIndicatorValue(indicators, 'dividend_yield');
      const liq = pickIndicatorValue(indicators, 'liquidez_diaria');

      if (pvp === null || dy === null || liq === null) continue;
      if (pvp <= 0.82 && dy >= 14.0 && liq >= 600000) {
        ranked.push({ code, pvp, dividendYield12m: dy, liquidity: liq });
      }
    }

    ranked.sort((a, b) => {
      const dy = (b.dividendYield12m ?? 0) - (a.dividendYield12m ?? 0);
      if (dy) return dy;
      const pvp = (a.pvp ?? 0) - (b.pvp ?? 0);
      if (pvp) return pvp;
      return (b.liquidity ?? 0) - (a.liquidity ?? 0);
    });

    await telegram.sendText(chatIdStr, formatRankHojeMessage({ items: ranked, total: existing.length, missing }));
    return c.json({ ok: true });
  }

  if (cmd.kind === 'documentos') {
    const limit = pickLimit(cmd.limit, 5);
    const code = cmd.code?.toUpperCase();

    if (code) {
      const existing = listExistingFundCodes(db, [code]);
      if (!existing.length) {
        await telegram.sendText(chatIdStr, `Fundo não encontrado: ${code}`);
        return c.json({ ok: true });
      }
      const docs = listLatestDocuments(db, [code], limit);
      await telegram.sendText(chatIdStr, formatDocumentsMessage({ docs, limit, code }));
      return c.json({ ok: true });
    }

    const funds = listTelegramUserFunds(db, chatIdStr);
    if (!funds.length) {
      await telegram.sendText(chatIdStr, 'Sua lista está vazia.');
      return c.json({ ok: true });
    }
    const docs = listLatestDocuments(db, funds, limit);
    await telegram.sendText(chatIdStr, formatDocumentsMessage({ docs, limit }));
    return c.json({ ok: true });
  }

  if (cmd.kind === 'pesquisa') {
    const code = cmd.code.toUpperCase();
    const existing = listExistingFundCodes(db, [code]);
    if (!existing.length) {
      await telegram.sendText(chatIdStr, `Fundo não encontrado: ${code}`);
      return c.json({ ok: true });
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
      .where(eq(fundMaster.code, code))
      .get();

    if (!fund) {
      await telegram.sendText(chatIdStr, `Fundo não encontrado: ${code}`);
      return c.json({ ok: true });
    }

    await telegram.sendText(chatIdStr, formatPesquisaMessage({ fund }));
    return c.json({ ok: true });
  }

  if (cmd.kind === 'cotation') {
    const code = cmd.code.toUpperCase();
    const existing = listExistingFundCodes(db, [code]);
    if (!existing.length) {
      await telegram.sendText(chatIdStr, `Fundo não encontrado: ${code}`);
      return c.json({ ok: true });
    }

    const stats = getOrComputeCotationStats(db, code);
    if (!stats) {
      await telegram.sendText(chatIdStr, `Sem cotações históricas para ${code}.`);
      return c.json({ ok: true });
    }
    await telegram.sendText(chatIdStr, formatCotationMessage(stats));
    return c.json({ ok: true });
  }

  const existing = listExistingFundCodes(db, cmd.codes);
  const missing = cmd.codes.filter((code) => !existing.includes(code));

  if (cmd.kind === 'set') {
    const before = listTelegramUserFunds(db, chatIdStr);
    const removed = before.filter((code) => !existing.includes(code));
    const added = existing.filter((code) => !before.includes(code));
    if (removed.length > 0) {
      const createdAt = upsertTelegramPendingAction(db, chatIdStr, { kind: 'set', codes: cmd.codes.map((c) => c.toUpperCase()) });
      await telegram.sendText(
        chatIdStr,
        formatConfirmSetMessage({ beforeCount: before.length, afterCodes: existing, added, removed, missing }),
        {
          replyMarkup: {
            inline_keyboard: [[
              { text: '✅ Confirmar', callback_data: `confirm:${createdAt}` },
              { text: '❌ Cancelar', callback_data: `cancel:${createdAt}` },
            ]],
          },
        }
      );
      return c.json({ ok: true });
    }
    setTelegramUserFunds(db, chatIdStr, existing);
    await telegram.sendText(chatIdStr, formatSetMessage({ existing, added, removed, missing }));
    return c.json({ ok: true });
  }

  if (cmd.kind === 'add') {
    const addedCount = addTelegramUserFunds(db, chatIdStr, existing);
    const nowList = listTelegramUserFunds(db, chatIdStr);
    await telegram.sendText(chatIdStr, formatAddMessage({ addedCount, nowList, missing }));
    return c.json({ ok: true });
  }

  if (cmd.kind === 'remove') {
    const before = listTelegramUserFunds(db, chatIdStr);
    const beforeSet = new Set(before);
    const toRemove = existing.filter((code) => beforeSet.has(code));
    if (toRemove.length > 0) {
      const createdAt = upsertTelegramPendingAction(db, chatIdStr, { kind: 'remove', codes: cmd.codes.map((c) => c.toUpperCase()) });
      await telegram.sendText(chatIdStr, formatConfirmRemoveMessage({ beforeCount: before.length, toRemove, missing }), {
        replyMarkup: {
          inline_keyboard: [[
            { text: '✅ Confirmar', callback_data: `confirm:${createdAt}` },
            { text: '❌ Cancelar', callback_data: `cancel:${createdAt}` },
          ]],
        },
      });
      return c.json({ ok: true });
    }
    await telegram.sendText(chatIdStr, formatRemoveMessage({ removedCount: 0, nowList: before, missing }));
    return c.json({ ok: true });
  }

  await telegram.sendText(chatIdStr, formatHelp());
  return c.json({ ok: true });
});

export default app;
