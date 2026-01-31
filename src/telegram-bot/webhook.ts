import { OpenAPIHono } from '@hono/zod-openapi';
import { getDb } from '../db';
import { sql, desc, eq, inArray } from 'drizzle-orm';
import { drizzle } from 'drizzle-orm/better-sqlite3';
import { cotation, dividend, document, fundMaster } from '../db/schema';
import { formatHelp, parseBotCommand } from './commands';
import { getOrComputeCotationStats } from './cotation-stats';
import { createTelegramService, type TelegramUpdate } from './telegram-api';
import {
  addTelegramUserFunds,
  listExistingFundCodes,
  listFundCategoryInfoByCodes,
  listTelegramUserFunds,
  removeTelegramUserFunds,
  setTelegramUserFunds,
  upsertTelegramUser,
} from './storage';
import {
  formatAddMessage,
  formatCategoriesMessage,
  formatCotationMessage,
  formatDocumentsMessage,
  formatFundsListMessage,
  formatPesquisaMessage,
  formatRemoveMessage,
  formatSetMessage,
} from './webhook-messages';

const app = new OpenAPIHono();

function pickLimit(value: number | undefined, fallback: number): number {
  if (!Number.isFinite(value) || (value as number) <= 0) return fallback;
  return Math.min(Math.max(1, Math.floor(value as number)), 50);
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
  const msg = update?.message;
  const text = msg?.text || '';
  if (!msg || !text) return c.json({ ok: true });

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
  const cmd = parseBotCommand(text);

  if (cmd.kind === 'help') {
    await telegram.sendText(chatIdStr, formatHelp());
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

    const counts = {
      documents: Number(orm.select({ c: sql<number>`count(*)` }).from(document).where(eq(document.fund_code, code)).get()?.c ?? 0),
      dividends: Number(orm.select({ c: sql<number>`count(*)` }).from(dividend).where(eq(dividend.fund_code, code)).get()?.c ?? 0),
      cotations: Number(orm.select({ c: sql<number>`count(*)` }).from(cotation).where(eq(cotation.fund_code, code)).get()?.c ?? 0),
    };

    await telegram.sendText(chatIdStr, formatPesquisaMessage({ fund, counts }));
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
    setTelegramUserFunds(db, chatIdStr, existing);
    const removed = before.filter((code) => !existing.includes(code));
    const added = existing.filter((code) => !before.includes(code));
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
    const removedCount = removeTelegramUserFunds(db, chatIdStr, existing);
    const nowList = listTelegramUserFunds(db, chatIdStr);
    await telegram.sendText(chatIdStr, formatRemoveMessage({ removedCount, nowList, missing }));
    return c.json({ ok: true });
  }

  await telegram.sendText(chatIdStr, formatHelp());
  return c.json({ ok: true });
});

export default app;
