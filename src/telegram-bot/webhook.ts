import { OpenAPIHono } from '@hono/zod-openapi';
import { getDb } from '../db';
import { parseBotCommand, type BotCommand } from './commands';
import { createTelegramService, type TelegramUpdate } from './telegram-api';
import { upsertTelegramUser } from './storage';
import {
  handleAdd,
  handleCancel,
  handleCategories,
  handleConfirm,
  handleCotation,
  handleDocumentos,
  handleExport,
  handleHelp,
  handleList,
  handlePesquisa,
  handleRankHoje,
  handleRankV,
  handleRemove,
  handleResumoDocumento,
  handleSet,
} from './webhook-handlers';

const app = new OpenAPIHono();

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

  const deps = { db, telegram, chatIdStr };
  switch (cmd.kind) {
    case 'help':
      await handleHelp(deps);
      break;
    case 'resumo-documento':
      await handleResumoDocumento(deps, cmd.codes);
      break;
    case 'cancel':
      await handleCancel(deps, callbackToken);
      break;
    case 'confirm':
      await handleConfirm(deps, callbackToken);
      break;
    case 'list':
      await handleList(deps);
      break;
    case 'categories':
      await handleCategories(deps);
      break;
    case 'export':
      await handleExport(deps, cmd.codes);
      break;
    case 'rank-hoje':
      await handleRankHoje(deps, cmd.codes);
      break;
    case 'rankv':
      await handleRankV(deps);
      break;
    case 'documentos':
      await handleDocumentos(deps, { code: cmd.code, limit: cmd.limit });
      break;
    case 'pesquisa':
      await handlePesquisa(deps, cmd.code);
      break;
    case 'cotation':
      await handleCotation(deps, cmd.code);
      break;
    case 'set':
      await handleSet(deps, cmd.codes);
      break;
    case 'add':
      await handleAdd(deps, cmd.codes);
      break;
    case 'remove':
      await handleRemove(deps, cmd.codes);
      break;
    default:
      await handleHelp(deps);
  }

  return c.json({ ok: true });
});

export default app;
