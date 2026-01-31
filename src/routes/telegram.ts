import { OpenAPIHono } from '@hono/zod-openapi';
import { getDb } from '../db';
import { formatHelp, parseBotCommand } from '../telegram-bot/commands';
import { createTelegramService, type TelegramUpdate } from '../telegram-bot/telegram-api';
import {
  addTelegramUserFunds,
  listExistingFundCodes,
  listTelegramUserFunds,
  removeTelegramUserFunds,
  setTelegramUserFunds,
  upsertTelegramUser,
} from '../telegram-bot/storage';

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
    await telegram.sendText(
      chatIdStr,
      funds.length ? `Sua lista (${funds.length} fundos): ${funds.join(', ')}` : 'Sua lista está vazia.'
    );
    return c.json({ ok: true });
  }

  const existing = listExistingFundCodes(db, cmd.codes);
  const missing = cmd.codes.filter((code) => !existing.includes(code));

  if (cmd.kind === 'set') {
    const before = listTelegramUserFunds(db, chatIdStr);
    setTelegramUserFunds(db, chatIdStr, existing);
    const removed = before.filter((code) => !existing.includes(code));
    const added = existing.filter((code) => !before.includes(code));
    const parts = [
      existing.length ? `Lista atualizada. Total: ${existing.length} fundos.` : 'Lista atualizada. Sua lista está vazia.',
    ];
    if (existing.length) parts.push(`Fundos: ${existing.join(', ')}`);
    if (added.length) parts.push(`Adicionados: ${added.join(', ')}`);
    if (removed.length) parts.push(`Removidos: ${removed.join(', ')}`);
    if (missing.length) parts.push(`Não encontrei no banco: ${missing.join(', ')}`);
    await telegram.sendText(chatIdStr, parts.join('\n'));
    return c.json({ ok: true });
  }

  if (cmd.kind === 'add') {
    const added = addTelegramUserFunds(db, chatIdStr, existing);
    const parts = [`Adicionados: ${added}`];
    const nowList = listTelegramUserFunds(db, chatIdStr);
    parts.push(nowList.length ? `Agora (${nowList.length} fundos): ${nowList.join(', ')}` : 'Agora: (vazia)');
    if (missing.length) parts.push(`Não encontrei no banco: ${missing.join(', ')}`);
    await telegram.sendText(chatIdStr, parts.join('\n'));
    return c.json({ ok: true });
  }

  if (cmd.kind === 'remove') {
    const removed = removeTelegramUserFunds(db, chatIdStr, existing);
    const parts = [`Removidos: ${removed}`];
    const nowList = listTelegramUserFunds(db, chatIdStr);
    parts.push(nowList.length ? `Agora (${nowList.length} fundos): ${nowList.join(', ')}` : 'Agora: (vazia)');
    if (missing.length) parts.push(`Não encontrei no banco: ${missing.join(', ')}`);
    await telegram.sendText(chatIdStr, parts.join('\n'));
    return c.json({ ok: true });
  }

  await telegram.sendText(chatIdStr, formatHelp());
  return c.json({ ok: true });
});

export default app;
