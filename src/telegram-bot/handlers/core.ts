import { formatHelp } from '../commands';
import {
  clearTelegramPendingAction,
  getTelegramPendingAction,
  listExistingFundCodes,
  listTelegramUserFunds,
  removeTelegramUserFunds,
  setTelegramUserFunds,
} from '../storage';
import { formatRemoveMessage, formatSetMessage } from '../webhook-messages';
import type { HandlerDeps } from './types';

function isExpired(iso: string, ttlMs: number): boolean {
  const t = new Date(iso).getTime();
  if (!Number.isFinite(t)) return true;
  return Date.now() - t > ttlMs;
}

export async function handleHelp({ telegram, chatIdStr }: HandlerDeps) {
  await telegram.sendText(chatIdStr, formatHelp());
}

export async function handleCancel({ db, telegram, chatIdStr }: HandlerDeps, callbackToken: string | null) {
  if (callbackToken) {
    const pending = getTelegramPendingAction(db, chatIdStr);
    if (!pending) {
      await telegram.sendText(chatIdStr, 'Não há nenhuma ação pendente para cancelar.');
      return;
    }
    if (pending.createdAt !== callbackToken) {
      await telegram.sendText(chatIdStr, 'Essa ação não é mais válida. Envie o comando novamente.');
      return;
    }
  }
  clearTelegramPendingAction(db, chatIdStr);
  await telegram.sendText(chatIdStr, '✅ Ok, ação cancelada.');
}

export async function handleConfirm({ db, telegram, chatIdStr }: HandlerDeps, callbackToken: string | null) {
  const pending = getTelegramPendingAction(db, chatIdStr);
  if (!pending) {
    await telegram.sendText(chatIdStr, 'Não há nenhuma ação pendente para confirmar.');
    return;
  }
  if (callbackToken && pending.createdAt !== callbackToken) {
    await telegram.sendText(chatIdStr, 'Essa confirmação não é mais válida. Envie o comando novamente.');
    return;
  }
  if (isExpired(pending.createdAt, 10 * 60 * 1000)) {
    clearTelegramPendingAction(db, chatIdStr);
    await telegram.sendText(chatIdStr, 'Essa confirmação expirou. Envie o comando novamente.');
    return;
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
    return;
  }

  if (pending.action.kind === 'remove') {
    const codes = pending.action.codes;
    const existing = listExistingFundCodes(db, codes);
    const missing = codes.map((c) => c.toUpperCase()).filter((code) => !existing.includes(code));
    const removedCount = removeTelegramUserFunds(db, chatIdStr, existing);
    clearTelegramPendingAction(db, chatIdStr);
    const nowList = listTelegramUserFunds(db, chatIdStr);
    await telegram.sendText(chatIdStr, `✅ Confirmado\n\n${formatRemoveMessage({ removedCount, nowList, missing })}`);
    return;
  }

  clearTelegramPendingAction(db, chatIdStr);
  await telegram.sendText(chatIdStr, 'Não consegui interpretar a ação pendente. Envie o comando novamente.');
}
