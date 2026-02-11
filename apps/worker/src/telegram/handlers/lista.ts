import {
  addTelegramUserFunds,
  listExistingFundCodes,
  listFundCategoryInfoByCodes,
  listTelegramUserFunds,
  setTelegramUserFunds,
  upsertTelegramPendingAction,
} from '../storage';
import {
  formatAddMessage,
  formatCategoriesMessage,
  formatConfirmRemoveMessage,
  formatConfirmSetMessage,
  formatFundsListMessage,
  formatRemoveMessage,
  formatSetMessage,
} from '../../telegram-bot/webhook-messages';
import type { HandlerDeps } from './types';

export async function handleList({ db, telegram, chatIdStr }: HandlerDeps) {
  const funds = await listTelegramUserFunds(db, chatIdStr);
  await telegram.sendText(chatIdStr, formatFundsListMessage(funds));
}

export async function handleCategories({ db, telegram, chatIdStr }: HandlerDeps) {
  const funds = await listTelegramUserFunds(db, chatIdStr);
  if (!funds.length) {
    await telegram.sendText(chatIdStr, 'Sua lista está vazia.');
    return;
  }
  const info = await listFundCategoryInfoByCodes(db, funds);
  await telegram.sendText(chatIdStr, formatCategoriesMessage(funds, info));
}

export async function handleSet({ db, telegram, chatIdStr }: HandlerDeps, codes: string[]) {
  const existing = await listExistingFundCodes(db, codes);
  const missing = codes.filter((code) => !existing.includes(code));
  const before = await listTelegramUserFunds(db, chatIdStr);
  const removed = before.filter((code) => !existing.includes(code));
  const added = existing.filter((code) => !before.includes(code));
  if (removed.length > 0) {
    const createdAt = await upsertTelegramPendingAction(db, chatIdStr, { kind: 'set', codes: codes.map((c) => c.toUpperCase()) });
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
    return;
  }
  await setTelegramUserFunds(db, chatIdStr, existing);
  await telegram.sendText(chatIdStr, formatSetMessage({ existing, added, removed, missing }));
}

export async function handleAdd({ db, telegram, chatIdStr }: HandlerDeps, codes: string[]) {
  const existing = await listExistingFundCodes(db, codes);
  const missing = codes.filter((code) => !existing.includes(code));
  const addedCount = await addTelegramUserFunds(db, chatIdStr, existing);
  const nowList = await listTelegramUserFunds(db, chatIdStr);
  await telegram.sendText(chatIdStr, formatAddMessage({ addedCount, nowList, missing }));
}

export async function handleRemove({ db, telegram, chatIdStr }: HandlerDeps, codes: string[]) {
  const existing = await listExistingFundCodes(db, codes);
  const missing = codes.filter((code) => !existing.includes(code));
  const before = await listTelegramUserFunds(db, chatIdStr);
  const beforeSet = new Set(before);
  const toRemove = existing.filter((code) => beforeSet.has(code));
  if (toRemove.length > 0) {
    const createdAt = await upsertTelegramPendingAction(db, chatIdStr, { kind: 'remove', codes: codes.map((c) => c.toUpperCase()) });
    await telegram.sendText(chatIdStr, formatConfirmRemoveMessage({ beforeCount: before.length, toRemove, missing }), {
      replyMarkup: {
        inline_keyboard: [[
          { text: '✅ Confirmar', callback_data: `confirm:${createdAt}` },
          { text: '❌ Cancelar', callback_data: `cancel:${createdAt}` },
        ]],
      },
    });
    return;
  }
  await telegram.sendText(chatIdStr, formatRemoveMessage({ removedCount: 0, nowList: before, missing }));
}
