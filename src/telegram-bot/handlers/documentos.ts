import { desc, inArray } from 'drizzle-orm';
import { drizzle } from 'drizzle-orm/better-sqlite3';
import { getDb } from '../../db';
import { document } from '../../db/schema';
import { listExistingFundCodes, listTelegramUserFunds } from '../storage';
import { formatDocumentsMessage } from '../webhook-messages';
import { handleResumoDocumentoCommand } from '../resumo-documento';
import type { HandlerDeps } from './types';

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

export async function handleDocumentos(
  { db, telegram, chatIdStr }: HandlerDeps,
  opts: { code?: string; limit?: number }
) {
  const limit = pickLimit(opts.limit, 5);
  const code = opts.code?.toUpperCase();

  if (code) {
    const existing = listExistingFundCodes(db, [code]);
    if (!existing.length) {
      await telegram.sendText(chatIdStr, `Fundo não encontrado: ${code}`);
      return;
    }
    const docs = listLatestDocuments(db, [code], limit);
    await telegram.sendText(chatIdStr, formatDocumentsMessage({ docs, limit, code }));
    return;
  }

  const funds = listTelegramUserFunds(db, chatIdStr);
  if (!funds.length) {
    await telegram.sendText(chatIdStr, 'Sua lista está vazia.');
    return;
  }
  const docs = listLatestDocuments(db, funds, limit);
  await telegram.sendText(chatIdStr, formatDocumentsMessage({ docs, limit }));
}

export async function handleResumoDocumento({ db, telegram, chatIdStr }: HandlerDeps, codes: string[]) {
  await handleResumoDocumentoCommand({ db, chatId: chatIdStr, telegram, codes });
}
