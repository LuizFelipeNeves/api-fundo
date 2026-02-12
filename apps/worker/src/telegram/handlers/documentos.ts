import { getRawSql } from '../../db';
import { listExistingFundCodes, listTelegramUserFunds } from '../storage';
import { formatDocumentsMessage } from '../../telegram-bot/webhook-messages';
import type { HandlerDeps } from './types';
import { handleResumoDocumentoCommand } from '../resumo-documento';

function pickLimit(value: number | undefined, fallback: number): number {
  if (!Number.isFinite(value) || (value as number) <= 0) return fallback;
  return Math.min(Math.max(1, Math.floor(value as number)), 50);
}

async function listLatestDocuments(fundCodes: string[], limit: number) {
  const codes = Array.from(new Set(fundCodes.map((c) => c.toUpperCase()))).filter(Boolean);
  if (!codes.length) return [];
  const sql = getRawSql();
  const rows = await sql.unsafe<{
    fund_code: string;
    title: string;
    category: string;
    type: string;
    dateUpload: string;
    url: string;
  }[]>(
    `SELECT fund_code, title, category, type, "dateUpload" AS "dateUpload", url
    FROM document
    WHERE fund_code = ANY($1)
    ORDER BY date_upload_iso DESC, document_id DESC
    LIMIT $2`,
    [codes, limit]
  );
  return rows;
}

export async function handleDocumentos(
  { db, telegram, chatIdStr }: HandlerDeps,
  opts: { code?: string; limit?: number }
) {
  const limit = pickLimit(opts.limit, 5);
  const code = opts.code?.toUpperCase();

  if (code) {
    const existing = await listExistingFundCodes(db, [code]);
    if (!existing.length) {
      await telegram.sendText(chatIdStr, `Fundo não encontrado: ${code}`);
      return;
    }
    const docs = await listLatestDocuments([code], limit);
    await telegram.sendText(chatIdStr, formatDocumentsMessage({ docs, limit, code }));
    return;
  }

  const funds = await listTelegramUserFunds(db, chatIdStr);
  if (!funds.length) {
    await telegram.sendText(chatIdStr, 'Sua lista está vazia.');
    return;
  }
  const docs = await listLatestDocuments(funds, limit);
  await telegram.sendText(chatIdStr, formatDocumentsMessage({ docs, limit }));
}

export async function handleResumoDocumento({ db, telegram, chatIdStr }: HandlerDeps, codes: string[]) {
  await handleResumoDocumentoCommand({ db, chatId: chatIdStr, telegram, codes });
}
