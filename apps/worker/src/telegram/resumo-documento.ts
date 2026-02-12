import { createHash } from 'node:crypto';
import { promises as fs } from 'node:fs';
import path from 'node:path';
import { listExistingFundCodes, listTelegramUserFunds } from './storage';
import { downloadDocumentToDataFileCached } from '../telegram-bot/document-pdf';
import { extract } from '../telegram-bot/html-extract';
import { errorToMeta, logger } from '../utils/logger';
import { getRawSql } from '../db';

type TelegramLike = {
  sendText: (chatId: string | number, text: string) => Promise<{ ok: true } | { ok: false }>;
  sendDocument: (chatId: string | number, opts: { filePath: string; filename?: string; caption?: string; contentType?: string }) => Promise<
    | { ok: true }
    | { ok: false }
  >;
  sendTextAndGetMessageId: (chatId: string | number, text: string) => Promise<{ ok: true; messageId: number } | { ok: false }>;
  editText: (chatId: string | number, messageId: number, text: string) => Promise<{ ok: true } | { ok: false }>;
};

function clipText(value: string, maxChars: number): string {
  const v = String(value || '').trim();
  if (!v) return '';
  if (v.length <= maxChars) return v;
  return `${v.slice(0, Math.max(0, maxChars - 1)).trimEnd()}…`;
}

function buildFinalMessage(
  results: Array<
    | {
        code: string;
        status: 'ok';
        doc: { id: number; category: string; type: string; dateUpload: string; url: string };
      }
    | { code: string; status: 'err'; reason: string }
  >
): string {
  const lines: string[] = [];
  for (const r of results) {
    if (r.status === 'err') {
      lines.push(`⚠️ ${r.code}: ${r.reason}`);
      continue;
    }
  }
  return lines.join('\n').trim();
}

function resolveDataDir(): string {
  const dataDir = path.resolve(process.cwd(), 'data');
  return dataDir;
}

function cacheKeyForDocument(opts: { fundCode: string; documentId: number; url: string }): string {
  const urlHash = createHash('sha1').update(String(opts.url || '')).digest('hex').slice(0, 10);
  return `${opts.fundCode.toUpperCase()}-${String(opts.documentId)}-${urlHash}`;
}

async function listLatestDocuments(codes: string[]) {
  const sql = getRawSql();
  const rows = await sql.unsafe<{ fund_code: string; id: number; category: string; type: string; dateUpload: string; url: string }[]>(
    `SELECT fund_code, document_id AS id, category, type, "dateUpload" AS "dateUpload", url
    FROM document
    WHERE fund_code = ANY($1)
    ORDER BY date_upload_iso DESC, document_id DESC`,
    [codes]
  );

  const byCode = new Map<string, { id: number; category: string; type: string; dateUpload: string; url: string }>();
  for (const row of rows) {
    if (!byCode.has(row.fund_code)) {
      byCode.set(row.fund_code, { id: row.id, category: row.category, type: row.type, dateUpload: row.dateUpload, url: row.url });
    }
  }
  return byCode;
}

async function htmlToText(htmlPath: string): Promise<string> {
  const html = await fs.readFile(htmlPath, 'utf-8');
  const data = extract(html);
  const lines = Object.entries(data).map(([k, v]) => `${k}: ${v}`);
  return lines.join('\n');
}

export async function handleResumoDocumentoCommand(opts: {
  db: unknown;
  chatId: string;
  telegram: TelegramLike;
  codes: string[];
}): Promise<void> {
  const started = await opts.telegram.sendTextAndGetMessageId(opts.chatId, '⏳ Gerando resumo…');
  const startedMessageId = started.ok ? started.messageId : null;

  const portfolio = await listTelegramUserFunds(opts.db, opts.chatId);
  const requested = (opts.codes.length ? opts.codes : portfolio).map((x) => x.toUpperCase());
  const unique = Array.from(new Set(requested)).filter(Boolean).slice(0, 2);
  if (!unique.length) {
    const msg = 'Sua lista está vazia.';
    if (startedMessageId) {
      const edited = await opts.telegram.editText(opts.chatId, startedMessageId, msg);
      if (edited.ok) return;
    }
    await opts.telegram.sendText(opts.chatId, msg);
    return;
  }

  const existing = (await listExistingFundCodes(opts.db, unique)).slice(0, 2);
  if (!existing.length) {
    const msg = `Nenhum fundo encontrado: ${unique.join(', ')}`;
    if (startedMessageId) {
      const edited = await opts.telegram.editText(opts.chatId, startedMessageId, msg);
      if (edited.ok) return;
    }
    await opts.telegram.sendText(opts.chatId, msg);
    return;
  }

  const latestDocs = await listLatestDocuments(existing);
  const targets = existing
    .map((code) => ({ code, doc: latestDocs.get(code) }))
    .filter((x) => x.doc);

  if (!targets.length) {
    const msg = `Sem documentos no banco para: ${existing.join(', ')}`;
    if (startedMessageId) {
      const edited = await opts.telegram.editText(opts.chatId, startedMessageId, msg);
      if (edited.ok) return;
    }
    await opts.telegram.sendText(opts.chatId, msg);
    return;
  }

  const results: Array<
    | {
        code: string;
        status: 'ok';
        doc: { id: number; category: string; type: string; dateUpload: string; url: string };
      }
    | { code: string; status: 'err'; reason: string }
  > = [];

  for (const t of targets) {
    try {
      const url = String(t.doc?.url || '');
      const cacheDir = path.join(resolveDataDir(), 'telegram', 'documents');
      const downloaded = await downloadDocumentToDataFileCached({
        url,
        cacheDir,
        cacheKey: cacheKeyForDocument({ fundCode: t.code, documentId: t.doc!.id, url }),
      });

      const docType = [String(t.doc!.category || '').trim(), String(t.doc!.type || '').trim()].filter(Boolean).join(' · ');
      const caption = clipText([
        t.code.toUpperCase(),
        docType,
        String(t.doc!.dateUpload || '').trim(),
        String(t.doc!.url || '').trim(),
      ].filter(Boolean).join('\n'), 900);

      if (downloaded.kind === 'pdf') {
        const sent = await opts.telegram.sendDocument(opts.chatId, {
          filePath: downloaded.filePath,
          filename: `${t.code.toUpperCase()}-${t.doc!.id}.pdf`,
          caption,
          contentType: 'application/pdf',
        });
        if (!sent.ok) throw new Error('TELEGRAM_SEND_DOCUMENT_FAILED');
      } else if (downloaded.kind === 'html') {
        const text = await htmlToText(downloaded.filePath);
        const outPath = `${downloaded.filePath}.txt`;
        await fs.writeFile(outPath, text);
        const sent = await opts.telegram.sendDocument(opts.chatId, {
          filePath: outPath,
          filename: `${t.code.toUpperCase()}-${t.doc!.id}.txt`,
          caption,
          contentType: 'text/plain; charset=utf-8',
        });
        if (!sent.ok) throw new Error('TELEGRAM_SEND_DOCUMENT_FAILED');
      } else {
        throw new Error('TELEGRAM_SEND_DOCUMENT_FAILED');
      }

      results.push({
        code: t.code,
        status: 'ok',
        doc: { id: t.doc!.id, category: t.doc!.category, type: t.doc!.type, dateUpload: t.doc!.dateUpload, url: t.doc!.url },
      });
    } catch (err) {
      const rawMsg = err instanceof Error ? err.message : String(err);
      let reason = 'Não consegui processar o documento.';
      if (rawMsg === 'DOCUMENT_PDF_NOT_FOUND') reason = 'Documento não é PDF.';
      else if (rawMsg === 'DOCUMENT_NOT_PDF') reason = 'Documento não é PDF.';
      else if (rawMsg === 'TELEGRAM_SEND_DOCUMENT_FAILED') reason = 'Não consegui enviar o arquivo no Telegram.';
      else if (rawMsg.startsWith('DOWNLOAD_FAILED:')) {
        const status = rawMsg.split(':')[1] || '';
        reason = `Falha ao baixar o documento (HTTP ${status}).`;
      }

      logger.error('resumo_documento_failed', {
        fund_code: t.code,
        document_id: t.doc?.id,
        document_url: t.doc?.url,
        err: errorToMeta(err),
      });
      results.push({ code: t.code, status: 'err', reason });
    }
  }

  const finalMessage = buildFinalMessage(results);
  const msg = finalMessage || '⚠️ Não consegui gerar o resumo.';
  if (startedMessageId) {
    const edited = await opts.telegram.editText(opts.chatId, startedMessageId, msg);
    if (edited.ok) return;
  }
  await opts.telegram.sendText(opts.chatId, msg);
}
