import type { DocumentData } from '../parsers/documents';
import { promises as fs } from 'node:fs';
import path from 'node:path';
import { formatNewDocumentMessage } from './webhook-messages';

export type TelegramUpdate = {
  update_id: number;
  message?: TelegramMessage;
  callback_query?: TelegramCallbackQuery;
};

export type TelegramMessage = {
  message_id: number;
  date: number;
  chat: { id: number; type: string; username?: string; first_name?: string; last_name?: string };
  text?: string;
};

export type TelegramCallbackQuery = {
  id: string;
  data?: string;
  message?: TelegramMessage;
};

type TelegramApiOk<T> = { ok: true; result: T };
type TelegramApiErr = { ok: false; description?: string; error_code?: number };

type TelegramBotCommand = { command: string; description: string };

type TelegramSendMessageResponse = {
  message_id: number;
};

type TelegramInlineKeyboardButton = {
  text: string;
  callback_data: string;
};

type TelegramReplyMarkup = {
  inline_keyboard: TelegramInlineKeyboardButton[][];
};

type TelegramSendMessageOpts = {
  disableWebPagePreview?: boolean;
  parseMode?: 'HTML' | 'MarkdownV2';
  replyMarkup?: TelegramReplyMarkup;
};

type TelegramEditMessageTextOpts = {
  disableWebPagePreview?: boolean;
  parseMode?: 'HTML' | 'MarkdownV2';
  replyMarkup?: TelegramReplyMarkup;
};

const registeredCommandsSignatureByToken = new Map<string, string>();

function buildQuery(qs: Record<string, string | number | boolean | undefined>): string {
  const query = Object.entries(qs)
    .filter(([, v]) => v !== undefined)
    .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(String(v))}`)
    .join('&');
  return query ? `?${query}` : '';
}

function createTelegramBaseUrl(token: string): string {
  return `https://api.telegram.org/bot${token}`;
}

async function telegramRequest<T>(
  baseUrl: string,
  path: string,
  opts?: { method?: 'GET' | 'POST'; body?: unknown; qs?: Record<string, string | number | boolean | undefined> }
): Promise<T> {
  const url = `${baseUrl}/${path}${buildQuery(opts?.qs ?? {})}`;
  const hasBody = opts?.body !== undefined;
  const res = await fetch(url, {
    method: opts?.method ?? (hasBody ? 'POST' : 'GET'),
    headers: hasBody ? { 'content-type': 'application/json' } : undefined,
    body: hasBody ? JSON.stringify(opts?.body) : undefined,
  });

  const json = (await res.json()) as TelegramApiOk<T> | TelegramApiErr;
  if (!json || typeof json !== 'object' || !('ok' in json)) throw new Error(`TELEGRAM_BAD_RESPONSE:${String(res.status)}`);
  if (json.ok) return json.result;
  throw new Error(`TELEGRAM_API_ERR:${json.error_code ?? res.status}:${json.description ?? 'unknown'}`);
}

async function telegramRequestForm<T>(
  baseUrl: string,
  path: string,
  opts: { form: FormData; qs?: Record<string, string | number | boolean | undefined> }
): Promise<T> {
  const url = `${baseUrl}/${path}${buildQuery(opts.qs ?? {})}`;
  const res = await fetch(url, { method: 'POST', body: opts.form });
  const json = (await res.json()) as TelegramApiOk<T> | TelegramApiErr;
  if (!json || typeof json !== 'object' || !('ok' in json)) throw new Error(`TELEGRAM_BAD_RESPONSE:${String(res.status)}`);
  if (json.ok) return json.result;
  throw new Error(`TELEGRAM_API_ERR:${json.error_code ?? res.status}:${json.description ?? 'unknown'}`);
}

export function createTelegramClient(token: string) {
  const baseUrl = createTelegramBaseUrl(token);

  async function getUpdates(opts: { offset?: number; timeoutSec?: number }): Promise<TelegramUpdate[]> {
    return telegramRequest<TelegramUpdate[]>(baseUrl, 'getUpdates', {
      method: 'GET',
      qs: { offset: opts.offset, timeout: opts.timeoutSec ?? 25, allowed_updates: JSON.stringify(['message', 'callback_query']) },
    });
  }

  async function setMyCommands(commands: TelegramBotCommand[]): Promise<boolean> {
    return telegramRequest<boolean>(baseUrl, 'setMyCommands', {
      method: 'POST',
      body: { commands },
    });
  }

  async function sendMessage(chatId: string | number, text: string, opts?: TelegramSendMessageOpts): Promise<TelegramSendMessageResponse> {
    return telegramRequest<TelegramSendMessageResponse>(baseUrl, 'sendMessage', {
      method: 'POST',
      body: {
        chat_id: chatId,
        text,
        disable_web_page_preview: opts?.disableWebPagePreview ?? true,
        parse_mode: opts?.parseMode,
        reply_markup: opts?.replyMarkup,
      },
    });
  }

  async function editMessageText(
    chatId: string | number,
    messageId: number,
    text: string,
    opts?: TelegramEditMessageTextOpts
  ): Promise<TelegramSendMessageResponse> {
    return telegramRequest<TelegramSendMessageResponse>(baseUrl, 'editMessageText', {
      method: 'POST',
      body: {
        chat_id: chatId,
        message_id: messageId,
        text,
        disable_web_page_preview: opts?.disableWebPagePreview ?? true,
        parse_mode: opts?.parseMode,
        reply_markup: opts?.replyMarkup,
      },
    });
  }

  async function sendDocument(
    chatId: string | number,
    opts: { filePath: string; filename?: string; caption?: string; contentType?: string }
  ): Promise<TelegramSendMessageResponse> {
    const buf = await fs.readFile(opts.filePath);
    const form = new FormData();
    form.append('chat_id', String(chatId));
    if (opts.caption) form.append('caption', opts.caption);
    const filename = opts.filename ?? path.basename(opts.filePath);
    form.append('document', new Blob([buf], { type: opts.contentType ?? 'application/octet-stream' }), filename);
    return telegramRequestForm<TelegramSendMessageResponse>(baseUrl, 'sendDocument', { form });
  }

  async function answerCallbackQuery(
    callbackQueryId: string,
    opts?: { text?: string; showAlert?: boolean }
  ): Promise<boolean> {
    return telegramRequest<boolean>(baseUrl, 'answerCallbackQuery', {
      method: 'POST',
      body: {
        callback_query_id: callbackQueryId,
        text: opts?.text,
        show_alert: opts?.showAlert,
      },
    });
  }

  return { getUpdates, setMyCommands, sendMessage, editMessageText, sendDocument, answerCallbackQuery };
}

export function createTelegramService(token: string) {
  const client = createTelegramClient(token);

  async function registerDefaultCommandsOnce(): Promise<{ ok: true } | { ok: false }> {
    const commands: TelegramBotCommand[] = [
      { command: 'menu', description: 'Mostrar comandos' },
      { command: 'lista', description: 'Ver sua lista de fundos' },
      { command: 'categorias', description: 'Resumo por categoria da sua lista' },
      { command: 'export', description: 'Exportar JSON da sua lista (ou fundos específicos)' },
      { command: 'rank', description: 'Rank hoje (oportunidades de valorização)' },
      { command: 'rankv', description: 'Rank Value (todos os fundos da base)' },
      { command: 'documentos', description: 'Últimos documentos (da lista ou por fundo)' },
      { command: 'resumo_documento', description: 'Resumo do último documento (2 fundos)' },
      { command: 'pesquisa', description: 'Resumo do fundo' },
      { command: 'cotation', description: 'Variações, drawdown e volatilidade (cache)' },
      { command: 'set', description: 'Substituir sua lista de fundos' },
      { command: 'add', description: 'Adicionar fundos na lista' },
      { command: 'remove', description: 'Remover fundos da lista' },
      { command: 'confirm', description: 'Confirmar ação pendente' },
      { command: 'cancel', description: 'Cancelar ação pendente' },
      { command: 'help', description: 'Ajuda' },
    ];
    const signature = JSON.stringify(commands);
    if (registeredCommandsSignatureByToken.get(token) === signature) return { ok: true };
    try {
      await client.setMyCommands(commands);
      registeredCommandsSignatureByToken.set(token, signature);
      return { ok: true };
    } catch {
      return { ok: false };
    }
  }

  async function sendTextAndGetMessageId(
    chatId: string | number,
    text: string,
    opts?: TelegramSendMessageOpts
  ): Promise<{ ok: true; messageId: number } | { ok: false }> {
    try {
      const res = await client.sendMessage(chatId, text, opts);
      return { ok: true, messageId: res.message_id };
    } catch {
      return { ok: false };
    }
  }

  async function sendText(
    chatId: string | number,
    text: string,
    opts?: TelegramSendMessageOpts
  ): Promise<{ ok: true } | { ok: false }> {
    try {
      await client.sendMessage(chatId, text, opts);
      return { ok: true };
    } catch {
      return { ok: false };
    }
  }

  async function sendDocument(
    chatId: string | number,
    opts: { filePath: string; filename?: string; caption?: string; contentType?: string }
  ): Promise<{ ok: true } | { ok: false }> {
    try {
      await client.sendDocument(chatId, opts);
      return { ok: true };
    } catch {
      return { ok: false };
    }
  }

  async function editText(
    chatId: string | number,
    messageId: number,
    text: string,
    opts?: TelegramEditMessageTextOpts
  ): Promise<{ ok: true } | { ok: false }> {
    try {
      await client.editMessageText(chatId, messageId, text, opts);
      return { ok: true };
    } catch {
      return { ok: false };
    }
  }

  async function ackCallbackQuery(callbackQueryId: string): Promise<{ ok: true } | { ok: false }> {
    try {
      await client.answerCallbackQuery(callbackQueryId);
      return { ok: true };
    } catch {
      return { ok: false };
    }
  }

  async function notifyNewDocuments(opts: {
    chatIds: string[];
    fundCode: string;
    documents: DocumentData[];
  }): Promise<{ sent: number; failed: number }> {
    const chatIds = opts.chatIds.filter(Boolean);
    if (chatIds.length === 0) return { sent: 0, failed: 0 };

    let sent = 0;
    let failed = 0;
    for (const chatId of chatIds) {
      for (const d of opts.documents) {
        const text = formatNewDocumentMessage(opts.fundCode, d);
        const res = await sendText(chatId, text);
        if (res.ok) {
          sent++;
        } else {
          failed++;
        }
      }
    }
    return { sent, failed };
  }

  return { registerDefaultCommandsOnce, sendText, sendDocument, sendTextAndGetMessageId, editText, ackCallbackQuery, notifyNewDocuments };
}
