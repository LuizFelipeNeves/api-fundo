import type { DocumentData } from '../parsers/documents';

export type TelegramUpdate = {
  update_id: number;
  message?: TelegramMessage;
};

export type TelegramMessage = {
  message_id: number;
  date: number;
  chat: { id: number; type: string; username?: string; first_name?: string; last_name?: string };
  text?: string;
};

type TelegramApiOk<T> = { ok: true; result: T };
type TelegramApiErr = { ok: false; description?: string; error_code?: number };

type TelegramBotCommand = { command: string; description: string };

type TelegramSendMessageResponse = {
  message_id: number;
};

type TelegramSendMessageOpts = {
  disableWebPagePreview?: boolean;
  parseMode?: 'HTML' | 'MarkdownV2';
};

const registeredTokens = new Set<string>();

function cleanLine(value: unknown): string {
  return String(value || '')
    .replace(/\s+/g, ' ')
    .trim();
}

function formatDate(value: string): string {
  const v = cleanLine(value);
  if (!v) return '';
  const iso = v.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (iso) return `${iso[3]}/${iso[2]}/${iso[1]}`;
  return v;
}

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

export function createTelegramClient(token: string) {
  const baseUrl = createTelegramBaseUrl(token);

  async function getUpdates(opts: { offset?: number; timeoutSec?: number }): Promise<TelegramUpdate[]> {
    return telegramRequest<TelegramUpdate[]>(baseUrl, 'getUpdates', {
      method: 'GET',
      qs: { offset: opts.offset, timeout: opts.timeoutSec ?? 25, allowed_updates: JSON.stringify(['message']) },
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
      },
    });
  }

  return { getUpdates, setMyCommands, sendMessage };
}

export function formatNewDocumentMessage(fundCode: string, d: DocumentData): string {
  const code = cleanLine(fundCode).toUpperCase();
  const title = cleanLine(d.title);
  const category = cleanLine(d.category);
  const type = cleanLine(d.type);
  const status = cleanLine(d.status);
  const url = cleanLine(d.url);
  const dateUpload = formatDate(d.dateUpload);
  const dateDoc = formatDate(d.date);
  const version = Number.isFinite(d.version) ? String(d.version) : '';

  const header = `Novo documento publicado para ${code}`;
  const docLine = category || type ? `Documento: ${[category, type].filter(Boolean).join(' · ')}` : '';
  const titleLine = title ? `Título: ${title}` : '';
  const whenLine = dateUpload
    ? `Data de upload: ${dateUpload}${dateDoc && dateDoc !== dateUpload ? ` (doc: ${dateDoc})` : ''}`
    : dateDoc
      ? `Data do documento: ${dateDoc}`
      : '';
  const statusLine = status ? `Status: ${status}` : '';
  const versionLine = version ? `Versão: ${version}` : '';
  const linkLine = url ? `Link: ${url}` : '';

  return [header, docLine, titleLine, whenLine, statusLine, versionLine, linkLine].filter(Boolean).join('\n');
}

export function createTelegramService(token: string) {
  const client = createTelegramClient(token);

  async function registerDefaultCommandsOnce(): Promise<{ ok: true } | { ok: false }> {
    if (registeredTokens.has(token)) return { ok: true };
    try {
      await client.setMyCommands([
        { command: 'menu', description: 'Mostrar comandos' },
        { command: 'lista', description: 'Ver sua lista de fundos' },
        { command: 'set', description: 'Substituir sua lista de fundos' },
        { command: 'add', description: 'Adicionar fundos na lista' },
        { command: 'remove', description: 'Remover fundos da lista' },
        { command: 'help', description: 'Ajuda' },
      ]);
      registeredTokens.add(token);
      return { ok: true };
    } catch {
      return { ok: false };
    }
  }

  async function sendText(chatId: string | number, text: string): Promise<{ ok: true } | { ok: false }> {
    try {
      await client.sendMessage(chatId, text);
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

  return { registerDefaultCommandsOnce, sendText, notifyNewDocuments };
}
