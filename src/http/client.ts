import { getDefaultHeaders } from '../config';
import { Agent, type Dispatcher } from 'undici';
import {
  getTorProxySelection,
  maybeSignalNewnym,
  shouldForceTorForHost,
  TOR_PROXY_ENABLED_EXPORT as TOR_PROXY_ENABLED,
  TOR_PROXY_MODE_EXPORT as TOR_PROXY_MODE,
  TOR_PROXY_REQUIRED_EXPORT as TOR_PROXY_REQUIRED,
} from './tor';

const DEFAULT_TIMEOUT = 25000; // 25 segundos

/* ======================================================
   🔥 GOD NETWORK AGENT para FNET (keepalive agressivo)
====================================================== */

const FNET_AGENT = new Agent({
  keepAliveTimeout: 60_000,
  keepAliveMaxTimeout: 120_000,
  connections: 50,
  pipelining: 1,
});

const FNET_SESSION_TTL_MS = 60 * 60 * 1000; // 1 hora

function extractJSessionId(resp: Response): string | null {
  const raw = resp.headers.get('set-cookie') || resp.headers.get('set-cookie2');
  console.log('[FNET] extractJSessionId - all headers:', Object.fromEntries(resp.headers.entries()));
  if (!raw) {
    console.log('[FNET] extractJSessionId: sem set-cookie');
    return null;
  }

  // Bun às vezes concatena cookies em linha única
  const match = raw.match(/JSESSIONID=[^;]+/i);
  console.log('[FNET] extractJSessionId:', { raw: raw.slice(0, 200), match: match?.[0] });
  return match ? match[0] : null;
}

function getFnetHeaders(cookie?: string): Record<string, string> {
  const headers: Record<string, string> = {
    'User-Agent': 'Mozilla/5.0',
    Accept: 'application/json, text/javascript, */*; q=0.01',
    'x-requested-with': 'XMLHttpRequest',
    Connection: 'keep-alive',
  };
  if (cookie) headers['Cookie'] = cookie;
  return headers;
}

function getFnetInitHeaders(): Record<string, string> {
  return {
    'User-Agent': 'Mozilla/5.0',
    Accept: '*/*',
    Connection: 'keep-alive',
  };
}
const DEFAULT_RETRY_MAX = 4;
const DEFAULT_RETRY_BASE_MS = 600;
const DEFAULT_HOST_CONCURRENCY = 3;
const DEFAULT_HOST_MIN_TIME_MS = 150;
const DEFAULT_STATUSINVEST_HOST_CONCURRENCY = 1;
const DEFAULT_STATUSINVEST_HOST_MIN_TIME_MS = 120;
const DEFAULT_OTHER_HOST_CONCURRENCY = 6;
const DEFAULT_OTHER_HOST_MIN_TIME_MS = 0;
const DEFAULT_FNET_HOST_CONCURRENCY = 2;
const DEFAULT_FNET_HOST_MIN_TIME_MS = 150;

interface RequestOptions {
  timeout?: number;
  headers?: Record<string, string>;
  retryMax?: number;
  retryBaseMs?: number;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function parsePositiveInt(raw: string | undefined, fallback: number): number {
  const parsed = raw ? Number.parseInt(raw, 10) : NaN;
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function clampInt(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, Math.floor(value)));
}

const HTTP_RETRY_MAX = clampInt(parsePositiveInt(process.env.HTTP_RETRY_MAX, DEFAULT_RETRY_MAX), 1, 20);
const HTTP_RETRY_BASE_MS = clampInt(parsePositiveInt(process.env.HTTP_RETRY_BASE_MS, DEFAULT_RETRY_BASE_MS), 50, 60000);

const I10_HOST_CONCURRENCY = clampInt(
  parsePositiveInt(process.env.INVESTIDOR10_CONCURRENCY, DEFAULT_HOST_CONCURRENCY),
  1,
  20
);
const I10_HOST_MIN_TIME_MS = clampInt(
  parsePositiveInt(process.env.INVESTIDOR10_MIN_TIME_MS, DEFAULT_HOST_MIN_TIME_MS),
  0,
  60000
);
const STATUSINVEST_HOST_CONCURRENCY = clampInt(
  parsePositiveInt(process.env.STATUSINVEST_CONCURRENCY, DEFAULT_STATUSINVEST_HOST_CONCURRENCY),
  1,
  20
);
const STATUSINVEST_HOST_MIN_TIME_MS = clampInt(
  parsePositiveInt(process.env.STATUSINVEST_MIN_TIME_MS, DEFAULT_STATUSINVEST_HOST_MIN_TIME_MS),
  0,
  60000
);
const OTHER_HOST_CONCURRENCY = clampInt(
  parsePositiveInt(process.env.OTHER_HOST_CONCURRENCY, DEFAULT_OTHER_HOST_CONCURRENCY),
  1,
  100
);
const OTHER_HOST_MIN_TIME_MS = clampInt(
  parsePositiveInt(process.env.OTHER_HOST_MIN_TIME_MS, DEFAULT_OTHER_HOST_MIN_TIME_MS),
  0,
  60000
);
const FNET_HOST_CONCURRENCY = clampInt(
  parsePositiveInt(process.env.FNET_HOST_CONCURRENCY, DEFAULT_FNET_HOST_CONCURRENCY),
  1,
  20
);
const FNET_HOST_MIN_TIME_MS = clampInt(
  parsePositiveInt(process.env.FNET_HOST_MIN_TIME_MS, DEFAULT_FNET_HOST_MIN_TIME_MS),
  0,
  60000
);

type HostLimiterState = {
  inflight: number;
  lastStartAt: number;
  lastAccessAt: number;
  queue: Array<{
    run: () => Promise<Response>;
    resolve: (value: Response) => void;
    reject: (reason: unknown) => void;
  }>;
  draining: boolean;
};

const HOST_LIMITERS = new Map<string, HostLimiterState>();
const HOST_LIMITER_IDLE_TIMEOUT_MS = 5 * 60 * 1000; // 5 minutos

function getHostLimiter(hostname: string): HostLimiterState {
  const now = Date.now();
  const existing = HOST_LIMITERS.get(hostname);
  if (existing) {
    existing.lastAccessAt = now;
    return existing;
  }
  const created: HostLimiterState = { inflight: 0, lastStartAt: 0, lastAccessAt: now, queue: [], draining: false };
  HOST_LIMITERS.set(hostname, created);
  return created;
}

function cleanupIdleHostLimiters(): number {
  const now = Date.now();
  let cleaned = 0;
  for (const [hostname, state] of HOST_LIMITERS) {
    if (state.inflight === 0 && state.queue.length === 0 && now - state.lastAccessAt > HOST_LIMITER_IDLE_TIMEOUT_MS) {
      HOST_LIMITERS.delete(hostname);
      cleaned++;
    }
  }
  return cleaned;
}

// Cleanup periódico de limiters inativos
if (typeof setInterval !== 'undefined') {
  const timer = setInterval(() => {
    const cleaned = cleanupIdleHostLimiters();
    if (cleaned > 0) {
      process.stderr.write(`[http] cleaned ${cleaned} idle host limiters\n`);
    }
  }, 60 * 1000); // a cada minuto
  (timer as any).unref?.();
}

function getHostPolicy(hostname: string): { maxConcurrent: number; minTimeMs: number } {
  if (hostname === 'investidor10.com.br' || hostname.endsWith('.investidor10.com.br')) {
    return { maxConcurrent: I10_HOST_CONCURRENCY, minTimeMs: I10_HOST_MIN_TIME_MS };
  }
  if (hostname === 'statusinvest.com.br' || hostname.endsWith('.statusinvest.com.br')) {
    return { maxConcurrent: STATUSINVEST_HOST_CONCURRENCY, minTimeMs: STATUSINVEST_HOST_MIN_TIME_MS };
  }
  if (hostname === 'fnet.bmfbovespa.com.br' || hostname.endsWith('.bmfbovespa.com.br')) {
    return { maxConcurrent: FNET_HOST_CONCURRENCY, minTimeMs: FNET_HOST_MIN_TIME_MS };
  }
  return { maxConcurrent: OTHER_HOST_CONCURRENCY, minTimeMs: OTHER_HOST_MIN_TIME_MS };
}

function drainHostLimiter(hostname: string) {
  const state = getHostLimiter(hostname);
  if (state.draining) return;
  state.draining = true;

  const { maxConcurrent, minTimeMs } = getHostPolicy(hostname);

  const loop = () => {
    while (state.inflight < maxConcurrent) {
      const item = state.queue.shift();
      if (!item) break;

      const now = Date.now();
      const waitMs = minTimeMs - (now - state.lastStartAt);
      if (waitMs > 0) {
        state.queue.unshift(item);
        setTimeout(loop, waitMs);
        return;
      }

      state.inflight++;
      state.lastStartAt = now;
      item
        .run()
        .then(item.resolve, item.reject)
        .finally(() => {
          state.inflight--;
          drainHostLimiter(hostname);
        });
    }

    state.draining = false;
  };

  loop();
}

async function withHostLimit(url: string, run: () => Promise<Response>): Promise<Response> {
  const hostname = new URL(url).hostname;
  const state = getHostLimiter(hostname);
  return new Promise<Response>((resolve, reject) => {
    state.queue.push({ run, resolve, reject });
    drainHostLimiter(hostname);
  });
}

function isRetryableStatus(status: number): boolean {
  return (
    status === 403 ||
    status === 429 ||
    status === 500 ||
    status === 502 ||
    status === 503 ||
    status === 504 ||
    status === 520 ||
    status === 521 ||
    status === 522 ||
    status === 523 ||
    status === 524
  );
}

function parseRetryAfterMs(headerValue: string | null): number | null {
  if (!headerValue) return null;
  const seconds = Number.parseInt(headerValue, 10);
  if (Number.isFinite(seconds) && seconds >= 0) return seconds * 1000;
  const dateMs = Date.parse(headerValue);
  if (Number.isFinite(dateMs)) return Math.max(0, dateMs - Date.now());
  return null;
}

function computeBackoffMs(
  attempt: number,
  status: number | undefined,
  retryAfterMs: number | null | undefined,
  baseMs: number
): number {
  if (retryAfterMs !== null && retryAfterMs !== undefined) {
    return clampInt(retryAfterMs, 0, 300000);
  }
  const base = status === 429 || status === 403 ? baseMs * 2 : baseMs;
  const exp = Math.min(6, Math.max(0, attempt));
  const jitter = Math.floor(Math.random() * 250);
  return clampInt(base * Math.pow(2, exp) + jitter, 0, 300000);
}

function resolveRetryMax(options: RequestOptions | undefined): number {
  return clampInt(options?.retryMax ?? HTTP_RETRY_MAX, 1, 50);
}

function resolveRetryBaseMs(options: RequestOptions | undefined): number {
  return clampInt(options?.retryBaseMs ?? HTTP_RETRY_BASE_MS, 50, 60000);
}

async function readResponseSnippet(response: Response, maxChars: number): Promise<string> {
  try {
    const text = await response.text();
    const trimmed = text.replace(/\s+/g, ' ').trim();
    if (!trimmed) return '';
    return trimmed.length > maxChars ? `${trimmed.slice(0, maxChars)}…` : trimmed;
  } catch {
    return '';
  }
}

async function throwHttpError(response: Response, method: string, url: string): Promise<never> {
  const statusText = response.statusText ? ` ${response.statusText}` : '';
  const contentType = response.headers.get('content-type') || '';
  const snippet = await readResponseSnippet(response, 800);
  const bodyPart = snippet ? ` body="${snippet}"` : '';
  const typePart = contentType ? ` content_type="${contentType}"` : '';
  throw new Error(`${method} ${url} -> HTTP ${response.status}${statusText}${typePart}${bodyPart}`);
}

type RequestInitWithDispatcher = RequestInit & { dispatcher?: Dispatcher };

function parseCookieNameValuesFromHeader(setCookieHeader: string): string[] {
  const cookies: string[] = [];
  const header = setCookieHeader.trim();
  if (!header) return cookies;

  const boundaries: number[] = [0];
  let inExpires = false;

  for (let i = 0; i < header.length; i++) {
    const ch = header[i];

    if (!inExpires) {
      if (
        (ch === 'E' || ch === 'e') &&
        header.slice(i, i + 8).toLowerCase() === 'expires='
      ) {
        inExpires = true;
        i += 7;
        continue;
      }
    } else if (ch === ';') {
      inExpires = false;
      continue;
    }

    if (ch !== ',' || inExpires) continue;

    const rest = header.slice(i + 1);
    const match = rest.match(/^\s*([^=;, \t]+)=/);
    if (!match) continue;

    boundaries.push(i + 1);
  }

  boundaries.push(header.length + 1);
  for (let i = 0; i < boundaries.length - 1; i++) {
    const start = boundaries[i]!;
    const end = boundaries[i + 1]!;
    const part = header.slice(start, end - 1).trim();
    if (!part) continue;
    const nameValue = part.split(';', 1)[0]?.trim();
    if (nameValue) cookies.push(nameValue);
  }

  return cookies;
}

async function fetchWithRetry(
  method: string,
  url: string,
  init: RequestInit,
  options: RequestOptions
): Promise<Response> {
  const timeout = options.timeout ?? DEFAULT_TIMEOUT;
  const retryMax = resolveRetryMax(options);
  const retryBaseMs = resolveRetryBaseMs(options);

  let lastError: unknown = null;
  for (let attempt = 0; attempt < retryMax; attempt++) {
    try {
      const hostname = new URL(url).hostname;
      const shouldUseTor =
        TOR_PROXY_ENABLED &&
        (TOR_PROXY_REQUIRED || TOR_PROXY_MODE === 'always' || attempt > 0 || shouldForceTorForHost(hostname));

      const tor = shouldUseTor ? await getTorProxySelection() : null;
      if (shouldUseTor && TOR_PROXY_REQUIRED && !tor) {
        await sleep(250);
        throw new Error('TOR_PROXY_UNAVAILABLE');
      }
      if (tor) {
        maybeSignalNewnym(tor.proxyUrl).catch(() => null);
      }
      const response = await withHostLimit(url, async () => {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), timeout);
        try {
          return await fetch(
            url,
            {
              ...init,
              signal: controller.signal,
              ...(tor ? { dispatcher: tor.dispatcher } : {}),
            } satisfies RequestInitWithDispatcher
          );
        } finally {
          clearTimeout(timeoutId);
        }
      });

      if (!response.ok && isRetryableStatus(response.status) && attempt + 1 < retryMax) {
        const retryAfterMs = parseRetryAfterMs(response.headers.get('retry-after'));
        try {
          response.body?.cancel();
        } catch {
          null;
        }
        await sleep(computeBackoffMs(attempt, response.status, retryAfterMs, retryBaseMs));
        continue;
      }

      return response;
    } catch (error) {
      if (error instanceof Error && error.name === 'AbortError') {
        throw new Error(`${method} ${url} -> timeout_after_ms=${timeout}`);
      }
      lastError = error;
      if (attempt + 1 < retryMax) {
        await sleep(computeBackoffMs(attempt, undefined, undefined, retryBaseMs));
        continue;
      }
      throw error;
    }
  }

  throw lastError instanceof Error ? lastError : new Error(`${method} ${url} -> request_failed`);
}

async function request<T>(
  url: string,
  options: RequestOptions = {}
): Promise<T> {
  const retryMax = resolveRetryMax(options);
  const retryBaseMs = resolveRetryBaseMs(options);
  for (let attempt = 0; attempt < retryMax; attempt++) {
    const response = await fetchWithRetry(
      'GET',
      url,
      { headers: options.headers ?? getDefaultHeaders() },
      options
    );

    if (!response.ok) {
      await throwHttpError(response, 'GET', url);
    }

    const contentType = response.headers.get('content-type') || '';
    if (!contentType.toLowerCase().includes('application/json')) {
      if (attempt + 1 < retryMax && contentType.toLowerCase().includes('text/html')) {
        try {
          response.body?.cancel();
        } catch {
          null;
        }
        await sleep(computeBackoffMs(attempt, 503, null, retryBaseMs));
        continue;
      }
      const snippet = await readResponseSnippet(response, 800);
      const typePart = contentType ? ` content_type="${contentType}"` : '';
      const bodyPart = snippet ? ` body="${snippet}"` : '';
      throw new Error(`GET ${url} -> unexpected_response${typePart}${bodyPart}`);
    }

    try {
      return (await response.json()) as T;
    } catch {
      const snippet = await readResponseSnippet(response, 800);
      const typePart = contentType ? ` content_type="${contentType}"` : '';
      const bodyPart = snippet ? ` body="${snippet}"` : '';
      throw new Error(`GET ${url} -> invalid_json${typePart}${bodyPart}`);
    }
  }

  throw new Error(`GET ${url} -> request_failed`);
}

export async function get<T>(url: string, options?: RequestOptions): Promise<T> {
  return request<T>(url, options);
}

export async function post<T>(
  url: string,
  body: string,
  options?: RequestOptions
): Promise<T> {
  const retryMax = resolveRetryMax(options);
  const retryBaseMs = resolveRetryBaseMs(options);
  for (let attempt = 0; attempt < retryMax; attempt++) {
    const response = await fetchWithRetry(
      'POST',
      url,
      {
        method: 'POST',
        headers: options?.headers ?? getDefaultHeaders(),
        body,
      },
      options ?? {}
    );

    if (!response.ok) {
      await throwHttpError(response, 'POST', url);
    }

    const contentType = response.headers.get('content-type') || '';
    if (!contentType.toLowerCase().includes('application/json')) {
      if (attempt + 1 < retryMax && contentType.toLowerCase().includes('text/html')) {
        try {
          response.body?.cancel();
        } catch {
          null;
        }
        await sleep(computeBackoffMs(attempt, 503, null, retryBaseMs));
        continue;
      }
      const snippet = await readResponseSnippet(response, 800);
      const typePart = contentType ? ` content_type="${contentType}"` : '';
      const bodyPart = snippet ? ` body="${snippet}"` : '';
      throw new Error(`POST ${url} -> unexpected_response${typePart}${bodyPart}`);
    }

    try {
      return (await response.json()) as T;
    } catch {
      const snippet = await readResponseSnippet(response, 800);
      const typePart = contentType ? ` content_type="${contentType}"` : '';
      const bodyPart = snippet ? ` body="${snippet}"` : '';
      throw new Error(`POST ${url} -> invalid_json${typePart}${bodyPart}`);
    }
  }

  throw new Error(`POST ${url} -> request_failed`);
}

export async function fetchText(url: string, options?: RequestOptions): Promise<string> {
  const headers: Record<string, string> = {
    ...getDefaultHeaders(),
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
  };
  delete (headers as any)['content-type'];
  delete (headers as any)['x-requested-with'];
  delete (headers as any)['x-csrf-token'];
  const response = await fetchWithRetry('GET', url, { method: 'GET', headers }, options ?? {});

  if (response.status === 410) {
    throw new Error('FII_NOT_FOUND');
  }

  if (!response.ok) {
    await throwHttpError(response, 'GET', url);
  }

  return response.text();
}

/* ======================================================
   🔥 FNET COM SESSION CACHE (JSESSIONID reutilizável)
====================================================== */

type FnetSessionCallbacks = {
  getSession: (cnpj: string) => { jsessionId: string | null; lastValidAt: number | null };
  saveSession: (cnpj: string, jsessionId: string, lastValidAt: number) => void;
};

export async function fetchFnetWithSession<T>(
  initUrl: string,
  dataUrl: string,
  cnpj: string,
  options: RequestOptions = {},
  sessionCallbacks?: FnetSessionCallbacks
): Promise<T> {
  const getSession = sessionCallbacks?.getSession;
  const saveSession = sessionCallbacks?.saveSession;

  if (!getSession || !saveSession) {
    throw new Error('FNET_SESSION_CALLBACKS_REQUIRED');
  }

  const now = Date.now();
  const session = getSession(cnpj);
  let jsessionId: string | null = session.jsessionId;
  let lastValidAt = session.lastValidAt;

  // 🔥 Sessão válida e dentro do TTL?
  const canReuseSession = jsessionId !== null && lastValidAt !== null && now - lastValidAt < FNET_SESSION_TTL_MS;

  // ==========================================
  // PASSO 1: Se não tiver sessão válida, faz INIT para obter JSESSIONID
  // ==========================================
  if (!canReuseSession) {
    const initResp = await fetch(initUrl, {
      method: 'GET',
      headers: getFnetInitHeaders(),
      dispatcher: FNET_AGENT,
    });

    if (!initResp.ok) {
      await throwHttpError(initResp, 'GET', initUrl);
    }

    const newJsessionId = extractJSessionId(initResp);
    if (newJsessionId) {
      jsessionId = newJsessionId;
      lastValidAt = Date.now();
      saveSession(cnpj, jsessionId, lastValidAt);
    } else {
      throw new Error('FNET_INIT_NO_JSESSIONID');
    }

    try {
      initResp.body?.cancel();
    } catch {
      null;
    }
  }

  // jsessionId agora é garantido não-nulo
  let validJsessionId = jsessionId!;

  // ==========================================
  // PASSO 2: Faz a request DATA com o JSESSIONID
  // ==========================================
  const headers = getFnetHeaders(validJsessionId);
  const retryMax = resolveRetryMax(options);
  const retryBaseMs = resolveRetryBaseMs(options);

  for (let attempt = 0; attempt < retryMax; attempt++) {
    try {
      const dataResp = await fetchWithRetry('GET', dataUrl, { method: 'GET', headers }, options);

      if (!dataResp.ok) {
        // 🔥 Se 401/403, renovar sessão e tentar novamente
        if (dataResp.status === 401 || dataResp.status === 403) {
          // Faz INIT para nova sessão
          const initResp = await fetch(initUrl, {
            method: 'GET',
            headers: getFnetInitHeaders(),
            dispatcher: FNET_AGENT,
          });

          if (initResp.ok) {
            const newJsessionId = extractJSessionId(initResp);
            if (newJsessionId) {
              validJsessionId = newJsessionId;
              lastValidAt = Date.now();
              saveSession(cnpj, validJsessionId, lastValidAt);
              headers['Cookie'] = validJsessionId;
            }
          }

          try {
            initResp.body?.cancel();
          } catch {
            null;
          }

          // Retry com nova sessão
          if (attempt + 1 < retryMax) {
            await sleep(computeBackoffMs(attempt, dataResp.status, null, retryBaseMs));
            continue;
          }
        }

        await throwHttpError(dataResp, 'GET', dataUrl);
      }

      const contentType = dataResp.headers.get('content-type') || '';
      if (!contentType.toLowerCase().includes('application/json')) {
        if (attempt + 1 < retryMax && contentType.toLowerCase().includes('text/html')) {
          try {
            dataResp.body?.cancel();
          } catch {
            null;
          }
          await sleep(computeBackoffMs(attempt, 503, null, retryBaseMs));
          continue;
        }
        const snippet = await readResponseSnippet(dataResp, 800);
        const typePart = contentType ? ` content_type="${contentType}"` : '';
        const bodyPart = snippet ? ` body="${snippet}"` : '';
        throw new Error(`GET ${dataUrl} -> unexpected_response${typePart}${bodyPart}`);
      }

      // 🔥 Atualiza timestamp de uso da sessão
      saveSession(cnpj, validJsessionId, Date.now());

      return await dataResp.json() as T;
    } catch (err) {
      if (attempt + 1 < retryMax) {
        await sleep(computeBackoffMs(attempt, undefined, undefined, retryBaseMs));
        continue;
      }
      throw err;
    }
  }

  throw new Error(`FNET ${dataUrl} -> request_failed`);
}
