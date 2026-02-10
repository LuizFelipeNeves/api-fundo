import { getDefaultHeaders } from '../config';
import { Agent } from 'undici';
import { getTorProxySelection, maybeSignalNewnym, shouldForceTorForHost, TOR_PROXY_ENABLED_EXPORT as TOR_PROXY_ENABLED, TOR_PROXY_MODE_EXPORT as TOR_PROXY_MODE, TOR_PROXY_REQUIRED_EXPORT as TOR_PROXY_REQUIRED } from './tor';

const DEFAULT_TIMEOUT = 25000; // 25 segundos
const HTTP_RETRY_MAX_DEFAULT = 4;
const HTTP_RETRY_BASE_MS_DEFAULT = 600;

/* ======================================================
   🔥 HTTP AGENTS DEDICADOS POR HOST (keepalive)
====================================================== */

const AGENTS = {
  fnet: new Agent({ keepAliveTimeout: 120_000, keepAliveMaxTimeout: 180_000, connections: 50, pipelining: 1 }),
  investidor10: new Agent({ keepAliveTimeout: 60_000, keepAliveMaxTimeout: 90_000, connections: 20, pipelining: 2 }),
  statusinvest: new Agent({ keepAliveTimeout: 60_000, keepAliveMaxTimeout: 90_000, connections: 10, pipelining: 1 }),
  default: new Agent({ keepAliveTimeout: 30_000, keepAliveMaxTimeout: 45_000, connections: 15, pipelining: 2 }),
};

function getAgent(hostname: string): Agent {
  if (hostname === 'fnet.bmfbovespa.com.br' || hostname.endsWith('.bmfbovespa.com.br')) return AGENTS.fnet;
  if (hostname === 'investidor10.com.br' || hostname.endsWith('.investidor10.com.br')) return AGENTS.investidor10;
  if (hostname === 'statusinvest.com.br' || hostname.endsWith('.statusinvest.com.br')) return AGENTS.statusinvest;
  return AGENTS.default;
}

const FNET_SESSION_TTL_MS = 60 * 60 * 1000; // 1 hora

function extractJSessionId(resp: Response): string | null {
  const raw = resp.headers.get('set-cookie') || resp.headers.get('set-cookie2');
  const match = raw?.match(/JSESSIONID=[^;]+/i);
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
  return { 'User-Agent': 'Mozilla/5.0', Accept: '*/*', Connection: 'keep-alive' };
}

/* ======================================================
   🔥 HELPERS & CONFIG
====================================================== */

function clampInt(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, Math.floor(value)));
}

function parsePositiveInt(raw: string | undefined, fallback: number): number {
  const parsed = raw ? Number.parseInt(raw, 10) : NaN;
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

const HTTP_RETRY_MAX = clampInt(parsePositiveInt(process.env.HTTP_RETRY_MAX, HTTP_RETRY_MAX_DEFAULT), 1, 20);
const HTTP_RETRY_BASE_MS = clampInt(parsePositiveInt(process.env.HTTP_RETRY_BASE_MS, HTTP_RETRY_BASE_MS_DEFAULT), 50, 60000);

function isRetryableStatus(status: number): boolean {
  return status === 403 || status === 429 || status >= 500;
}

function computeBackoff(attempt: number, status: number | undefined, retryAfter: number | null, baseMs: number): number {
  if (retryAfter !== null) return clampInt(retryAfter, 0, 300000);
  const base = status === 429 || status === 403 ? baseMs * 2 : baseMs;
  return clampInt(base * Math.pow(2, Math.min(6, attempt)) + Math.floor(Math.random() * 250), 0, 300000);
}

function sleep(ms: number): Promise<void> { return new Promise(r => setTimeout(r, ms)); }

async function readSnippet(response: Response, maxChars = 800): Promise<string> {
  try { const t = (await response.text()).replace(/\s+/g, ' ').trim(); return t.length > maxChars ? t.slice(0, maxChars) + '…' : t; } catch { return ''; }
}

async function throwHttpError(response: Response, method: string, url: string): Promise<never> {
  const body = await readSnippet(response);
  throw new Error(`${method} ${url} -> HTTP ${response.status}${response.statusText ? ' ' + response.statusText : ''} content_type="${response.headers.get('content-type') || ''}" body="${body}"`);
}

/* ======================================================
   🔥 CORE FETCH COM TOR + LIMITS + RETRY
====================================================== */

interface ReqOptions { timeout?: number; headers?: Record<string, string>; retryMax?: number; retryBaseMs?: number; }

async function fetchWithRetry(method: string, url: string, init: RequestInit, opts: ReqOptions): Promise<Response> {
  const timeout = opts.timeout ?? DEFAULT_TIMEOUT;
  const retryMax = clampInt(opts.retryMax ?? HTTP_RETRY_MAX, 1, 50);
  const retryBase = clampInt(opts.retryBaseMs ?? HTTP_RETRY_BASE_MS, 50, 60000);
  const hostname = new URL(url).hostname;
  const agent = getAgent(hostname);

  for (let attempt = 0; attempt < retryMax; attempt++) {
    try {
      const useTor = TOR_PROXY_ENABLED && (TOR_PROXY_REQUIRED || TOR_PROXY_MODE === 'always' || attempt > 0 || shouldForceTorForHost(hostname));
      const tor = useTor ? await getTorProxySelection() : null;
      if (useTor && TOR_PROXY_REQUIRED && !tor) { await sleep(250); throw new Error('TOR_PROXY_UNAVAILABLE'); }
      if (tor) maybeSignalNewnym(tor.proxyUrl).catch(() => null);

      const ctrl = new AbortController();
      const tid = setTimeout(() => ctrl.abort(), timeout);
      try {
        const res = await fetch(url, { ...init, signal: ctrl.signal, dispatcher: tor?.dispatcher ?? agent });
        clearTimeout(tid);

        if (!res.ok && isRetryableStatus(res.status) && attempt + 1 < retryMax) {
          const after = parseRetryAfterMs(res.headers.get('retry-after'));
          try { res.body?.cancel(); } catch { null; }
          await sleep(computeBackoff(attempt, res.status, after, retryBase));
          continue;
        }
        return res;
      } finally { clearTimeout(tid); }
    } catch (e) {
      if (attempt + 1 >= retryMax) throw e;
      await sleep(computeBackoff(attempt, undefined, null, retryBase));
    }
  }
  throw new Error(`${method} ${url} -> failed`);
}

function parseRetryAfterMs(h: string | null): number | null {
  if (!h) return null;
  const s = Number.parseInt(h, 10);
  if (Number.isFinite(s) && s >= 0) return s * 1000;
  const d = Date.parse(h);
  return Number.isFinite(d) ? Math.max(0, d - Date.now()) : null;
}

/* ======================================================
   🔥 REQUEST HANDLERS (UNIFICADO)
====================================================== */

async function request<T>(url: string, opts: ReqOptions = {}, expectJson = true): Promise<T> {
  const retryMax = clampInt(opts.retryMax ?? HTTP_RETRY_MAX, 1, 50);
  const retryBase = clampInt(opts.retryBaseMs ?? HTTP_RETRY_BASE_MS, 50, 60000);

  for (let attempt = 0; attempt < retryMax; attempt++) {
    const res = await fetchWithRetry('GET', url, { headers: opts.headers ?? getDefaultHeaders() }, opts);
    if (!res.ok) await throwHttpError(res, 'GET', url);

    if (expectJson) {
      const ct = res.headers.get('content-type') || '';
      if (!ct.toLowerCase().includes('application/json')) {
        if (attempt + 1 < retryMax && ct.toLowerCase().includes('text/html')) {
          try { res.body?.cancel(); } catch { null; }
          await sleep(computeBackoff(attempt, 503, null, retryBase));
          continue;
        }
        throw new Error(`GET ${url} -> unexpected_content_type="${ct}"`);
      }
      try { return await res.json() as T; } catch { throw new Error(`GET ${url} -> invalid_json`); }
    }
    return await res.text() as T;
  }
  throw new Error(`GET ${url} -> failed`);
}

async function doPost<T>(url: string, body: string, opts: ReqOptions = {}): Promise<T> {
  const retryMax = clampInt(opts.retryMax ?? HTTP_RETRY_MAX, 1, 50);
  const retryBase = clampInt(opts.retryBaseMs ?? HTTP_RETRY_BASE_MS, 50, 60000);

  for (let attempt = 0; attempt < retryMax; attempt++) {
    const res = await fetchWithRetry('POST', url, {
      method: 'POST',
      headers: opts.headers ?? getDefaultHeaders(),
      body,
    }, opts);
    if (!res.ok) await throwHttpError(res, 'POST', url);

    const ct = res.headers.get('content-type') || '';
    if (!ct.toLowerCase().includes('application/json')) {
      if (attempt + 1 < retryMax && ct.toLowerCase().includes('text/html')) {
        try { res.body?.cancel(); } catch { null; }
        await sleep(computeBackoff(attempt, 503, null, retryBase));
        continue;
      }
      throw new Error(`POST ${url} -> unexpected_content_type="${ct}"`);
    }
    try { return await res.json() as T; } catch { throw new Error(`POST ${url} -> invalid_json`); }
  }
  throw new Error(`POST ${url} -> failed`);
}

/* ======================================================
   🔥 EXPORTS
====================================================== */

export async function get<T>(url: string, opts?: ReqOptions): Promise<T> {
  return request<T>(url, opts ?? {});
}

export async function post<T>(url: string, body: string, opts?: ReqOptions): Promise<T> {
  return doPost<T>(url, body, opts ?? {});
}

export async function fetchText(url: string, opts?: ReqOptions): Promise<string> {
  const res = await fetchWithRetry('GET', url, {
    method: 'GET',
    headers: { ...getDefaultHeaders(), accept: 'text/html,*/*' },
  }, opts ?? {});

  if (res.status === 410) throw new Error('FII_NOT_FOUND');
  if (!res.ok) await throwHttpError(res, 'GET', url);
  return res.text();
}

/* ======================================================
   🔥 FNET COM SESSION CACHE
====================================================== */

type FnetSessionCallbacks = {
  getSession: (cnpj: string) => { jsessionId: string | null; lastValidAt: number | null };
  saveSession: (cnpj: string, jsessionId: string, lastValidAt: number) => void;
};

export async function fetchFnetWithSession<T>(
  initUrl: string,
  dataUrl: string,
  cnpj: string,
  opts: ReqOptions = {},
  callbacks?: FnetSessionCallbacks
): Promise<T> {
  if (!callbacks?.getSession || !callbacks?.saveSession) throw new Error('FNET_SESSION_CALLBACKS_REQUIRED');

  const { getSession, saveSession } = callbacks;
  const now = Date.now();
  const session = getSession(cnpj);
  let jsessionId: string | null = session.jsessionId;
  let lastValidAt = session.lastValidAt;

  const canReuse = jsessionId !== null && lastValidAt !== null && now - lastValidAt < FNET_SESSION_TTL_MS;

  // STEP 1: INIT para obter JSESSIONID se necessário
  if (!canReuse) {
    const initRes = await fetch(initUrl, { method: 'GET', headers: getFnetInitHeaders(), dispatcher: AGENTS.fnet });
    if (!initRes.ok) throwHttpError(initRes, 'GET', initUrl);
    const newId = extractJSessionId(initRes);
    if (newId) { jsessionId = newId; lastValidAt = now; saveSession(cnpj, jsessionId, lastValidAt); }
    else throw new Error('FNET_INIT_NO_JSESSIONID');
    try { initRes.body?.cancel(); } catch { null; }
  }

  const validId = jsessionId!;
  const headers = getFnetHeaders(validId);
  const retryMax = clampInt(opts.retryMax ?? HTTP_RETRY_MAX, 1, 50);
  const retryBase = clampInt(opts.retryBaseMs ?? HTTP_RETRY_BASE_MS, 50, 60000);

  for (let attempt = 0; attempt < retryMax; attempt++) {
    try {
      const res = await fetchWithRetry('GET', dataUrl, { method: 'GET', headers }, opts);

      if (!res.ok) {
        if (res.status === 401 || res.status === 403) {
          // Renovar sessão
          const initRes = await fetch(initUrl, { method: 'GET', headers: getFnetInitHeaders(), dispatcher: AGENTS.fnet });
          if (initRes.ok) {
            const newId = extractJSessionId(initRes);
            if (newId) { saveSession(cnpj, newId, Date.now()); headers['Cookie'] = validId; }
          }
          try { initRes.body?.cancel(); } catch { null; }

          if (attempt + 1 < retryMax) { await sleep(computeBackoff(attempt, res.status, null, retryBase)); continue; }
        }
        throwHttpError(res, 'GET', dataUrl);
      }

      const ct = res.headers.get('content-type') || '';
      if (!ct.toLowerCase().includes('application/json')) {
        if (attempt + 1 < retryMax && ct.toLowerCase().includes('text/html')) {
          try { res.body?.cancel(); } catch { null; }
          await sleep(computeBackoff(attempt, 503, null, retryBase));
          continue;
        }
        throw new Error(`FNET ${dataUrl} -> content_type="${ct}"`);
      }

      saveSession(cnpj, validId, Date.now());
      return await res.json() as T;
    } catch (e) {
      if (attempt + 1 >= retryMax) throw e;
      await sleep(computeBackoff(attempt, undefined, null, retryBase));
    }
  }
  throw new Error(`FNET ${dataUrl} -> failed`);
}
