import { getDefaultHeaders } from '../config';

const DEFAULT_TIMEOUT = 25000; // 25 segundos

/* ======================================================
   🔥 RETRY CONFIG (FIXED DELAY)
====================================================== */

const HTTP_RETRY_MAX_DEFAULT = 5;
const HTTP_RETRY_DELAY_MS_DEFAULT = 2000;

function clampInt(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, Math.floor(value)));
}

function parsePositiveInt(raw: string | undefined, fallback: number): number {
  const parsed = raw ? Number.parseInt(raw, 10) : NaN;
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

const HTTP_RETRY_MAX = clampInt(
  parsePositiveInt(process.env.HTTP_RETRY_MAX, HTTP_RETRY_MAX_DEFAULT),
  1,
  20
);

const HTTP_RETRY_DELAY_MS = clampInt(
  parsePositiveInt(process.env.HTTP_RETRY_DELAY_MS, HTTP_RETRY_DELAY_MS_DEFAULT),
  100,
  60000
);

const HTTP_RETRY_AFTER_MAX_MS = clampInt(
  parsePositiveInt(process.env.HTTP_RETRY_AFTER_MAX_MS, 5000),
  0,
  60000
);

const HTTP_MAX_TOTAL_MS = clampInt(
  parsePositiveInt(process.env.HTTP_MAX_TOTAL_MS, 45000),
  1000,
  300000
);

/* ======================================================
   🔥 HEADERS
====================================================== */

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

function getHtmlHeaders(): Record<string, string> {
  return {
    accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    'user-agent': 'Mozilla/5.0',
    connection: 'keep-alive',
  };
}

/* ======================================================
   🔥 HELPERS
====================================================== */

function sleep(ms: number): Promise<void> {
  return new Promise(r => setTimeout(r, ms));
}

function isRetryableStatus(status: number): boolean {
  return status === 429 || status === 520 || status >= 500;
}

function parseRetryAfterMs(h: string | null): number | null {
  if (!h) return null;
  const s = Number.parseInt(h, 10);
  if (Number.isFinite(s) && s >= 0) return s * 1000;
  const d = Date.parse(h);
  return Number.isFinite(d) ? Math.max(0, d - Date.now()) : null;
}

function getFixedRetryDelay(retryAfter: number | null): number {
  if (retryAfter !== null) {
    return clampInt(retryAfter, 0, HTTP_RETRY_AFTER_MAX_MS);
  }
  return HTTP_RETRY_DELAY_MS;
}

async function throwHttpError(response: Response, method: string, url: string): Promise<never> {
  try { response.body?.cancel(); } catch {}
  throw new Error(
    `${method} ${url} -> HTTP ${response.status}${response.statusText ? ' ' + response.statusText : ''} content_type="${response.headers.get('content-type') || ''}"`
  );
}

function remainingBudgetMs(startedAt: number, maxTotalMs: number): number {
  return maxTotalMs - (Date.now() - startedAt);
}

function getRetrySleepMs(startedAt: number, maxTotalMs: number, plannedSleepMs: number): number {
  const remaining = remainingBudgetMs(startedAt, maxTotalMs);
  if (remaining <= 50) return -1;
  return Math.max(0, Math.min(plannedSleepMs, remaining - 50));
}

/* ======================================================
   🔥 CORE FETCH COM RETRY FIXO
====================================================== */

interface ReqOptions {
  timeout?: number;
  headers?: Record<string, string>;
  retryMax?: number;
}

async function fetchWithRetry(
  method: string,
  url: string,
  init: RequestInit,
  opts: ReqOptions
): Promise<Response> {

  const timeout = opts.timeout ?? DEFAULT_TIMEOUT;
  const retryMax = clampInt(opts.retryMax ?? HTTP_RETRY_MAX, 1, 50);
  const maxTotalMs = Math.max(timeout, HTTP_MAX_TOTAL_MS);
  const startedAt = Date.now();

  for (let attempt = 0; attempt < retryMax; attempt++) {
    try {
      const remaining = remainingBudgetMs(startedAt, maxTotalMs);
      if (remaining <= 50) {
        throw new Error(`timeout_budget_exhausted max_total_ms=${maxTotalMs}`);
      }

      const ctrl = new AbortController();
      const tid = setTimeout(() => ctrl.abort(), Math.min(timeout, remaining));

      try {
        const res = await fetch(url, { ...init, signal: ctrl.signal });

        if (!res.ok && isRetryableStatus(res.status) && attempt + 1 < retryMax) {
          const after = parseRetryAfterMs(res.headers.get('retry-after'));
          try { res.body?.cancel(); } catch {}

          const planned = getFixedRetryDelay(after);
          const sleepMs = getRetrySleepMs(startedAt, maxTotalMs, planned);

          if (sleepMs < 0) return res;

          await sleep(sleepMs);
          continue;
        }

        return res;

      } finally {
        clearTimeout(tid);
      }

    } catch (e) {

      if (attempt + 1 >= retryMax) {
        const errMsg = e instanceof Error && e.name === 'AbortError'
          ? `timeout_after_ms=${timeout}`
          : String(e);

        throw new Error(`${method} ${url} -> retry_failed after ${attempt + 1}/${retryMax}: ${errMsg}`);
      }

      const planned = getFixedRetryDelay(null);
      const sleepMs = getRetrySleepMs(startedAt, maxTotalMs, planned);

      if (sleepMs < 0) {
        throw new Error(`${method} ${url} -> retry_failed budget_exhausted max_total_ms=${maxTotalMs}`);
      }

      await sleep(sleepMs);
    }
  }

  throw new Error(`${method} ${url} -> failed`);
}

/* ======================================================
   🔥 REQUEST HANDLERS
====================================================== */

async function request<T>(url: string, opts: ReqOptions = {}, expectJson = true): Promise<T> {

  const res = await fetchWithRetry('GET', url, {
    headers: opts.headers ?? getDefaultHeaders()
  }, opts);

  if (!res.ok) await throwHttpError(res, 'GET', url);

  if (expectJson) {
    const ct = res.headers.get('content-type') || '';

    if (!ct.toLowerCase().includes('application/json')) {
      try { res.body?.cancel(); } catch {}
      throw new Error(`GET ${url} -> unexpected_content_type="${ct}"`);
    }

    try {
      return await res.json() as T;
    } catch {
      try { res.body?.cancel(); } catch {}
      throw new Error(`GET ${url} -> invalid_json`);
    }
  }

  return await res.text() as T;
}

async function doPost<T>(url: string, body: string, opts: ReqOptions = {}): Promise<T> {

  const res = await fetchWithRetry('POST', url, {
    method: 'POST',
    headers: opts.headers ?? getDefaultHeaders(),
    body
  }, opts);

  if (!res.ok) await throwHttpError(res, 'POST', url);

  const ct = res.headers.get('content-type') || '';

  if (!ct.toLowerCase().includes('application/json')) {
    try { res.body?.cancel(); } catch {}
    throw new Error(`POST ${url} -> unexpected_content_type="${ct}"`);
  }

  try {
    return await res.json() as T;
  } catch {
    try { res.body?.cancel(); } catch {}
    throw new Error(`POST ${url} -> invalid_json`);
  }
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

export const postForm = post;
export const getJson = get;

export const getText = (url: string, opts?: ReqOptions) => fetchText(url, opts);

export async function fetchText(url: string, opts?: ReqOptions): Promise<string> {

  const headers = opts?.headers ?? getHtmlHeaders();

  const res = await fetchWithRetry('GET', url, {
    method: 'GET',
    headers,
  }, opts ?? {});

  if (res.status === 410) {
    try { res.body?.cancel(); } catch {}
    throw new Error('FII_NOT_FOUND');
  }

  if (!res.ok) await throwHttpError(res, 'GET', url);

  return res.text();
}

/* ======================================================
   🔥 FNET FETCH
====================================================== */

export async function fetchFnetWithSession<T>(
  initUrl: string,
  dataUrl: string,
  opts: ReqOptions = {}
): Promise<T> {

  const retryMax = clampInt(opts.retryMax ?? HTTP_RETRY_MAX, 1, 50);
  const timeout = opts.timeout ?? DEFAULT_TIMEOUT;
  const maxTotalMs = Math.max(timeout, HTTP_MAX_TOTAL_MS);
  const startedAt = Date.now();

  for (let attempt = 0; attempt < retryMax; attempt++) {

    if (remainingBudgetMs(startedAt, maxTotalMs) <= 50) {
      throw new Error(`FNET retry budget exhausted max_total_ms=${maxTotalMs}`);
    }

    let jsessionId: string | null = null;

    try {
      const initRes = await fetch(initUrl, {
        method: 'GET',
        headers: getFnetInitHeaders()
      });

      if (!initRes.ok) {
        try { initRes.body?.cancel(); } catch {}

        if (attempt + 1 < retryMax) {
          await sleep(getFixedRetryDelay(null));
          continue;
        }

        await throwHttpError(initRes, 'GET', initUrl);
      }

      jsessionId = extractJSessionId(initRes);

      try { initRes.body?.cancel(); } catch {}

    } catch (e) {

      if (attempt + 1 >= retryMax) throw e;

      await sleep(getFixedRetryDelay(null));
      continue;
    }

    if (!jsessionId) {

      if (attempt + 1 >= retryMax) {
        throw new Error('FNET_INIT_NO_JSESSIONID');
      }

      await sleep(getFixedRetryDelay(null));
      continue;
    }

    const headers = getFnetHeaders(jsessionId);

    try {

      const res = await fetchWithRetry('GET', dataUrl, {
        method: 'GET',
        headers
      }, opts);

      if (!res.ok) {

        if ((res.status === 401 || res.status === 403) && attempt + 1 < retryMax) {
          await sleep(getFixedRetryDelay(null));
          continue;
        }

        await throwHttpError(res, 'GET', dataUrl);
      }

      const ct = res.headers.get('content-type') || '';

      if (!ct.toLowerCase().includes('application/json')) {

        if (ct.toLowerCase().includes('text/html') && attempt + 1 < retryMax) {
          try { res.body?.cancel(); } catch {}
          await sleep(getFixedRetryDelay(null));
          continue;
        }

        try { res.body?.cancel(); } catch {}
        throw new Error(`FNET ${dataUrl} -> content_type="${ct}"`);
      }

      try {
        return await res.json() as T;
      } catch {
        try { res.body?.cancel(); } catch {}
        throw new Error(`FNET ${dataUrl} -> invalid_json`);
      }

    } catch (e) {

      if (attempt + 1 >= retryMax) throw e;

      await sleep(getFixedRetryDelay(null));
    }
  }

  throw new Error(`FNET ${dataUrl} -> failed`);
}
