import { getDefaultHeaders } from '../config';
import dns from 'node:dns/promises';
import net from 'node:net';
import { ProxyAgent, type Dispatcher } from 'undici';

const DEFAULT_TIMEOUT = 25000; // 25 segundos
const DEFAULT_RETRY_MAX = 4;
const DEFAULT_RETRY_BASE_MS = 600;
const DEFAULT_HOST_CONCURRENCY = 3;
const DEFAULT_HOST_MIN_TIME_MS = 150;
const DEFAULT_OTHER_HOST_CONCURRENCY = 6;
const DEFAULT_OTHER_HOST_MIN_TIME_MS = 0;
const DEFAULT_FNET_HOST_CONCURRENCY = 2;
const DEFAULT_FNET_HOST_MIN_TIME_MS = 150;

interface RequestOptions {
  timeout?: number;
  headers?: Record<string, string>;
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

function parseEnabledFlag(raw: string | undefined): boolean {
  const value = (raw ?? '').trim().toLowerCase();
  return value === '1' || value === 'true' || value === 'yes' || value === 'on';
}

const HTTP_RETRY_MAX = clampInt(parsePositiveInt(process.env.HTTP_RETRY_MAX, DEFAULT_RETRY_MAX), 1, 20);
const HTTP_RETRY_BASE_MS = clampInt(parsePositiveInt(process.env.HTTP_RETRY_BASE_MS, DEFAULT_RETRY_BASE_MS), 50, 60000);

const TOR_PROXY_ENABLED = parseEnabledFlag(process.env.TOR_PROXY_ENABLED);
const TOR_PROXY_REQUIRED = TOR_PROXY_ENABLED ? !parseEnabledFlag(process.env.TOR_PROXY_OPTIONAL) : false;
const TOR_PROXY_MODE = (() => {
  const raw = (process.env.TOR_PROXY_MODE ?? 'fallback').trim().toLowerCase();
  return raw === 'always' ? 'always' : 'fallback';
})();
const TOR_PROXY_SERVICE = (process.env.TOR_PROXY_SERVICE || 'tor').trim();
const TOR_PROXY_PORT = clampInt(parsePositiveInt(process.env.TOR_PROXY_PORT, 8118), 1, 65535);
const TOR_PROXY_REFRESH_MS = clampInt(parsePositiveInt(process.env.TOR_PROXY_REFRESH_MS, 15000), 1000, 300000);
const TOR_CONTROL_PASSWORD = (process.env.TOR_CONTROL_PASSWORD ?? '').trim();
const TOR_CONTROL_PORT = clampInt(parsePositiveInt(process.env.TOR_CONTROL_PORT, 9051), 1, 65535);
const TOR_NEWNYM_ENABLED = TOR_PROXY_ENABLED ? parseEnabledFlag(process.env.TOR_NEWNYM_ENABLED) : false;
const TOR_NEWNYM_EVERY_REQUESTS = clampInt(parsePositiveInt(process.env.TOR_NEWNYM_EVERY_REQUESTS, 25), 1, 10000);
const TOR_NEWNYM_MIN_INTERVAL_MS = clampInt(parsePositiveInt(process.env.TOR_NEWNYM_MIN_INTERVAL_MS, 10000), 0, 300000);
const TOR_NEWNYM_TIMEOUT_MS = clampInt(parsePositiveInt(process.env.TOR_NEWNYM_TIMEOUT_MS, 1500), 250, 10000);

type TorProxyPoolState = {
  proxyUrls: string[];
  agentsByUrl: Map<string, ProxyAgent>;
  nextIndex: number;
  lastRefreshAt: number;
  refreshInFlight: Promise<void> | null;
};

const TOR_PROXY_POOL: TorProxyPoolState = {
  proxyUrls: [],
  agentsByUrl: new Map(),
  nextIndex: 0,
  lastRefreshAt: 0,
  refreshInFlight: null,
};

async function refreshTorProxyPool(): Promise<void> {
  if (!TOR_PROXY_ENABLED) return;

  const results = await dns.lookup(TOR_PROXY_SERVICE, { all: true, family: 4 });
  const proxyUrls = Array.from(new Set(results.map((r) => `http://${r.address}:${TOR_PROXY_PORT}`)));
  if (proxyUrls.length === 0) return;

  const previous = new Set(TOR_PROXY_POOL.proxyUrls);
  TOR_PROXY_POOL.proxyUrls = proxyUrls;
  TOR_PROXY_POOL.lastRefreshAt = Date.now();

  for (const url of proxyUrls) {
    if (!TOR_PROXY_POOL.agentsByUrl.has(url)) {
      TOR_PROXY_POOL.agentsByUrl.set(url, new ProxyAgent(url));
    }
    previous.delete(url);
  }

  for (const removed of previous) {
    const agent = TOR_PROXY_POOL.agentsByUrl.get(removed);
    try {
      (agent as any)?.close?.();
      (agent as any)?.destroy?.();
    } catch {
      null;
    }
    TOR_PROXY_POOL.agentsByUrl.delete(removed);
  }
}

type TorProxySelection = { proxyUrl: string; dispatcher: ProxyAgent };

type TorNewnymState = {
  requestCount: number;
  lastNewnymAt: number;
  inFlight: Promise<void> | null;
};

const TOR_NEWNYM_STATE = new Map<string, TorNewnymState>();

function getNewnymState(proxyUrl: string): TorNewnymState {
  const existing = TOR_NEWNYM_STATE.get(proxyUrl);
  if (existing) return existing;
  const created: TorNewnymState = { requestCount: 0, lastNewnymAt: 0, inFlight: null };
  TOR_NEWNYM_STATE.set(proxyUrl, created);
  return created;
}

function signalTorNewnym(host: string): Promise<void> {
  return new Promise((resolve, reject) => {
    if (!TOR_CONTROL_PASSWORD) {
      reject(new Error('TOR_CONTROL_PASSWORD_MISSING'));
      return;
    }

    const socket = new net.Socket();
    let settled = false;
    let output = '';

    const timeout = setTimeout(() => {
      if (settled) return;
      settled = true;
      try {
        socket.destroy();
      } catch {
        null;
      }
      reject(new Error('TOR_CONTROL_TIMEOUT'));
    }, TOR_NEWNYM_TIMEOUT_MS);

    const finish = (err?: unknown) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      try {
        socket.destroy();
      } catch {
        null;
      }
      if (err) reject(err);
      else resolve();
    };

    socket.on('error', (err) => finish(err));
    socket.on('data', (chunk) => {
      output += chunk.toString('utf8');
    });
    socket.on('close', () => {
      const authFailed = output.includes('515');
      const ok = output.includes('250 OK');
      if (!ok || authFailed) {
        finish(new Error('TOR_CONTROL_NEWNYM_FAILED'));
        return;
      }
      finish();
    });

    socket.connect(TOR_CONTROL_PORT, host, () => {
      socket.write(`AUTHENTICATE "${TOR_CONTROL_PASSWORD}"\r\nSIGNAL NEWNYM\r\nQUIT\r\n`);
    });
  });
}

async function maybeSignalNewnym(proxyUrl: string): Promise<void> {
  if (!TOR_NEWNYM_ENABLED) return;
  if (!TOR_CONTROL_PASSWORD) return;

  const state = getNewnymState(proxyUrl);
  state.requestCount++;

  if (state.requestCount < TOR_NEWNYM_EVERY_REQUESTS) return;

  const now = Date.now();
  if (TOR_NEWNYM_MIN_INTERVAL_MS > 0 && now - state.lastNewnymAt < TOR_NEWNYM_MIN_INTERVAL_MS) return;
  if (state.inFlight) return;

  const host = new URL(proxyUrl).hostname;
  state.inFlight = signalTorNewnym(host)
    .then(() => {
      state.lastNewnymAt = Date.now();
      state.requestCount = 0;
    })
    .finally(() => {
      state.inFlight = null;
    });

  await state.inFlight;
}

async function getTorProxySelection(): Promise<TorProxySelection | null> {
  if (!TOR_PROXY_ENABLED) return null;

  const now = Date.now();
  const needsRefresh =
    TOR_PROXY_POOL.proxyUrls.length === 0 || now - TOR_PROXY_POOL.lastRefreshAt >= TOR_PROXY_REFRESH_MS;

  if (needsRefresh) {
    if (!TOR_PROXY_POOL.refreshInFlight) {
      TOR_PROXY_POOL.refreshInFlight = refreshTorProxyPool().finally(() => {
        TOR_PROXY_POOL.refreshInFlight = null;
      });
    }
    try {
      await TOR_PROXY_POOL.refreshInFlight;
    } catch {
      null;
    }
  }

  if (TOR_PROXY_POOL.proxyUrls.length === 0) return null;
  const idx = TOR_PROXY_POOL.nextIndex++ % TOR_PROXY_POOL.proxyUrls.length;
  const proxyUrl = TOR_PROXY_POOL.proxyUrls[idx]!;
  const dispatcher = TOR_PROXY_POOL.agentsByUrl.get(proxyUrl) ?? null;
  return dispatcher ? { proxyUrl, dispatcher } : null;
}

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
  queue: Array<{
    run: () => Promise<Response>;
    resolve: (value: Response) => void;
    reject: (reason: unknown) => void;
  }>;
  draining: boolean;
};

const HOST_LIMITERS = new Map<string, HostLimiterState>();

function getHostLimiter(hostname: string): HostLimiterState {
  const existing = HOST_LIMITERS.get(hostname);
  if (existing) return existing;
  const created: HostLimiterState = { inflight: 0, lastStartAt: 0, queue: [], draining: false };
  HOST_LIMITERS.set(hostname, created);
  return created;
}

function getHostPolicy(hostname: string): { maxConcurrent: number; minTimeMs: number } {
  if (hostname === 'investidor10.com.br' || hostname.endsWith('.investidor10.com.br')) {
    return { maxConcurrent: I10_HOST_CONCURRENCY, minTimeMs: I10_HOST_MIN_TIME_MS };
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

function shouldForceTorForHost(hostname: string): boolean {
  return hostname === 'fnet.bmfbovespa.com.br' || hostname.endsWith('.bmfbovespa.com.br');
}

function parseRetryAfterMs(headerValue: string | null): number | null {
  if (!headerValue) return null;
  const seconds = Number.parseInt(headerValue, 10);
  if (Number.isFinite(seconds) && seconds >= 0) return seconds * 1000;
  const dateMs = Date.parse(headerValue);
  if (Number.isFinite(dateMs)) return Math.max(0, dateMs - Date.now());
  return null;
}

function computeBackoffMs(attempt: number, status?: number, retryAfterMs?: number | null): number {
  if (retryAfterMs !== null && retryAfterMs !== undefined) {
    return clampInt(retryAfterMs, 0, 300000);
  }
  const base = status === 429 || status === 403 ? HTTP_RETRY_BASE_MS * 2 : HTTP_RETRY_BASE_MS;
  const exp = Math.min(6, Math.max(0, attempt));
  const jitter = Math.floor(Math.random() * 250);
  return clampInt(base * Math.pow(2, exp) + jitter, 0, 300000);
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

  let lastError: unknown = null;
  for (let attempt = 0; attempt < HTTP_RETRY_MAX; attempt++) {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeout);
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
      const response = await withHostLimit(url, () =>
        fetch(
          url,
          {
            ...init,
            signal: controller.signal,
            ...(tor ? { dispatcher: tor.dispatcher } : {}),
          } satisfies RequestInitWithDispatcher
        )
      );

      clearTimeout(timeoutId);

      if (!response.ok && isRetryableStatus(response.status) && attempt + 1 < HTTP_RETRY_MAX) {
        const retryAfterMs = parseRetryAfterMs(response.headers.get('retry-after'));
        try {
          response.body?.cancel();
        } catch {
          null;
        }
        await sleep(computeBackoffMs(attempt, response.status, retryAfterMs));
        continue;
      }

      return response;
    } catch (error) {
      clearTimeout(timeoutId);
      if (error instanceof Error && error.name === 'AbortError') {
        throw new Error(`${method} ${url} -> timeout_after_ms=${timeout}`);
      }
      lastError = error;
      if (attempt + 1 < HTTP_RETRY_MAX) {
        await sleep(computeBackoffMs(attempt));
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
  for (let attempt = 0; attempt < HTTP_RETRY_MAX; attempt++) {
    const response = await fetchWithRetry(
      'GET',
      url,
      { headers: options.headers ?? getDefaultHeaders() },
      { timeout: options.timeout }
    );

    if (!response.ok) {
      await throwHttpError(response, 'GET', url);
    }

    const contentType = response.headers.get('content-type') || '';
    if (!contentType.toLowerCase().includes('application/json')) {
      if (attempt + 1 < HTTP_RETRY_MAX && contentType.toLowerCase().includes('text/html')) {
        try {
          response.body?.cancel();
        } catch {
          null;
        }
        await sleep(computeBackoffMs(attempt, 503));
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
  for (let attempt = 0; attempt < HTTP_RETRY_MAX; attempt++) {
    const response = await fetchWithRetry(
      'POST',
      url,
      {
        method: 'POST',
        headers: options?.headers ?? getDefaultHeaders(),
        body,
      },
      { timeout: options?.timeout }
    );

    if (!response.ok) {
      await throwHttpError(response, 'POST', url);
    }

    const contentType = response.headers.get('content-type') || '';
    if (!contentType.toLowerCase().includes('application/json')) {
      if (attempt + 1 < HTTP_RETRY_MAX && contentType.toLowerCase().includes('text/html')) {
        try {
          response.body?.cancel();
        } catch {
          null;
        }
        await sleep(computeBackoffMs(attempt, 503));
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

export async function fetchWithSession<T>(
  initUrl: string,
  dataUrl: string,
  options: RequestOptions = {}
): Promise<T> {
  const initResponse = await fetchWithRetry(
    'GET',
    initUrl,
    {
      method: 'GET',
      headers: {
        ...getDefaultHeaders(),
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      },
    },
    options
  );

  if (!initResponse.ok) {
    await throwHttpError(initResponse, 'GET', initUrl);
  }

  const cookies: string[] = [];
  const setCookies = (initResponse.headers as any).getSetCookie?.() as string[] | undefined;
  if (setCookies?.length) {
    for (const cookie of setCookies) {
      const cookieNameValue = cookie.split(';')[0]?.trim();
      if (cookieNameValue) cookies.push(cookieNameValue);
    }
  } else {
    const setCookie = initResponse.headers.get('set-cookie');
    if (setCookie) cookies.push(...parseCookieNameValuesFromHeader(setCookie));
  }
  try {
    initResponse.body?.cancel();
  } catch {
    null;
  }

  const headers: Record<string, string> = {
    ...getDefaultHeaders(),
    'accept': 'application/json, text/javascript, */*; q=0.01',
    'x-requested-with': 'XMLHttpRequest',
  };

  if (cookies.length > 0) {
    headers['cookie'] = cookies.join('; ');
  }

  for (let attempt = 0; attempt < HTTP_RETRY_MAX; attempt++) {
    const dataResponse = await fetchWithRetry('GET', dataUrl, { method: 'GET', headers }, options);

    if (!dataResponse.ok) {
      await throwHttpError(dataResponse, 'GET', dataUrl);
    }

    const contentType = dataResponse.headers.get('content-type') || '';
    if (!contentType.toLowerCase().includes('application/json')) {
      if (attempt + 1 < HTTP_RETRY_MAX && contentType.toLowerCase().includes('text/html')) {
        try {
          dataResponse.body?.cancel();
        } catch {
          null;
        }
        await sleep(computeBackoffMs(attempt, 503));
        continue;
      }
      const snippet = await readResponseSnippet(dataResponse, 800);
      const typePart = contentType ? ` content_type="${contentType}"` : '';
      const bodyPart = snippet ? ` body="${snippet}"` : '';
      throw new Error(`GET ${dataUrl} -> unexpected_response${typePart}${bodyPart}`);
    }

    try {
      return (await dataResponse.json()) as T;
    } catch {
      const snippet = await readResponseSnippet(dataResponse, 800);
      const typePart = contentType ? ` content_type="${contentType}"` : '';
      const bodyPart = snippet ? ` body="${snippet}"` : '';
      throw new Error(`GET ${dataUrl} -> invalid_json${typePart}${bodyPart}`);
    }
  }

  throw new Error(`GET ${dataUrl} -> request_failed`);
}
