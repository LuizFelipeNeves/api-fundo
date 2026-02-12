import dns from 'node:dns/promises';
import net from 'node:net';
import { ProxyAgent } from 'undici';

/* ======================================================
   🔥 TOR PROXY CONFIG & MANAGEMENT
====================================================== */

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

const TOR_PROXY_ENABLED = parseEnabledFlag(process.env.TOR_PROXY_ENABLED ?? '');
const TOR_PROXY_REQUIRED = TOR_PROXY_ENABLED ? !parseEnabledFlag(process.env.TOR_PROXY_OPTIONAL ?? '') : false;
const TOR_PROXY_MODE = ((process.env.TOR_PROXY_MODE ?? 'fallback') as 'always' | 'fallback').toLowerCase() === 'always' ? 'always' : 'fallback';
const TOR_PROXY_SERVICE = (process.env.TOR_PROXY_SERVICE || 'tor').trim();
const TOR_PROXY_PORT = clampInt(parsePositiveInt(process.env.TOR_PROXY_PORT, 8118), 1, 65535);
const TOR_PROXY_REFRESH_MS = clampInt(parsePositiveInt(process.env.TOR_PROXY_REFRESH_MS, 15000), 1000, 300000);
const TOR_CONTROL_PASSWORD = (process.env.TOR_CONTROL_PASSWORD ?? '').trim();
const TOR_CONTROL_PORT = clampInt(parsePositiveInt(process.env.TOR_CONTROL_PORT, 9051), 1, 65535);
const TOR_NEWNYM_ENABLED = TOR_PROXY_ENABLED ? parseEnabledFlag(process.env.TOR_NEWNYM_ENABLED ?? '') : false;
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

  destroyAgents(previous);
}

function destroyAgents(urls: Iterable<string>): void {
  for (const url of urls) {
    const agent = TOR_PROXY_POOL.agentsByUrl.get(url);
    if (agent) {
      try {
        (agent as any)?.close?.();
        (agent as any)?.destroy?.();
      } catch {
        null;
      }
      TOR_PROXY_POOL.agentsByUrl.delete(url);
    }
  }
}

// Cleanup periódico de agents órfãos (stale)
if (typeof setInterval !== 'undefined') {
  const timer = setInterval(() => {
    if (!TOR_PROXY_ENABLED) return;
    const staleUrls: string[] = [];
    for (const url of TOR_PROXY_POOL.agentsByUrl.keys()) {
      if (!TOR_PROXY_POOL.proxyUrls.includes(url)) {
        staleUrls.push(url);
      }
    }
    if (staleUrls.length > 0) {
      destroyAgents(staleUrls);
      process.stderr.write(`[tor] destroyed ${staleUrls.length} stale tor agents\n`);
    }
  }, TOR_PROXY_REFRESH_MS);
  (timer as any).unref?.();
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

export const TOR_PROXY_ENABLED_EXPORT = TOR_PROXY_ENABLED;
export const TOR_PROXY_REQUIRED_EXPORT = TOR_PROXY_REQUIRED;
export const TOR_PROXY_MODE_EXPORT = TOR_PROXY_MODE;
export { maybeSignalNewnym };

export async function getTorProxySelection(): Promise<TorProxySelection | null> {
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

export function shouldForceTorForHost(hostname: string): boolean {
  return hostname === 'fnet.bmfbovespa.com.br' || hostname.endsWith('.bmfbovespa.com.br');
}

export const torConfig = {
  enabled: TOR_PROXY_ENABLED,
  required: TOR_PROXY_REQUIRED,
  mode: TOR_PROXY_MODE,
};

export type { TorProxySelection };
