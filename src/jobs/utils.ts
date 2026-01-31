function formatMs(ms: number): string {
  if (!Number.isFinite(ms) || ms < 0) return '0ms';
  if (ms < 1000) return `${Math.round(ms)}ms`;
  const sec = ms / 1000;
  if (sec < 60) return `${sec.toFixed(1)}s`;
  const min = Math.floor(sec / 60);
  const rem = sec - min * 60;
  return `${min}m${rem.toFixed(0)}s`;
}

export function createJobLogger(jobName: string, opts?: { every?: number }) {
  const every = opts?.every && opts.every > 0 ? Math.floor(opts.every) : 25;
  const startedAt = Date.now();

  function log(line: string) {
    process.stdout.write(line.endsWith('\n') ? line : `${line}\n`);
  }

  function shouldLogProgress(i: number, total: number) {
    if (total <= 0) return false;
    return i === 1 || i === total || i % every === 0;
  }

  function start(meta?: Record<string, any>) {
    const metaStr = meta ? Object.entries(meta).map(([k, v]) => `${k}=${String(v)}`).join(' ') : '';
    log(`[${jobName}] start${metaStr ? ` ${metaStr}` : ''}`);
  }

  function skipped(reason: string) {
    log(`[${jobName}] skipped reason=${reason}`);
  }

  function progress(i: number, total: number, code?: string) {
    if (!shouldLogProgress(i, total)) return;
    log(`[${jobName}] ${i}/${total}${code ? ` code=${code}` : ''}`);
  }

  function progressDone(i: number, total: number, code: string, meta?: Record<string, any>) {
    if (!shouldLogProgress(i, total)) return;
    const metaStr = meta ? Object.entries(meta).map(([k, v]) => `${k}=${String(v)}`).join(' ') : '';
    log(`[${jobName}] done ${i}/${total} code=${code}${metaStr ? ` ${metaStr}` : ''}`);
  }

  function end(meta?: Record<string, any>) {
    const metaStr = meta ? Object.entries(meta).map(([k, v]) => `${k}=${String(v)}`).join(' ') : '';
    log(`[${jobName}] end duration=${formatMs(Date.now() - startedAt)}${metaStr ? ` ${metaStr}` : ''}`);
  }

  return { start, skipped, progress, progressDone, shouldLogProgress, end };
}

export function resolveConcurrency(opts?: { envKey?: string; fallback?: number; max?: number }): number {
  const envKey = opts?.envKey ?? 'JOB_CONCURRENCY';
  const fallback = opts?.fallback ?? 5;
  const max = opts?.max ?? 20;

  const raw = process.env[envKey];
  const parsed = raw ? Number.parseInt(raw, 10) : NaN;
  const value = Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
  return Math.max(1, Math.min(max, Math.floor(value)));
}

export async function forEachConcurrent<T>(
  items: readonly T[],
  concurrency: number,
  fn: (item: T, index: number) => Promise<void>
): Promise<void> {
  const safeConcurrency = Math.max(1, Math.floor(concurrency));
  const total = items.length;
  if (total === 0) return;

  let nextIndex = 0;
  const workerCount = Math.min(safeConcurrency, total);
  const workers = new Array(workerCount);

  for (let w = 0; w < workerCount; w++) {
    workers[w] = (async () => {
      while (true) {
        const i = nextIndex++;
        if (i >= total) break;
        await fn(items[i], i);
      }
    })();
  }

  await Promise.all(workers);
}

export function shouldRunCotationsToday(): boolean {
  const now = new Date();
  if (!isWeekdayInSaoPaulo(now)) return false;
  const minutes = getMinutesInSaoPaulo(now);
  return minutes >= 10 * 60 && minutes <= 18 * 60 + 30;
}

export function isWeekdayInSaoPaulo(date: Date): boolean {
  const weekday = new Intl.DateTimeFormat('en-US', { timeZone: 'America/Sao_Paulo', weekday: 'short' }).format(date);
  return weekday !== 'Sat' && weekday !== 'Sun';
}

function getMinutesInSaoPaulo(date: Date): number {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/Sao_Paulo',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).formatToParts(date);

  let hour = 0;
  let minute = 0;
  for (const p of parts) {
    if (p.type === 'hour') hour = Number.parseInt(p.value, 10);
    if (p.type === 'minute') minute = Number.parseInt(p.value, 10);
  }

  if (!Number.isFinite(hour) || !Number.isFinite(minute)) return 0;
  return hour * 60 + minute;
}
