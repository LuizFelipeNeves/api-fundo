import { fetchCotationsToday } from '../services/client';
import { getDb, nowIso, sha256 } from '../db';
import * as repo from '../db/repo';
import { createJobLogger, forEachConcurrentUntil, resolveConcurrency, shouldRunCotationsToday } from './utils';
import { syncFundCotationsToday } from '../core/sync/sync-fund-cotations-today';

export async function syncCotationsToday(): Promise<{ ran: boolean }> {
  const log = createJobLogger('sync-cotations-today');
  if (!shouldRunCotationsToday()) {
    log.skipped('outside_window');
    return { ran: false };
  }

  const db = getDb();
  const batchSizeRaw = Number.parseInt(process.env.COTATIONS_TODAY_BATCH_SIZE || '100', 10);
  const batchSize = Number.isFinite(batchSizeRaw) && batchSizeRaw > 0 ? Math.min(batchSizeRaw, 5000) : 100;
  const candidatesLimit = Math.min(5000, Math.max(batchSize, batchSize * 5));
  const codes = repo.listFundCodesForCotationsTodayBatch(db, candidatesLimit);
  const concurrency = resolveConcurrency({ envKey: 'COTATIONS_TODAY_CONCURRENCY', fallback: 5, max: 20 });

  const minIntervalMinRaw = Number.parseInt(process.env.COTATIONS_TODAY_MIN_INTERVAL_MIN || '1', 10);
  const minIntervalMin = Number.isFinite(minIntervalMinRaw) && minIntervalMinRaw > 0 ? Math.min(minIntervalMinRaw, 24 * 60) : 1;
  const minIntervalMs = minIntervalMin * 60 * 1000;

  const timeBudgetMsRaw = Number.parseInt(process.env.COTATIONS_TODAY_TIME_BUDGET_MS || '55000', 10);
  const timeBudgetMs = Number.isFinite(timeBudgetMsRaw) && timeBudgetMsRaw > 1000 ? Math.min(timeBudgetMsRaw, 10 * 60 * 1000) : 55000;
  const deadlineMs = Date.now() + timeBudgetMs;

  log.start({ candidates: codes.length, concurrency, batchSize, candidatesLimit, minIntervalMin, timeBudgetMs });

  let ok = 0;
  let skipped = 0;
  let errCount = 0;
  let attempts = 0;
  let totalMs = 0;
  let maxMs = 0;
  await forEachConcurrentUntil(
    codes,
    concurrency,
    () => Date.now() < deadlineMs && attempts < batchSize,
    async (code, i) => {
    log.progress(i + 1, codes.length, code);
    const startedAt = Date.now();
    let status: 'ok' | 'err' | 'skipped' = 'ok';
    try {
      const state = repo.getFundCotationsTodayState(db, code);
      const lastAt = state?.last_cotations_today_at ?? null;
      if (lastAt) {
        const lastMs = Date.parse(lastAt);
        if (Number.isFinite(lastMs) && startedAt - lastMs < minIntervalMs) {
          skipped++;
          status = 'skipped';
          return;
        }
      }
      if (attempts >= batchSize) {
        skipped++;
        status = 'skipped';
        return;
      }
      attempts++;
      await syncFundCotationsToday(db, code, {
        fetcher: { fetchCotationsToday },
        repo,
        clock: { nowIso, sha256 },
      });
      ok++;
    } catch (err) {
      errCount++;
      status = 'err';
      const message = err instanceof Error ? err.stack || err.message : String(err);
      process.stderr.write(`sync-cotations-today:${code}:${message.replace(/\n/g, '\\n')}\n`);
    } finally {
      const durationMs = Date.now() - startedAt;
      totalMs += durationMs;
      maxMs = Math.max(maxMs, durationMs);
      log.progressDone(i + 1, codes.length, code, { status, duration_ms: durationMs });
    }
    }
  );

  const avgMs = codes.length > 0 ? Math.round(totalMs / codes.length) : 0;
  log.end({ ok, skipped, err: errCount, attempts, avg_ms: avgMs, max_ms: maxMs, minIntervalMin, batchSize, timeBudgetMs });
  return { ran: true };
}
