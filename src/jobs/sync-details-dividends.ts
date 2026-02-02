import { fetchDividends, fetchFIIDetails } from '../services/client';
import { getDb } from '../db';
import * as repo from '../db/repo';
import { syncFundDetailsAndDividends } from '../core/sync/sync-fund-details';
import { createJobLogger, forEachConcurrentUntil, resolveConcurrency, shouldRunCotationsToday } from './utils';

export async function syncDetailsDividends(): Promise<{ ran: boolean }> {
  const log = createJobLogger('sync-details-dividends');
  if (!shouldRunCotationsToday()) {
    log.skipped('outside_window');
    return { ran: false };
  }

  const db = getDb();

  const batchSizeRaw = Number.parseInt(process.env.DETAILS_DIVIDENDS_BATCH_SIZE || '50', 10);
  const batchSize = Number.isFinite(batchSizeRaw) && batchSizeRaw > 0 ? Math.min(batchSizeRaw, 5000) : 50;
  const candidatesLimit = Math.min(5000, Math.max(batchSize, batchSize * 5));
  const codes = repo.listFundCodesForDetailsSyncBatch(db, candidatesLimit);
  const concurrency = resolveConcurrency({ envKey: 'DETAILS_DIVIDENDS_CONCURRENCY', fallback: 5, max: 20 });

  const minIntervalMinRaw = Number.parseInt(process.env.DETAILS_DIVIDENDS_MIN_INTERVAL_MIN || '1', 10);
  const minIntervalMin = Number.isFinite(minIntervalMinRaw) && minIntervalMinRaw > 0 ? Math.min(minIntervalMinRaw, 24 * 60) : 1;
  const minIntervalMs = minIntervalMin * 60 * 1000;

  const timeBudgetMsRaw = Number.parseInt(process.env.DETAILS_DIVIDENDS_TIME_BUDGET_MS || '55000', 10);
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
        const state = repo.getFundDetailsSyncState(db, code);
        const lastAt = state?.last_details_sync_at ?? null;
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
        await syncFundDetailsAndDividends(db, code, {
          fetcher: { fetchFIIDetails, fetchDividends },
          repo,
        });
        ok++;
      } catch (err) {
        errCount++;
        status = 'err';
        const message = err instanceof Error ? err.stack || err.message : String(err);
        process.stderr.write(`sync-details-dividends:${code}:${message.replace(/\n/g, '\\n')}\n`);
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

