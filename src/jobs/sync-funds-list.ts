import { fetchFIIList, fetchFIIDetails } from '../services/client';
import { getDb } from '../db';
import * as repo from '../db/repo';
import { createJobLogger, forEachConcurrentUntil, resolveConcurrency, shouldRunCotationsToday } from './utils';
import { syncFundDetails } from '../core/sync/sync-fund-details';

export async function syncFundsList(): Promise<{ ran: boolean }> {
  const log = createJobLogger('sync-funds-list');
  if (!shouldRunCotationsToday()) {
    log.skipped('outside_window');
    return { ran: false };
  }
  const db = getDb();
  const list = await fetchFIIList();
  repo.upsertFundList(db, list);

  const seedBatchSizeRaw = Number.parseInt(process.env.FUNDS_LIST_SEED_BATCH_SIZE || '50', 10);
  const seedBatchSize = Number.isFinite(seedBatchSizeRaw) && seedBatchSizeRaw > 0 ? Math.min(seedBatchSizeRaw, 5000) : 50;
  const candidatesLimit = Math.min(5000, Math.max(seedBatchSize, seedBatchSize * 5));
  const allMissing = repo.listFundCodesMissingDetails(db);
  const codes = allMissing.slice(0, candidatesLimit);
  const concurrency = resolveConcurrency({ envKey: 'FUNDS_LIST_SEED_CONCURRENCY', fallback: 3, max: 10 });

  const timeBudgetMsRaw = Number.parseInt(process.env.FUNDS_LIST_SEED_TIME_BUDGET_MS || '55000', 10);
  const timeBudgetMs = Number.isFinite(timeBudgetMsRaw) && timeBudgetMsRaw > 1000 ? Math.min(timeBudgetMsRaw, 10 * 60 * 1000) : 55000;
  const deadlineMs = Date.now() + timeBudgetMs;

  log.start({ total: list.total, missing_details: allMissing.length, candidates: codes.length, candidatesLimit, concurrency, seedBatchSize, timeBudgetMs });

  let ok = 0;
  let errCount = 0;
  let skipped = 0;
  let attempts = 0;
  let totalMs = 0;
  let maxMs = 0;
  await forEachConcurrentUntil(
    codes,
    concurrency,
    () => Date.now() < deadlineMs && attempts < seedBatchSize,
    async (code, i) => {
      log.progress(i + 1, codes.length, code);
      const startedAt = Date.now();
      let status: 'ok' | 'err' | 'skipped' = 'ok';
      try {
        if (attempts >= seedBatchSize) {
          skipped++;
          status = 'skipped';
          return;
        }
        attempts++;
        await syncFundDetails(db, code, { fetcher: { fetchFIIDetails }, repo });
        ok++;
      } catch (err) {
        errCount++;
        status = 'err';
        const message = err instanceof Error ? err.stack || err.message : String(err);
        process.stderr.write(`sync-funds-list:seed-details:${code}:${message.replace(/\n/g, '\\n')}\n`);
      } finally {
        const durationMs = Date.now() - startedAt;
        totalMs += durationMs;
        maxMs = Math.max(maxMs, durationMs);
        log.progressDone(i + 1, codes.length, code, { status, duration_ms: durationMs });
      }
    }
  );

  const avgMs = codes.length > 0 ? Math.round(totalMs / codes.length) : 0;
  log.end({ ok, skipped, err: errCount, attempts, avg_ms: avgMs, max_ms: maxMs, seedBatchSize, timeBudgetMs });
  return { ran: true };
}
