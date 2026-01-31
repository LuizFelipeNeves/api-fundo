import { fetchCotationsToday } from '../services/client';
import { getDb, nowIso, sha256 } from '../db';
import * as repo from '../db/repo';
import { createJobLogger, forEachConcurrent, resolveConcurrency, shouldRunCotationsToday } from './utils';
import { syncFundCotationsToday } from '../core/sync/sync-fund-cotations-today';

export async function syncCotationsToday(): Promise<{ ran: boolean }> {
  const log = createJobLogger('sync-cotations-today');
  if (!shouldRunCotationsToday()) {
    log.skipped('outside_window');
    return { ran: false };
  }

  const db = getDb();
  const codes = repo.listFundCodes(db);
  const concurrency = resolveConcurrency({ envKey: 'COTATIONS_TODAY_CONCURRENCY', fallback: 5, max: 20 });

  const minIntervalMinRaw = Number.parseInt(process.env.COTATIONS_TODAY_MIN_INTERVAL_MIN || '5', 10);
  const minIntervalMin = Number.isFinite(minIntervalMinRaw) && minIntervalMinRaw > 0 ? Math.min(minIntervalMinRaw, 24 * 60) : 5;
  const minIntervalMs = minIntervalMin * 60 * 1000;

  log.start({ candidates: codes.length, concurrency, minIntervalMin });

  let ok = 0;
  let skipped = 0;
  let errCount = 0;
  let totalMs = 0;
  let maxMs = 0;
  await forEachConcurrent(codes, concurrency, async (code, i) => {
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
  });

  const avgMs = codes.length > 0 ? Math.round(totalMs / codes.length) : 0;
  log.end({ ok, skipped, err: errCount, avg_ms: avgMs, max_ms: maxMs, minIntervalMin });
  return { ran: true };
}
