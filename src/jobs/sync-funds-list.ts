import { fetchFIIList, fetchFIIDetails } from '../services/client';
import { getDb } from '../db';
import * as repo from '../db/repo';
import { createJobLogger, shouldRunCotationsToday } from './utils';
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

  const codes = repo.listFundCodesMissingDetails(db);

  log.start({ total: list.total });

  let ok = 0;
  let errCount = 0;
  let totalMs = 0;
  let maxMs = 0;
  for (let i = 0; i < codes.length; i++) {
    const code = codes[i];
    log.progress(i + 1, codes.length, code);
    const startedAt = Date.now();
    let status: 'ok' | 'err' = 'ok';
    try {
      await syncFundDetails(db, code, { fetcher: { fetchFIIDetails }, repo });
      ok++;
    } catch (err) {
      errCount++;
      status = 'err';
      const message = err instanceof Error ? err.stack || err.message : String(err);
      process.stderr.write(`sync-funds-list:seed-details:${code}:${message.replace(/\n/g, '\\n')}\n`);
      continue;
    } finally {
      const durationMs = Date.now() - startedAt;
      totalMs += durationMs;
      maxMs = Math.max(maxMs, durationMs);
      log.progressDone(i + 1, codes.length, code, { status, duration_ms: durationMs });
    }
  }

  const avgMs = codes.length > 0 ? Math.round(totalMs / codes.length) : 0;
  log.end({ ok, err: errCount, avg_ms: avgMs, max_ms: maxMs });
  return { ran: true };
}
