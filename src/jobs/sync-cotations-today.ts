import { fetchCotationsToday } from '../services/client';
import { getDb, nowIso, sha256 } from '../db';
import * as repo from '../db/repo';
import { createJobLogger } from './utils';
import { syncFundCotationsToday } from '../core/sync/sync-fund-cotations-today';

let didInitialRun = false;

function shouldRunCotationsToday(): boolean {
  if (!didInitialRun) return true;
  const now = new Date();
  const minutes = now.getHours() * 60 + now.getMinutes();
  return minutes >= 10 * 60 && minutes <= 18 * 60 + 20;
}

export async function syncCotationsToday(): Promise<{ ran: boolean }> {
  const log = createJobLogger('sync-cotations-today');
  if (!shouldRunCotationsToday()) {
    log.skipped('outside_window');
    return { ran: false };
  }
  didInitialRun = true;

  const db = getDb();
  const codes = repo.listFundCodes(db);

  log.start({ candidates: codes.length });

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
