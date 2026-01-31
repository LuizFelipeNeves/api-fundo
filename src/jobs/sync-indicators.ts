import { fetchFIIIndicators, fetchFIICotations } from '../services/client';
import { getDb, nowIso, sha256 } from '../db';
import * as repo from '../db/repo';
import { createJobLogger } from './utils';
import { syncFundIndicators } from '../core/sync/sync-fund-indicators';

export async function syncIndicators(): Promise<{ ran: boolean }> {
  const log = createJobLogger('sync-indicators');
  const db = getDb();
  const codes = repo.listFundCodesWithId(db);

  const enableHistorical = (process.env.ENABLE_HISTORICAL_BACKFILL || 'true').toLowerCase() !== 'false';
  const days = Number.parseInt(process.env.HISTORICAL_COTATIONS_DAYS || '365', 10);
  const historicalDays = Number.isFinite(days) && days > 0 ? Math.min(days, 1825) : 365;

  log.start({ candidates: codes.length, historical: enableHistorical, historicalDays });

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
      await syncFundIndicators(db, code, {
        fetcher: { fetchFIIIndicators, fetchFIICotations },
        repo,
        clock: { nowIso, sha256 },
        options: { enableHistoricalBackfill: enableHistorical, historicalDays },
      });
      ok++;
    } catch (err) {
      errCount++;
      status = 'err';
      const message = err instanceof Error ? err.stack || err.message : String(err);
      process.stderr.write(`sync-indicators:${code}:${message.replace(/\n/g, '\\n')}\n`);
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
