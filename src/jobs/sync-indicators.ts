import { fetchFIIIndicators, fetchFIICotations } from '../services/client';
import { getDb, nowIso, sha256 } from '../db';
import * as repo from '../db/repo';
import { pickCodesForRun } from './utils';
import { syncFundIndicators } from '../core/sync/sync-fund-indicators';

export async function syncIndicators(): Promise<{ ran: boolean }> {
  const db = getDb();
  const perRun = Number.parseInt(process.env.INDICATORS_LIMIT || '200', 10);
  const limit = Number.isFinite(perRun) && perRun > 0 ? Math.min(perRun, 2000) : 200;
  const allCodes = repo.listFundCodesWithId(db);
  const codes = pickCodesForRun(allCodes, limit);

  const enableHistorical = (process.env.ENABLE_HISTORICAL_BACKFILL || 'true').toLowerCase() !== 'false';
  const days = Number.parseInt(process.env.HISTORICAL_COTATIONS_DAYS || '365', 10);
  const historicalDays = Number.isFinite(days) && days > 0 ? Math.min(days, 1825) : 365;

  for (const code of codes) {
    try {
      await syncFundIndicators(db, code, {
        fetcher: { fetchFIIIndicators, fetchFIICotations },
        repo,
        clock: { nowIso, sha256 },
        options: { enableHistoricalBackfill: enableHistorical, historicalDays },
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      process.stderr.write(`sync-indicators:${code}:${message}\n`);
      continue;
    }
  }

  return { ran: true };
}
