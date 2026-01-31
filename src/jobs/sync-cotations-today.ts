import { fetchCotationsToday } from '../services/client';
import { getDb, nowIso, sha256 } from '../db';
import * as repo from '../db/repo';
import { pickCodesForRun } from './utils';
import { syncFundCotationsToday } from '../core/sync/sync-fund-cotations-today';

let didInitialRun = false;

function shouldRunCotationsToday(): boolean {
  if (!didInitialRun) return true;
  const now = new Date();
  const minutes = now.getHours() * 60 + now.getMinutes();
  return minutes >= 10 * 60 && minutes <= 18 * 60 + 20;
}

export async function syncCotationsToday(): Promise<{ ran: boolean }> {
  if (!shouldRunCotationsToday()) return { ran: false };
  didInitialRun = true;

  const db = getDb();
  const perRun = Number.parseInt(process.env.COTATIONS_TODAY_LIMIT || '200', 10);
  const limit = Number.isFinite(perRun) && perRun > 0 ? Math.min(perRun, 2000) : 200;

  const allCodes = repo.listFundCodes(db);
  const codes = pickCodesForRun(allCodes, limit);

  for (const code of codes) {
    try {
      await syncFundCotationsToday(db, code, {
        fetcher: { fetchCotationsToday },
        repo,
        clock: { nowIso, sha256 },
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      process.stderr.write(`sync-cotations-today:${code}:${message}\n`);
      continue;
    }
  }

  return { ran: true };
}
