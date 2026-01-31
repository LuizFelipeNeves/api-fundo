import { fetchFIIList, fetchFIIDetails } from '../services/client';
import { getDb } from '../db';
import * as repo from '../db/repo';
import { pickCodesForRun } from './utils';
import { syncFundDetails } from '../core/sync/sync-fund-details';

export async function syncFundsList(): Promise<{ ran: boolean }> {
  const db = getDb();
  const list = await fetchFIIList();
  repo.upsertFundList(db, list);

  const seedLimit = Number.parseInt(process.env.DETAILS_SEED_LIMIT || '10', 10);
  const limit = Number.isFinite(seedLimit) && seedLimit > 0 ? Math.min(seedLimit, 200) : 10;
  const allCodes = repo.listFundCodesMissingDetails(db);
  const codes = pickCodesForRun(allCodes, limit);

  for (const code of codes) {
    try {
      await syncFundDetails(db, code, { fetcher: { fetchFIIDetails }, repo });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      process.stderr.write(`sync-funds-list:seed-details:${code}:${message}\n`);
      continue;
    }
  }

  return { ran: true };
}
