import { fetchFIIList, fetchFIIDetails } from '../services/client';
import { getDb } from '../db';
import * as repo from '../db/repo';
import { createJobLogger } from './utils';
import { syncFundDetails } from '../core/sync/sync-fund-details';

export async function syncFundsList(): Promise<{ ran: boolean }> {
  const log = createJobLogger('sync-funds-list');
  const db = getDb();
  const list = await fetchFIIList();
  repo.upsertFundList(db, list);

  const codes = repo.listFundCodesMissingDetails(db);

  log.start({ total: list.total });

  let ok = 0;
  let errCount = 0;
  for (let i = 0; i < codes.length; i++) {
    const code = codes[i];
    log.progress(i + 1, codes.length, code);
    try {
      await syncFundDetails(db, code, { fetcher: { fetchFIIDetails }, repo });
      ok++;
    } catch (err) {
      errCount++;
      const message = err instanceof Error ? err.message : String(err);
      process.stderr.write(`sync-funds-list:seed-details:${code}:${message}\n`);
      continue;
    }
  }

  log.end({ ok, err: errCount });
  return { ran: true };
}
