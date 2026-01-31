import { fetchDocuments, fetchFIIDetails, fetchDividends } from '../services/client';
import { getDb } from '../db';
import * as repo from '../db/repo';
import { createJobLogger } from './utils';
import { syncFundDocuments } from '../core/sync/sync-fund-documents';

export async function syncDocuments(): Promise<{ ran: boolean }> {
  const log = createJobLogger('sync-documents');
  const db = getDb();
  const codes = repo.listFundCodesWithCnpj(db);

  log.start({ candidates: codes.length });

  let ok = 0;
  let errCount = 0;
  for (let i = 0; i < codes.length; i++) {
    const code = codes[i];
    log.progress(i + 1, codes.length, code);
    try {
      await syncFundDocuments(db, code, {
        fetcher: { fetchDocuments, fetchFIIDetails, fetchDividends },
        repo,
      });
      ok++;
    } catch (err) {
      errCount++;
      const message = err instanceof Error ? err.message : String(err);
      process.stderr.write(`sync-documents:${code}:${message}\n`);
      continue;
    }
  }

  log.end({ ok, err: errCount });
  return { ran: true };
}
