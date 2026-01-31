import { fetchDocuments, fetchFIIDetails, fetchDividends } from '../services/client';
import { getDb } from '../db';
import * as repo from '../db/repo';
import { pickCodesForRun } from './utils';
import { syncFundDocuments } from '../core/sync/sync-fund-documents';

export async function syncDocuments(): Promise<{ ran: boolean }> {
  const db = getDb();
  const perRun = Number.parseInt(process.env.DOCUMENTS_LIMIT || '100', 10);
  const limit = Number.isFinite(perRun) && perRun > 0 ? Math.min(perRun, 1000) : 100;
  const allCodes = repo.listFundCodesWithCnpj(db);
  const codes = pickCodesForRun(allCodes, limit);

  for (const code of codes) {
    try {
      await syncFundDocuments(db, code, {
        fetcher: { fetchDocuments, fetchFIIDetails, fetchDividends },
        repo,
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      process.stderr.write(`sync-documents:${code}:${message}\n`);
      continue;
    }
  }

  return { ran: true };
}
