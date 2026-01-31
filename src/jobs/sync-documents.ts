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
  let totalMs = 0;
  let maxMs = 0;
  for (let i = 0; i < codes.length; i++) {
    const code = codes[i];
    log.progress(i + 1, codes.length, code);
    const startedAt = Date.now();
    let status: 'ok' | 'err' = 'ok';
    try {
      await syncFundDocuments(db, code, {
        fetcher: { fetchDocuments, fetchFIIDetails, fetchDividends },
        repo,
      });
      ok++;
    } catch (err) {
      errCount++;
      status = 'err';
      const message = err instanceof Error ? err.stack || err.message : String(err);
      process.stderr.write(`sync-documents:${code}:${message.replace(/\n/g, '\\n')}\n`);
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
