import { fetchDocuments, fetchFIIDetails, fetchDividends } from '../services/client';
import { getDb } from '../db';
import * as repo from '../db/repo';
import { createJobLogger, forEachConcurrent, resolveConcurrency } from './utils';
import { syncFundDocuments } from '../core/sync/sync-fund-documents';
import { syncFundDetailsAndDividends } from '../core/sync/sync-fund-details';

export async function syncDocuments(): Promise<{ ran: boolean }> {
  const log = createJobLogger('sync-documents');
  const db = getDb();
  const codes = repo.listFundCodesWithCnpj(db);
  const concurrency = resolveConcurrency({ envKey: 'DOCUMENTS_CONCURRENCY', fallback: 3, max: 10 });

  const dividendsTotal = repo.getDividendsTotalCount(db);
  if (dividendsTotal === 0) {
    const logDividends = createJobLogger('sync-dividends');
    const divConcurrency = resolveConcurrency({ envKey: 'DIVIDENDS_CONCURRENCY', fallback: 3, max: 10 });
    logDividends.start({ candidates: codes.length, concurrency: divConcurrency });

    let okDiv = 0;
    let errDiv = 0;
    let totalMsDiv = 0;
    let maxMsDiv = 0;
    await forEachConcurrent(codes, divConcurrency, async (code, i) => {
      logDividends.progress(i + 1, codes.length, code);
      const startedAt = Date.now();
      let status: 'ok' | 'err' = 'ok';
      try {
        await syncFundDetailsAndDividends(db, code, { fetcher: { fetchFIIDetails, fetchDividends }, repo });
        okDiv++;
      } catch (err) {
        errDiv++;
        status = 'err';
        const message = err instanceof Error ? err.stack || err.message : String(err);
        process.stderr.write(`sync-dividends:${code}:${message.replace(/\n/g, '\\n')}\n`);
      } finally {
        const durationMs = Date.now() - startedAt;
        totalMsDiv += durationMs;
        maxMsDiv = Math.max(maxMsDiv, durationMs);
        logDividends.progressDone(i + 1, codes.length, code, { status, duration_ms: durationMs });
      }
    });

    const avgMsDiv = codes.length > 0 ? Math.round(totalMsDiv / codes.length) : 0;
    logDividends.end({ ok: okDiv, err: errDiv, avg_ms: avgMsDiv, max_ms: maxMsDiv });
    return { ran: true };
  }

  log.start({ candidates: codes.length, concurrency });

  let ok = 0;
  let errCount = 0;
  let totalMs = 0;
  let maxMs = 0;
  await forEachConcurrent(codes, concurrency, async (code, i) => {
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
    } finally {
      const durationMs = Date.now() - startedAt;
      totalMs += durationMs;
      maxMs = Math.max(maxMs, durationMs);
      log.progressDone(i + 1, codes.length, code, { status, duration_ms: durationMs });
    }
  });

  const avgMs = codes.length > 0 ? Math.round(totalMs / codes.length) : 0;
  log.end({ ok, err: errCount, avg_ms: avgMs, max_ms: maxMs });
  return { ran: true };
}
