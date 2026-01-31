import { fetchDocuments, fetchFIIDetails, fetchDividends } from '../services/client';
import { getDb, nowIso } from '../db';
import * as repo from '../db/repo';
import { createJobLogger, forEachConcurrent, forEachConcurrentUntil, resolveConcurrency } from './utils';
import { syncFundDocuments } from '../core/sync/sync-fund-documents';
import { syncFundDetailsAndDividends } from '../core/sync/sync-fund-details';
import { createTelegramService } from '../telegram-bot/telegram-api';
import { listTelegramChatIdsByFundCode } from '../telegram-bot/storage';

export async function syncDocuments(): Promise<{ ran: boolean }> {
  const log = createJobLogger('sync-documents');
  const db = getDb();
  const telegramToken = (process.env.TELEGRAM_BOT_TOKEN || '').trim();
  const telegram = telegramToken ? createTelegramService(telegramToken) : null;
  const batchSizeRaw = Number.parseInt(process.env.DOCUMENTS_BATCH_SIZE || '100', 10);
  const batchSize = Number.isFinite(batchSizeRaw) && batchSizeRaw > 0 ? Math.min(batchSizeRaw, 5000) : 100;
  const candidatesLimit = Math.min(5000, Math.max(batchSize, batchSize * 5));
  const codes = repo.listFundCodesForDocumentsBatch(db, candidatesLimit);
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

  const timeBudgetMsRaw = Number.parseInt(process.env.DOCUMENTS_TIME_BUDGET_MS || '55000', 10);
  const timeBudgetMs = Number.isFinite(timeBudgetMsRaw) && timeBudgetMsRaw > 1000 ? Math.min(timeBudgetMsRaw, 10 * 60 * 1000) : 55000;
  const deadlineMs = Date.now() + timeBudgetMs;

  log.start({ candidates: codes.length, concurrency, batchSize, candidatesLimit, timeBudgetMs });

  const minIntervalMinRaw = Number.parseInt(process.env.DOCUMENTS_MIN_INTERVAL_MIN || '360', 10);
  const minIntervalMin = Number.isFinite(minIntervalMinRaw) && minIntervalMinRaw > 0 ? Math.min(minIntervalMinRaw, 30 * 24 * 60) : 360;
  const minIntervalMs = minIntervalMin * 60 * 1000;

  let ok = 0;
  let skipped = 0;
  let errCount = 0;
  let attempts = 0;
  let totalMs = 0;
  let maxMs = 0;
  await forEachConcurrentUntil(
    codes,
    concurrency,
    () => Date.now() < deadlineMs && attempts < batchSize,
    async (code, i) => {
    log.progress(i + 1, codes.length, code);
    const startedAt = Date.now();
    let status: 'ok' | 'err' | 'skipped' = 'ok';
    try {
      const state = repo.getFundState(db, code);
      const lastMaxId = state?.last_documents_max_id ?? null;
      const docsState = repo.getFundDocumentsState(db, code);
      const lastAt = docsState?.last_documents_at ?? null;
      if (lastMaxId !== null && lastAt) {
        const lastMs = Date.parse(lastAt);
        if (Number.isFinite(lastMs) && startedAt - lastMs < minIntervalMs) {
          skipped++;
          status = 'skipped';
          return;
        }
      }
      if (attempts >= batchSize) {
        skipped++;
        status = 'skipped';
        return;
      }
      attempts++;
      const result = await syncFundDocuments(db, code, {
        fetcher: { fetchDocuments, fetchFIIDetails, fetchDividends },
        repo,
      });
      ok++;
      repo.updateFundDocumentsAt(db, code, nowIso());

      if (telegram && lastMaxId !== null && result.status === 'ok' && result.hasNewDocument) {
        const newDocs = repo.listDocumentsSinceId(db, code, lastMaxId, 20);
        if (newDocs.length) {
          const chatIds = listTelegramChatIdsByFundCode(db, code);
          const { failed } = await telegram.notifyNewDocuments({ chatIds, fundCode: code, documents: newDocs });
          if (failed) process.stderr.write(`telegram:notify_failed:${code}:failed=${failed}\n`);
        }
      }
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
    }
  );

  const avgMs = codes.length > 0 ? Math.round(totalMs / codes.length) : 0;
  log.end({ ok, skipped, err: errCount, attempts, avg_ms: avgMs, max_ms: maxMs, minIntervalMin, batchSize, timeBudgetMs });
  return { ran: true };
}
