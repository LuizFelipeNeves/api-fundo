import type { FIIDetails } from '../types';
import { fetchDividends, fetchFIICotations, fetchFIIDetails, fetchFIIIndicators } from '../services/client';
import { getDb, nowIso, sha256, toDateIsoFromBr } from '../db';
import * as repo from '../db/repo';
import { syncFundIndicators } from '../core/sync/sync-fund-indicators';
import { createJobLogger, forEachConcurrentUntil, resolveConcurrency, shouldRunCotationsToday } from './utils';

function isFundMasterChanged(prev: FIIDetails | null, next: FIIDetails): boolean {
  if (!prev) return true;
  return (
    prev.id !== next.id ||
    prev.cnpj !== next.cnpj ||
    prev.razao_social !== next.razao_social ||
    prev.publico_alvo !== next.publico_alvo ||
    prev.mandato !== next.mandato ||
    prev.segmento !== next.segmento ||
    prev.tipo_fundo !== next.tipo_fundo ||
    prev.prazo_duracao !== next.prazo_duracao ||
    prev.tipo_gestao !== next.tipo_gestao ||
    prev.taxa_adminstracao !== next.taxa_adminstracao ||
    prev.vacancia !== next.vacancia ||
    prev.numero_cotistas !== next.numero_cotistas ||
    prev.cotas_emitidas !== next.cotas_emitidas ||
    prev.valor_patrimonial_cota !== next.valor_patrimonial_cota ||
    prev.valor_patrimonial !== next.valor_patrimonial ||
    prev.ultimo_rendimento !== next.ultimo_rendimento
  );
}

export async function syncDetailsDividends(): Promise<{ ran: boolean }> {
  const log = createJobLogger('sync-details-dividends');
  if (!shouldRunCotationsToday()) {
    log.skipped('outside_window');
    return { ran: false };
  }

  const db = getDb();

  const batchSizeRaw = Number.parseInt(process.env.DETAILS_DIVIDENDS_BATCH_SIZE || '50', 10);
  const batchSize = Number.isFinite(batchSizeRaw) && batchSizeRaw > 0 ? Math.min(batchSizeRaw, 5000) : 50;
  const candidatesLimit = Math.min(5000, Math.max(batchSize, batchSize * 5));
  const codes = repo.listFundCodesForDetailsSyncBatch(db, candidatesLimit);
  const concurrency = resolveConcurrency({ envKey: 'DETAILS_DIVIDENDS_CONCURRENCY', fallback: 5, max: 20 });

  const minIntervalMinRaw = Number.parseInt(process.env.DETAILS_DIVIDENDS_MIN_INTERVAL_MIN || '1', 10);
  const minIntervalMin = Number.isFinite(minIntervalMinRaw) && minIntervalMinRaw > 0 ? Math.min(minIntervalMinRaw, 24 * 60) : 1;
  const minIntervalMs = minIntervalMin * 60 * 1000;

  const timeBudgetMsRaw = Number.parseInt(process.env.DETAILS_DIVIDENDS_TIME_BUDGET_MS || '55000', 10);
  const timeBudgetMs = Number.isFinite(timeBudgetMsRaw) && timeBudgetMsRaw > 1000 ? Math.min(timeBudgetMsRaw, 10 * 60 * 1000) : 55000;
  const deadlineMs = Date.now() + timeBudgetMs;

  const enableHistorical = (process.env.ENABLE_HISTORICAL_BACKFILL || 'true').toLowerCase() !== 'false';
  const days = Number.parseInt(process.env.HISTORICAL_COTATIONS_DAYS || '365', 10);
  const historicalDays = Number.isFinite(days) && days > 0 ? Math.min(days, 1825) : 365;

  log.start({ candidates: codes.length, concurrency, batchSize, candidatesLimit, minIntervalMin, timeBudgetMs, historical: enableHistorical, historicalDays });

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
        const state = repo.getFundDetailsSyncState(db, code);
        const lastAt = state?.last_details_sync_at ?? null;
        if (lastAt) {
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
        const prev = repo.getFundDetails(db, code);
        const { details, dividendsHistory } = await fetchFIIDetails(code);
        const fundMasterChanged = isFundMasterChanged(prev, details);

        repo.updateFundDetails(db, details);

        const dividendCount = repo.getDividendCount(db, code);
        let hasNewDividendNotInDb = dividendCount === 0;
        if (!hasNewDividendNotInDb && Array.isArray(dividendsHistory) && dividendsHistory.length > 0) {
          const existing = repo.listDividendKeys(db, code, 200);
          const existingSet = new Set(existing.map((d) => `${d.date_iso}:${d.type}`));

          const limit = Math.min(dividendsHistory.length, 50);
          for (let j = 0; j < limit; j++) {
            const item: any = dividendsHistory[j];
            const dateIso = toDateIsoFromBr(item?.date);
            const type = item?.type;
            if ((type === 'Dividendos' || type === 'Amortização') && dateIso && !existingSet.has(`${dateIso}:${type}`)) {
              hasNewDividendNotInDb = true;
              break;
            }
          }
        }

        if (hasNewDividendNotInDb) {
          const dividends = await fetchDividends(code, { id: details.id, dividendsHistory });
          repo.upsertDividends(db, code, dividends);
        }

        if (fundMasterChanged) {
          await syncFundIndicators(db, code, {
            fetcher: { fetchFIIIndicators, fetchFIICotations },
            repo,
            clock: { nowIso, sha256 },
            options: { enableHistoricalBackfill: enableHistorical, historicalDays },
          });
        }
        ok++;
      } catch (err) {
        errCount++;
        status = 'err';
        const message = err instanceof Error ? err.stack || err.message : String(err);
        process.stderr.write(`sync-details-dividends:${code}:${message.replace(/\n/g, '\\n')}\n`);
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
