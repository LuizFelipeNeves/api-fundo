import type { ChannelModel } from 'amqplib';
import { createCollectorPublisher } from '../queues/collector';
import { shouldRunCotationsToday, shouldRunEodCotation } from '../utils/time';
import { runEodCotationRoutineWithSql } from '../runner/eod-cotation';
import { listCandidatesByState, listDocumentsCandidates } from '../db/queries';
import { withTryAdvisoryXactLock } from '../utils/pg-lock';

const intervalMs = Number.parseInt(process.env.CRON_INTERVAL_MS || String(5 * 60 * 1000), 10);
const eodLockKeyRaw = Number.parseInt(process.env.EOD_COTATION_LOCK_KEY || '4419270101', 10);
const eodLockKey = Number.isFinite(eodLockKeyRaw) ? eodLockKeyRaw : 4419270101;
const collectorRequestsQueue = String(process.env.COLLECT_REQUESTS_QUEUE || 'collector.requests').trim() || 'collector.requests';
const queueBacklogLimitRaw = Number.parseInt(process.env.COLLECTOR_QUEUE_BACKLOG_LIMIT || '1200', 10);
const queueBacklogLimit =
  Number.isFinite(queueBacklogLimitRaw) && queueBacklogLimitRaw > 0 ? Math.min(queueBacklogLimitRaw, 200000) : 1200;

// Dynamic intervals per task type (in minutes)
const TASK_INTERVALS = {
  fund_list: Number.parseInt(process.env.INTERVAL_FUND_LIST_MIN || '30', 10),
  fund_details: Number.parseInt(process.env.INTERVAL_FUND_DETAILS_MIN || '15', 10),
  cotations: Number.parseInt(process.env.INTERVAL_COTATIONS_MIN || '5', 10),
  cotations_today: Number.parseInt(process.env.INTERVAL_COTATIONS_TODAY_MIN || '5', 10),
  indicators: Number.parseInt(process.env.INTERVAL_INDICATORS_MIN || '30', 10),
  documents: Number.parseInt(process.env.INTERVAL_DOCUMENTS_MIN || '25', 25),
};

const TOTAL_CODES = 500;

function getIntervalMs(taskType: keyof typeof TASK_INTERVALS): number {
  return TASK_INTERVALS[taskType] * 60 * 1000;
}

function getBatchSize(taskType: keyof typeof TASK_INTERVALS): number {
  return Math.floor((TOTAL_CODES / TASK_INTERVALS[taskType]));
}

export async function startCronScheduler(connection: ChannelModel, isActive: () => boolean = () => true) {
  const publisher = await createCollectorPublisher(connection);

  let lastRun: Record<string, number> = {};
  let cotationsBackfillDone = false;
  let eodCotationDone = false;

  function resetDailyFlags() {
    const now = new Date();
    const hour = now.getHours();
    const minute = now.getMinutes();
    const totalMinutes = hour * 60 + minute;

    // Reset EOD flag before market opens (before 10:00)
    if (totalMinutes < 10 * 60) {
      eodCotationDone = false;
    }
  }

  async function tick() {
    if (!isActive()) return;
    try {
      resetDailyFlags();
      const now = Date.now();
      const queueState = await publisher.channel.checkQueue(collectorRequestsQueue);
      const readyCount = queueState?.messageCount ?? 0;
      if (readyCount >= queueBacklogLimit) {
        process.stderr.write(
          `[cron] skip scheduling: queue=${collectorRequestsQueue} ready=${readyCount} limit=${queueBacklogLimit}\n`
        );
        return;
      }

      // fund_list: 30min
      if (!lastRun['fund_list'] || now - lastRun['fund_list'] >= getIntervalMs('fund_list')) {
        try {
          await publisher.publish({ collector: 'fund_list', triggered_by: 'cron' });
        } catch {
          return;
        }
        lastRun['fund_list'] = now;
      }

      // fund_details: 10min
      const detailsCandidates = await listCandidatesByState('last_details_sync_at', getBatchSize('fund_details'), getIntervalMs('fund_details'));
      for (const code of detailsCandidates) {
        try {
          await publisher.publish({ collector: 'fund_details', fund_code: code, triggered_by: 'cron' });
        } catch {
          return;
        }
      }

      // cotations_today: 5min
      if (shouldRunCotationsToday()) {
        const todayCandidates = await listCandidatesByState('last_cotations_today_at', getBatchSize('cotations_today'), getIntervalMs('cotations_today'));
        for (const code of todayCandidates) {
          try {
            await publisher.publish({ collector: 'cotations_today', fund_code: code, triggered_by: 'cron' });
          } catch {
            return;
          }
        }
      }

      // indicators: 30min
      const indicatorsCandidates = await listCandidatesByState('last_indicators_at', getBatchSize('indicators'), getIntervalMs('indicators'), { requireId: true });
      for (const code of indicatorsCandidates) {
        try {
          await publisher.publish({ collector: 'indicators', fund_code: code, triggered_by: 'cron' });
        } catch {
          return;
        }
      }

      // cotations (historical): backfill only once
      if (!cotationsBackfillDone) {
        // Backfill: get all funds with id (100 years = ~3e12 ms)
        const BACKFILL_INTERVAL_MS = 100 * 365 * 24 * 60 * 60 * 1000;
        const cotationsCandidates = await listCandidatesByState('last_historical_cotations_at', getBatchSize('cotations'), BACKFILL_INTERVAL_MS, { requireId: true });
        if (cotationsCandidates.length === 0) {
          cotationsBackfillDone = true;
          return;
        }
        for (const code of cotationsCandidates) {
          try {
            await publisher.publish({ collector: 'cotations', fund_code: code, range: { days: 365 }, triggered_by: 'cron' });
          } catch {
            return;
          }
        }
      }

      // documents: 10min
      const documentsCandidates = await listDocumentsCandidates(getBatchSize('documents'), getIntervalMs('documents'));
      for (const { code, cnpj } of documentsCandidates) {
        try {
          await publisher.publish({ collector: 'documents', fund_code: code, cnpj, triggered_by: 'cron' });
        } catch {
          return;
        }
      }

      // eod_cotation: once per day after market close
      if (shouldRunEodCotation() && !eodCotationDone) {
        const processedCount = await withTryAdvisoryXactLock(eodLockKey, async (tx) => runEodCotationRoutineWithSql(tx));
        if (processedCount !== null) {
          process.stderr.write(`[cron] eod_cotation: processed=${processedCount}\n`);
          eodCotationDone = true;
        }
      }
    } catch (err) {
      const message = err instanceof Error ? err.stack || err.message : String(err);
      process.stderr.write(`[cron] tick error ${message.replace(/\n/g, '\\n')}\n`);
    }
  }

  let running = false;
  const safeInterval = Number.isFinite(intervalMs) && intervalMs > 0 ? intervalMs : 5 * 60 * 1000;

  await tick();
  const timer = setInterval(async () => {
    if (!isActive()) {
      clearInterval(timer);
      try {
        await publisher.channel.close();
      } catch {
        // ignore
      }
      return;
    }
    if (running) return;
    running = true;
    try {
      await tick();
    } finally {
      running = false;
    }
  }, safeInterval);

  const shutdown = async () => {
    clearInterval(timer);
    await publisher.channel.close();
  };

  process.once('SIGINT', shutdown);
  process.once('SIGTERM', shutdown);
}
