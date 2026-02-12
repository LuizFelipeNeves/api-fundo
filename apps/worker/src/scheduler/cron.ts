import type { ChannelModel } from 'amqplib';
import { createCollectorPublisher } from '../queues/collector';
import { shouldRunCotationsToday, shouldRunEodCotation } from '../utils/time';
import { runEodCotationRoutineWithSql } from '../runner/eod-cotation';
import { listCandidatesByState, listDocumentsCandidates } from '../db/queries';
import { getRawSql } from '../db';
import { withTryAdvisoryXactLock } from '../utils/pg-lock';

const cronIntervalMsRaw = Number.parseInt(process.env.CRON_INTERVAL_MS || String(1 * 60 * 1000), 10);
const cronIntervalMs = Number.isFinite(cronIntervalMsRaw) && cronIntervalMsRaw > 0 ? cronIntervalMsRaw : 1 * 60 * 1000;
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
  documents: Number.parseInt(process.env.INTERVAL_DOCUMENTS_MIN || '25', 10),
};

const DEFAULT_TOTAL_CODES_ESTIMATE = 500;
let totalCodesEstimate = DEFAULT_TOTAL_CODES_ESTIMATE;
let lastTotalCodesRefreshAt = 0;

async function refreshTotalCodesEstimate(nowMs: number) {
  const refreshEveryMs = 10 * 60 * 1000;
  if (nowMs - lastTotalCodesRefreshAt < refreshEveryMs) return;

  const sql = getRawSql();
  const rows = await sql.unsafe<Array<{ count: string | number }>>('SELECT COUNT(*)::bigint AS count FROM fund_master');
  const raw = rows[0]?.count;
  const count = typeof raw === 'number' ? raw : Number.parseInt(String(raw), 10);
  if (Number.isFinite(count) && count > 0) {
    totalCodesEstimate = Math.min(count, 200000);
    lastTotalCodesRefreshAt = nowMs;
  }
}

function getIntervalMs(taskType: keyof typeof TASK_INTERVALS): number {
  const minutes = TASK_INTERVALS[taskType];
  const safeMinutes = Number.isFinite(minutes) && minutes > 0 ? minutes : 5;
  return safeMinutes * 60 * 1000;
}

function getBucketing(taskType: keyof typeof TASK_INTERVALS, nowMs: number): { bucket: number; buckets: number } {
  const taskIntervalMs = getIntervalMs(taskType);
  const buckets = Math.max(1, Math.ceil(taskIntervalMs / cronIntervalMs));
  const tick = Math.floor(nowMs / cronIntervalMs);
  const bucket = ((tick % buckets) + buckets) % buckets;
  return { bucket, buckets };
}

function getBatchSize(taskType: keyof typeof TASK_INTERVALS): number {
  const taskIntervalMs = getIntervalMs(taskType);
  const cyclesPerInterval = Math.max(1, Math.ceil(taskIntervalMs / cronIntervalMs));
  const batch = Math.ceil(totalCodesEstimate / cyclesPerInterval);
  if (!Number.isFinite(batch) || batch <= 0) return 1;
  return Math.min(batch, totalCodesEstimate);
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
      await refreshTotalCodesEstimate(now);
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
          console.log('[cron] fund_list');
          await publisher.publish({ collector: 'fund_list', triggered_by: 'cron' });
        } catch {
          return;
        }
        lastRun['fund_list'] = now;
      }

      // fund_details: 10min
      const fundDetailsBucketing = getBucketing('fund_details', now);
      const detailsCandidates = await listCandidatesByState(
        'last_details_sync_at',
        getBatchSize('fund_details'),
        getIntervalMs('fund_details'),
        fundDetailsBucketing
      );
      for (const code of detailsCandidates) {
        try {
          console.log(`[cron] fund_details: fund_code=${code}`);
          await publisher.publish({ collector: 'fund_details', fund_code: code, triggered_by: 'cron' });
        } catch {
          return;
        }
      }

      // cotations_today: 5min
      if (shouldRunCotationsToday()) {
        const cotationsTodayBucketing = getBucketing('cotations_today', now);
        const todayCandidates = await listCandidatesByState(
          'last_cotations_today_at',
          getBatchSize('cotations_today'),
          getIntervalMs('cotations_today'),
          cotationsTodayBucketing
        );
        for (const code of todayCandidates) {
          try {
            console.log(`[cron] cotations_today: fund_code=${code}`);
            await publisher.publish({ collector: 'cotations_today', fund_code: code, triggered_by: 'cron' });
          } catch {
            return;
          }
        }
      }

      // indicators: 30min
      const indicatorsBucketing = getBucketing('indicators', now);
      const indicatorsCandidates = await listCandidatesByState(
        'last_indicators_at',
        getBatchSize('indicators'),
        getIntervalMs('indicators'),
        { requireId: true, ...indicatorsBucketing }
      );
      for (const code of indicatorsCandidates) {
        try {
          console.log(`[cron] indicators: fund_code=${code}`);
          await publisher.publish({ collector: 'indicators', fund_code: code, triggered_by: 'cron' });
        } catch {
          return;
        }
      }

      // cotations (historical): backfill only once
      if (!cotationsBackfillDone) {
        const backfillEveryMs = getIntervalMs('cotations');
        if (!lastRun['cotations_backfill'] || now - lastRun['cotations_backfill'] >= backfillEveryMs) {
          lastRun['cotations_backfill'] = now;
          const BACKFILL_INTERVAL_MS = 100 * 365 * 24 * 60 * 60 * 1000;
          const cotationsCandidates = await listCandidatesByState(
            'last_historical_cotations_at',
            getBatchSize('cotations'),
            BACKFILL_INTERVAL_MS,
            { requireId: true }
          );
          if (cotationsCandidates.length === 0) {
            cotationsBackfillDone = true;
          } else {
            for (const code of cotationsCandidates) {
              try {
                console.log(`[cron] cotations: fund_code=${code}`);
                await publisher.publish({ collector: 'cotations', fund_code: code, range: { days: 365 }, triggered_by: 'cron' });
              } catch {
                return;
              }
            }
          }
        }
      }

      // documents: 10min
      const documentsBucketing = getBucketing('documents', now);
      const documentsCandidates = await listDocumentsCandidates(
        getBatchSize('documents'),
        getIntervalMs('documents'),
        documentsBucketing
      );
      for (const { code, cnpj } of documentsCandidates) {
        try {
          console.log(`[cron] documents: fund_code=${code} cnpj=${cnpj}`);
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
  const safeInterval = cronIntervalMs;

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
}
