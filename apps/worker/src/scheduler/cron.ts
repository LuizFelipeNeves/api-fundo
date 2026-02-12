import type { ChannelModel } from 'amqplib';
import { createCollectorPublisher } from '../queues/collector';
import { shouldRunCotationsToday, shouldRunEodCotation } from '../utils/time';
import { runEodCotationRoutine } from '../runner/eod-cotation';
import { listCandidatesByState, listDocumentsCandidates } from '../db/queries';

const intervalMs = Number.parseInt(process.env.CRON_INTERVAL_MS || String(5 * 60 * 1000), 10);

// Dynamic intervals per task type (in minutes)
const TASK_INTERVALS = {
  fund_list: Number.parseInt(process.env.INTERVAL_FUND_LIST_MIN || '30', 10),
  fund_details: Number.parseInt(process.env.INTERVAL_FUND_DETAILS_MIN || '10', 10),
  cotations_today: Number.parseInt(process.env.INTERVAL_COTATIONS_TODAY_MIN || '5', 10),
  indicators: Number.parseInt(process.env.INTERVAL_INDICATORS_MIN || '30', 10),
  documents: Number.parseInt(process.env.INTERVAL_DOCUMENTS_MIN || '10', 10),
};

function getIntervalMs(taskType: keyof typeof TASK_INTERVALS): number {
  return TASK_INTERVALS[taskType] * 60 * 1000;
}

export async function startCronScheduler(connection: ChannelModel, isActive: () => boolean = () => true) {
  const publisher = await createCollectorPublisher(connection);

  const batchSizeRaw = Number.parseInt(process.env.SCHED_BATCH_SIZE || '200', 10);
  const batchSize = Number.isFinite(batchSizeRaw) && batchSizeRaw > 0 ? Math.min(batchSizeRaw, 2000) : 200;

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
    resetDailyFlags();
    const now = Date.now();

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
    const detailsCandidates = await listCandidatesByState('last_details_sync_at', batchSize, getIntervalMs('fund_details'));
    for (const code of detailsCandidates) {
      try {
        await publisher.publish({ collector: 'fund_details', fund_code: code, triggered_by: 'cron' });
      } catch {
        return;
      }
    }

    // cotations_today: 5min
    if (shouldRunCotationsToday()) {
      const todayCandidates = await listCandidatesByState('last_cotations_today_at', batchSize, getIntervalMs('cotations_today'));
      for (const code of todayCandidates) {
        try {
          await publisher.publish({ collector: 'cotations_today', fund_code: code, triggered_by: 'cron' });
        } catch {
          return;
        }
      }
    }

    // indicators: 30min
    const indicatorsCandidates = await listCandidatesByState('last_indicators_at', batchSize, getIntervalMs('indicators'), { requireId: true });
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
      const cotationsCandidates = await listCandidatesByState('last_historical_cotations_at', batchSize, BACKFILL_INTERVAL_MS, { requireId: true });
      for (const code of cotationsCandidates) {
        try {
          await publisher.publish({ collector: 'cotations', fund_code: code, range: { days: 365 }, triggered_by: 'cron' });
        } catch {
          return;
        }
      }
      cotationsBackfillDone = true;
    }

    // documents: 10min
    const documentsCandidates = await listDocumentsCandidates(batchSize, getIntervalMs('documents'));
    for (const { code, cnpj } of documentsCandidates) {
      try {
        await publisher.publish({ collector: 'documents', fund_code: code, cnpj, triggered_by: 'cron' });
      } catch {
        return;
      }
    }

    // eod_cotation: once per day after market close
    if (shouldRunEodCotation() && !eodCotationDone) {
      const processedCount = await runEodCotationRoutine();
      process.stderr.write(`[cron] eod_cotation: processed=${processedCount}\n`);
      eodCotationDone = true;
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
