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

export async function startCronScheduler(connection: ChannelModel) {
  const publisher = await createCollectorPublisher(connection);

  const batchSizeRaw = Number.parseInt(process.env.SCHED_BATCH_SIZE || '200', 10);
  const batchSize = Number.isFinite(batchSizeRaw) && batchSizeRaw > 0 ? Math.min(batchSizeRaw, 2000) : 200;

  let lastRun: Record<string, number> = {};
  let cotationsBackfillDone = false;

  async function tick() {
    const now = Date.now();

    // fund_list: 30min
    if (!lastRun['fund_list'] || now - lastRun['fund_list'] >= getIntervalMs('fund_list')) {
      await publisher.publish({ collector: 'fund_list', triggered_by: 'cron' });
      lastRun['fund_list'] = now;
    }

    // fund_details: 10min
    const detailsCandidates = await listCandidatesByState('last_details_sync_at', batchSize, getIntervalMs('fund_details'));
    for (const code of detailsCandidates) {
      await publisher.publish({ collector: 'fund_details', fund_code: code, triggered_by: 'cron' });
    }

    // cotations_today: 5min
    if (shouldRunCotationsToday()) {
      const todayCandidates = await listCandidatesByState('last_cotations_today_at', batchSize, getIntervalMs('cotations_today'));
      for (const code of todayCandidates) {
        await publisher.publish({ collector: 'cotations_today', fund_code: code, triggered_by: 'cron' });
      }
    }

    // indicators: 30min
    const indicatorsCandidates = await listCandidatesByState('last_indicators_at', batchSize, getIntervalMs('indicators'), { requireId: true });
    for (const code of indicatorsCandidates) {
      await publisher.publish({ collector: 'indicators', fund_code: code, triggered_by: 'cron' });
    }

    // cotations (historical): backfill only once
    if (!cotationsBackfillDone) {
      const cotationsCandidates = await listCandidatesByState('last_historical_cotations_at', batchSize, Number.MAX_SAFE_INTEGER, { requireId: true });
      for (const code of cotationsCandidates) {
        await publisher.publish({ collector: 'cotations', fund_code: code, range: { days: 365 }, triggered_by: 'cron' });
      }
      cotationsBackfillDone = true;
    }

    // documents: 10min
    const documentsCandidates = await listDocumentsCandidates(batchSize, getIntervalMs('documents'));
    for (const { code, cnpj } of documentsCandidates) {
      await publisher.publish({ collector: 'documents', fund_code: code, cnpj, triggered_by: 'cron' });
    }

    if (shouldRunEodCotation()) {
      const processedCount = await runEodCotationRoutine();
      process.stderr.write(`[cron] eod_cotation: processed=${processedCount}\n`);
    }
  }

  let running = false;
  const safeInterval = Number.isFinite(intervalMs) && intervalMs > 0 ? intervalMs : 5 * 60 * 1000;

  await tick();
  const timer = setInterval(async () => {
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
