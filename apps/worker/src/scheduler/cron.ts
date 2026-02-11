import type { ChannelModel } from 'amqplib';
import { createCollectorPublisher } from '../queues/collector';
import { shouldRunCotationsToday, shouldRunEodCotation } from '../utils/time';
import { getWriteDb } from '../pipeline/db';
import { runEodCotationRoutine } from '../runner/eod-cotation';

const intervalMs = Number.parseInt(process.env.CRON_INTERVAL_MS || String(5 * 60 * 1000), 10);

type FundRow = { code: string };
type FundWithCnpjRow = { code: string; cnpj: string | null };

function toIsoCutoff(minIntervalMs: number): string {
  return new Date(Date.now() - minIntervalMs).toISOString();
}

async function listCandidatesByState(
  field: 'last_details_sync_at' | 'last_indicators_at' | 'last_cotations_today_at' | 'last_historical_cotations_at' | 'last_documents_at',
  limit: number,
  minIntervalMs: number,
  opts?: { requireId?: boolean; requireCnpj?: boolean }
): Promise<string[]> {
  const sql = getWriteDb();
  const cutoff = toIsoCutoff(minIntervalMs);

  const requireId = opts?.requireId === true;
  const requireCnpj = opts?.requireCnpj === true;

  const rows = await sql<FundRow[]>`
    SELECT fm.code
    FROM fund_master fm
    LEFT JOIN fund_state fs ON fs.fund_code = fm.code
    WHERE (${requireId} IS FALSE OR fm.id IS NOT NULL)
      AND (${requireCnpj} IS FALSE OR fm.cnpj IS NOT NULL)
      AND (fs.${sql(field)} IS NULL OR fs.${sql(field)} < ${cutoff})
    ORDER BY fs.${sql(field)} NULLS FIRST, fm.code ASC
    LIMIT ${limit}
  `;

  return rows.map((r) => r.code.toUpperCase());
}

async function listDocumentsCandidates(limit: number, minIntervalMs: number): Promise<Array<{ code: string; cnpj: string }>> {
  const sql = getWriteDb();
  const cutoff = toIsoCutoff(minIntervalMs);

  const rows = await sql<FundWithCnpjRow[]>`
    SELECT fm.code, fm.cnpj
    FROM fund_master fm
    LEFT JOIN fund_state fs ON fs.fund_code = fm.code
    WHERE fm.cnpj IS NOT NULL
      AND (fs.last_documents_at IS NULL OR fs.last_documents_at < ${cutoff})
    ORDER BY fs.last_documents_at NULLS FIRST, fm.code ASC
    LIMIT ${limit}
  `;

  return rows
    .filter((r) => r.cnpj)
    .map((r) => ({ code: r.code.toUpperCase(), cnpj: r.cnpj! }));
}

export async function startCronScheduler(connection: ChannelModel) {
  const publisher = await createCollectorPublisher(connection);

  const batchSizeRaw = Number.parseInt(process.env.SCHED_BATCH_SIZE || '200', 10);
  const batchSize = Number.isFinite(batchSizeRaw) && batchSizeRaw > 0 ? Math.min(batchSizeRaw, 2000) : 200;

  const minIntervalMinRaw = Number.parseInt(process.env.SCHED_MIN_INTERVAL_MIN || '5', 10);
  const minIntervalMin = Number.isFinite(minIntervalMinRaw) && minIntervalMinRaw > 0 ? Math.min(minIntervalMinRaw, 1440) : 5;
  const minIntervalMs = minIntervalMin * 60 * 1000;

  let lastRun: Record<string, number> = {};

  async function tick() {
    const now = Date.now();
    const safeIntervalMs = Number.isFinite(intervalMs) && intervalMs > 0 ? intervalMs : 5 * 60 * 1000;

    if (!lastRun['fund_list'] || now - lastRun['fund_list'] >= safeIntervalMs) {
      await publisher.publish({ collector: 'fund_list', triggered_by: 'cron' });
      lastRun['fund_list'] = now;
    }

    const detailsCandidates = await listCandidatesByState('last_details_sync_at', batchSize, minIntervalMs);
    for (const code of detailsCandidates) {
      await publisher.publish({ collector: 'fund_details', fund_code: code, triggered_by: 'cron' });
    }

    if (shouldRunCotationsToday()) {
      const todayCandidates = await listCandidatesByState('last_cotations_today_at', batchSize, minIntervalMs);
      for (const code of todayCandidates) {
        await publisher.publish({ collector: 'cotations_today', fund_code: code, triggered_by: 'cron' });
      }
    }

    const indicatorsCandidates = await listCandidatesByState('last_indicators_at', batchSize, minIntervalMs, { requireId: true });
    for (const code of indicatorsCandidates) {
      await publisher.publish({ collector: 'indicators', fund_code: code, triggered_by: 'cron' });
    }

    const cotationsCandidates = await listCandidatesByState('last_historical_cotations_at', batchSize, minIntervalMs, { requireId: true });
    for (const code of cotationsCandidates) {
      await publisher.publish({ collector: 'cotations', fund_code: code, range: { days: 365 }, triggered_by: 'cron' });
    }

    const documentsCandidates = await listDocumentsCandidates(batchSize, minIntervalMs);
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
