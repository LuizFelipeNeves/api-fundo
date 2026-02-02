import { syncFundsList } from './sync-funds-list';
import { syncCotationsToday } from './sync-cotations-today';
import { syncIndicators } from './sync-indicators';
import { syncDetailsDividends } from './sync-details-dividends';
import { syncDocuments } from './sync-documents';
import { syncEodCotation } from './sync-eod-cotation';

const intervalMs = Number.parseInt(process.env.CRON_INTERVAL_MS || String(5 * 60 * 1000), 10);

async function runStep(name: string, fn: () => Promise<unknown>) {
  try {
    await fn();
  } catch (err) {
    const message = err instanceof Error ? err.stack || err.message : String(err);
    process.stderr.write(`[jobs:cron] step_failed step=${name} err=${message.replace(/\n/g, '\\n')}\n`);
  }
}

async function runOnce() {
  process.stdout.write(`[jobs:cron] tick at=${new Date().toISOString()}\n`);
  await runStep('sync-funds-list', syncFundsList);
  await runStep('sync-cotations-today', syncCotationsToday);
  await runStep('sync-indicators', syncIndicators);
  await runStep('sync-details-dividends', syncDetailsDividends);
  await runStep('sync-documents', syncDocuments);
  await runStep('sync-eod-cotation', syncEodCotation);
}

async function main() {
  let running = false;
  await runOnce();
  setInterval(() => {
    if (running) {
      process.stdout.write(`[jobs:cron] skipped reason=previous_tick_still_running at=${new Date().toISOString()}\n`);
      return;
    }
    running = true;
    runOnce()
      .catch((err) => {
        const message = err instanceof Error ? err.message : String(err);
        process.stderr.write(`${message}\n`);
      })
      .finally(() => {
        running = false;
      });
  }, intervalMs);
}

main().catch((err) => {
  const message = err instanceof Error ? err.message : String(err);
  process.stderr.write(`${message}\n`);
  process.exit(1);
});
