import { syncFundsList } from './sync-funds-list';
import { syncCotationsToday } from './sync-cotations-today';
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
  await runStep('sync-details-dividends', syncDetailsDividends);
  await runStep('sync-documents', syncDocuments);
  await runStep('sync-eod-cotation', syncEodCotation);
}

async function main() {
  const safeIntervalMs = Number.isFinite(intervalMs) && intervalMs > 0 ? intervalMs : 5 * 60 * 1000;
  let running = false;
  let shuttingDown = false;
  let runningPromise: Promise<void> | null = null;
  let timer: NodeJS.Timeout | null = null;

  function stopScheduling(signal: string) {
    if (shuttingDown) return;
    shuttingDown = true;
    if (timer) clearInterval(timer);
    timer = null;
    process.stdout.write(`[jobs:cron] shutdown signal=${signal} at=${new Date().toISOString()}\n`);

    if (!runningPromise) {
      process.exit(0);
      return;
    }
    runningPromise
      .catch(() => null)
      .finally(() => process.exit(0));
  }

  process.once('SIGINT', () => stopScheduling('SIGINT'));
  process.once('SIGTERM', () => stopScheduling('SIGTERM'));

  await runOnce();
  timer = setInterval(() => {
    if (shuttingDown) return;
    if (running) {
      process.stdout.write(`[jobs:cron] skipped reason=previous_tick_still_running at=${new Date().toISOString()}\n`);
      return;
    }
    running = true;
    runningPromise = runOnce()
      .catch((err) => {
        const message = err instanceof Error ? err.message : String(err);
        process.stderr.write(`${message}\n`);
      })
      .finally(() => {
        running = false;
        runningPromise = null;
      });
  }, safeIntervalMs);
}

main().catch((err) => {
  const message = err instanceof Error ? err.message : String(err);
  process.stderr.write(`${message}\n`);
  process.exit(1);
});
