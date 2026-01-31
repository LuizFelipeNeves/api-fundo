import { syncFundsList } from './sync-funds-list';
import { syncCotationsToday } from './sync-cotations-today';
import { syncIndicators } from './sync-indicators';
import { syncDocuments } from './sync-documents';
import { syncEodCotation } from './sync-eod-cotation';

const intervalMs = Number.parseInt(process.env.CRON_INTERVAL_MS || String(5 * 60 * 1000), 10);

async function runOnce() {
  process.stdout.write(`[jobs:cron] tick at=${new Date().toISOString()}\n`);
  await syncFundsList();
  await syncCotationsToday();
  await syncIndicators();
  await syncDocuments();
  await syncEodCotation();
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
