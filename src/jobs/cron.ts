import { syncFundsList } from './sync-funds-list';
import { syncCotationsToday } from './sync-cotations-today';
import { syncIndicators } from './sync-indicators';
import { syncDocuments } from './sync-documents';

const intervalMs = Number.parseInt(process.env.CRON_INTERVAL_MS || String(5 * 60 * 1000), 10);

async function runOnce() {
  process.stdout.write(`[jobs:cron] tick at=${new Date().toISOString()}\n`);
  await syncFundsList();
  await syncCotationsToday();
  await syncIndicators();
  await syncDocuments();
}

async function main() {
  await runOnce();
  setInterval(() => {
    runOnce().catch((err) => {
      const message = err instanceof Error ? err.message : String(err);
      process.stderr.write(`${message}\n`);
    });
  }, intervalMs);
}

main().catch((err) => {
  const message = err instanceof Error ? err.message : String(err);
  process.stderr.write(`${message}\n`);
  process.exit(1);
});
