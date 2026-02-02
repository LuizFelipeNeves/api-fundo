import { syncFundsList } from './sync-funds-list';
import { syncCotationsToday } from './sync-cotations-today';
import { syncIndicators } from './sync-indicators';
import { syncDocuments } from './sync-documents';
import { syncEodCotation } from './sync-eod-cotation';
import { syncDetailsDividends } from './sync-details-dividends';

type JobName =
  | 'sync-funds-list'
  | 'sync-cotations-today'
  | 'sync-eod-cotation'
  | 'sync-indicators'
  | 'sync-details-dividends'
  | 'sync-documents'
  | 'all';

function parseJobName(argv: string[]): JobName {
  const raw = (argv[2] || 'all').trim();
  if (raw === 'all') return 'all';
  if (raw === 'sync-funds-list') return raw;
  if (raw === 'sync-cotations-today') return raw;
  if (raw === 'sync-eod-cotation') return raw;
  if (raw === 'sync-indicators') return raw;
  if (raw === 'sync-details-dividends') return raw;
  if (raw === 'sync-documents') return raw;
  throw new Error(`JOB_NOT_FOUND:${raw}`);
}

async function main() {
  const job = parseJobName(process.argv);

  if (job === 'all') {
    await syncFundsList();
    await syncCotationsToday();
    await syncEodCotation();
    await syncIndicators();
    await syncDetailsDividends();
    await syncDocuments();
    return;
  }

  if (job === 'sync-funds-list') {
    await syncFundsList();
    return;
  }
  if (job === 'sync-cotations-today') {
    await syncCotationsToday();
    return;
  }
  if (job === 'sync-eod-cotation') {
    await syncEodCotation();
    return;
  }
  if (job === 'sync-indicators') {
    await syncIndicators();
    return;
  }
  if (job === 'sync-details-dividends') {
    await syncDetailsDividends();
    return;
  }
  if (job === 'sync-documents') {
    await syncDocuments();
    return;
  }
}

main().catch((err) => {
  const message = err instanceof Error ? err.message : String(err);
  process.stderr.write(`${message}\n`);
  process.exit(1);
});
