import { syncFundsList } from './sync-funds-list';
import { syncCotationsToday } from './sync-cotations-today';
import { syncDocuments } from './sync-documents';
import { syncEodCotation } from './sync-eod-cotation';
import { syncDetailsDividends } from './sync-details-dividends';
import { getDb, nowIso } from '../db';

type JobName =
  | 'sync-funds-list'
  | 'sync-cotations-today'
  | 'sync-eod-cotation'
  | 'sync-details-dividends'
  | 'sync-documents'
  | 'reset-dividends'
  | 'all';

function parseJobName(argv: string[]): JobName {
  const raw = (argv[2] || 'all').trim();
  if (raw === 'all') return 'all';
  if (raw === 'sync-funds-list') return raw;
  if (raw === 'sync-cotations-today') return raw;
  if (raw === 'sync-eod-cotation') return raw;
  if (raw === 'sync-details-dividends') return raw;
  if (raw === 'sync-documents') return raw;
  if (raw === 'reset-dividends') return raw;
  throw new Error(`JOB_NOT_FOUND:${raw}`);
}

async function main() {
  const job = parseJobName(process.argv);

  if (job === 'all') {
    await syncFundsList();
    await syncCotationsToday();
    await syncEodCotation();
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
  if (job === 'sync-details-dividends') {
    await syncDetailsDividends();
    return;
  }
  if (job === 'sync-documents') {
    await syncDocuments();
    return;
  }
  if (job === 'reset-dividends') {
    const db = getDb();
    const beforeRow = db.prepare('select count(*) as c from dividend').get() as { c?: number } | undefined;
    const before = Number(beforeRow?.c ?? 0);
    const deleted = db.prepare('delete from dividend').run().changes;
    const now = nowIso();
    db.prepare('update fund_state set last_details_sync_at = null, updated_at = ?').run(now);
    process.stdout.write(`[reset-dividends] before=${before} deleted=${deleted}\n`);
    return;
  }
}

main().catch((err) => {
  const message = err instanceof Error ? err.message : String(err);
  process.stderr.write(`${message}\n`);
  process.exit(1);
});
