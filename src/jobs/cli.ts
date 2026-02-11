import { syncFundsList } from './sync-funds-list';
import { syncCotationsToday } from './sync-cotations-today';
import { syncDocuments } from './sync-documents';
import { syncEodCotation } from './sync-eod-cotation';
import { syncDetailsDividends } from './sync-details-dividends';
import { getDb, nowIso } from '../db';
import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';

type JobName =
  | 'sync-funds-list'
  | 'sync-cotations-today'
  | 'reset-cotations-today'
  | 'repair-db'
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
  if (raw === 'reset-cotations-today') return raw;
  if (raw === 'repair-db') return raw;
  if (raw === 'sync-eod-cotation') return raw;
  if (raw === 'sync-details-dividends') return raw;
  if (raw === 'sync-documents') return raw;
  if (raw === 'reset-dividends') return raw;
  throw new Error(`JOB_NOT_FOUND:${raw}`);
}

function resolveDbPathForCli(): string {
  const explicit = process.env.DB_PATH?.trim();
  if (explicit) return explicit;
  const cwd = process.cwd();
  return path.resolve(cwd, 'data', 'data.sqlite');
}

function copyIfExists(filePath: string): void {
  if (!fs.existsSync(filePath)) return;
  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  const target = `${filePath}.bak.${stamp}`;
  fs.copyFileSync(filePath, target);
}

function runShell(command: string): void {
  const result = spawnSync(command, { shell: true, stdio: 'inherit' });
  if (result.status !== 0) {
    throw new Error(`CMD_FAILED:${command}`);
  }
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
  if (job === 'repair-db') {
    const dbPath = resolveDbPathForCli();
    if (!fs.existsSync(dbPath)) {
      throw new Error(`DB_NOT_FOUND:${dbPath}`);
    }

    copyIfExists(dbPath);
    copyIfExists(`${dbPath}-wal`);
    copyIfExists(`${dbPath}-shm`);

    const stamp = new Date().toISOString().replace(/[:.]/g, '-');
    const outPath = `${dbPath}.recovered.${stamp}`;

    try {
      runShell(`sqlite3 "${dbPath}" ".recover" | sqlite3 "${outPath}"`);
    } catch {
      runShell(`sqlite3 "${dbPath}" ".dump" | sqlite3 "${outPath}"`);
    }

    runShell(`sqlite3 "${outPath}" "PRAGMA integrity_check;"`);

    const badPath = `${dbPath}.corrupt.${stamp}`;
    fs.renameSync(dbPath, badPath);
    fs.renameSync(outPath, dbPath);
    try {
      fs.rmSync(`${dbPath}-wal`, { force: true });
      fs.rmSync(`${dbPath}-shm`, { force: true });
    } catch {
      null;
    }
    process.stdout.write(`[repair-db] ok db=${dbPath} backup=${badPath}\n`);
    return;
  }
  if (job === 'reset-cotations-today') {
    const db = getDb();
    const dbInfo = db.prepare('PRAGMA database_list').all() as any[];
    const file = dbInfo?.find((x) => x?.name === 'main')?.file ?? dbInfo?.[0]?.file ?? '';
    const beforeRow = db.prepare('select count(*) as c from cotations_today_snapshot').get() as { c?: number } | undefined;
    const before = Number(beforeRow?.c ?? 0);
    const deleted = db.prepare('delete from cotations_today_snapshot').run().changes;
    const now = nowIso();
    db.prepare('update fund_state set last_cotations_today_at = null, updated_at = ?').run(now);
    const afterRow = db.prepare('select count(*) as c from cotations_today_snapshot').get() as { c?: number } | undefined;
    const after = Number(afterRow?.c ?? 0);
    const fundsRow = db.prepare('select count(*) as c from fund_master').get() as { c?: number } | undefined;
    const funds = Number(fundsRow?.c ?? 0);
    process.stdout.write(`[reset-cotations-today] db=${file || '(memory)'} funds=${funds} before=${before} deleted=${deleted} after=${after}\n`);
    process.env.FORCE_RUN_JOBS = '1';
    process.env.COTATIONS_TODAY_BATCH_SIZE = process.env.COTATIONS_TODAY_BATCH_SIZE || '5000';
    process.env.COTATIONS_TODAY_TIME_BUDGET_MS = process.env.COTATIONS_TODAY_TIME_BUDGET_MS || '600000';
    process.env.COTATIONS_TODAY_MIN_INTERVAL_MIN = process.env.COTATIONS_TODAY_MIN_INTERVAL_MIN || '1';
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
