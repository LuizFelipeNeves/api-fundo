import Database from 'better-sqlite3';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';

let dbSingleton: Database.Database | null = null;

function clampInt(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, Math.floor(value)));
}

export function getDb(): Database.Database {
  if (dbSingleton) return dbSingleton;

  const dbPath = resolveDbPath();
  const db = new Database(dbPath);

  db.pragma('journal_mode = WAL');
  db.pragma('foreign_keys = ON');

  const busyTimeoutMsRaw = Number.parseInt(process.env.SQLITE_BUSY_TIMEOUT_MS || '5000', 10);
  const busyTimeoutMs = Number.isFinite(busyTimeoutMsRaw) ? clampInt(busyTimeoutMsRaw, 0, 60_000) : 5000;
  db.pragma(`busy_timeout = ${busyTimeoutMs}`);

  const tempStoreRaw = String(process.env.SQLITE_TEMP_STORE || '').trim().toUpperCase();
  if (tempStoreRaw === 'MEMORY' || tempStoreRaw === 'FILE') {
    db.pragma(`temp_store = ${tempStoreRaw}`);
  }

  const cacheKbRaw = Number.parseInt(process.env.SQLITE_CACHE_KB || '', 10);
  if (Number.isFinite(cacheKbRaw) && cacheKbRaw !== 0) {
    const safeKb = clampInt(Math.abs(cacheKbRaw), 256, 512_000);
    db.pragma(`cache_size = ${-safeKb}`);
  }

  const mmapMbRaw = Number.parseInt(process.env.SQLITE_MMAP_MB || '', 10);
  if (Number.isFinite(mmapMbRaw) && mmapMbRaw > 0) {
    const bytes = clampInt(mmapMbRaw, 1, 4096) * 1024 * 1024;
    db.pragma(`mmap_size = ${bytes}`);
  }

  const synchronousRaw = String(process.env.SQLITE_SYNCHRONOUS || '').trim().toUpperCase();
  if (synchronousRaw === 'OFF' || synchronousRaw === 'NORMAL' || synchronousRaw === 'FULL' || synchronousRaw === 'EXTRA') {
    db.pragma(`synchronous = ${synchronousRaw}`);
  }

  migrate(db);

  dbSingleton = db;
  return db;
}

function resolveDbPath(): string {
  const explicit = process.env.DB_PATH?.trim();
  if (explicit) return explicit;

  const cwd = process.cwd();
  const dataDir = path.resolve(cwd, 'data');
  const dataFile = path.resolve(dataDir, 'data.sqlite');
  if (fs.existsSync(dataFile)) return dataFile;
  if (fs.existsSync(dataDir)) return dataFile;

  return path.resolve(cwd, 'data.sqlite');
}

function migrate(db: Database.Database) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS fund_master (
      code TEXT PRIMARY KEY,
      id TEXT,
      cnpj TEXT,
      sector TEXT,
      p_vp REAL,
      dividend_yield REAL,
      dividend_yield_last_5_years REAL,
      daily_liquidity REAL,
      net_worth REAL,
      type TEXT,

      razao_social TEXT,
      publico_alvo TEXT,
      mandato TEXT,
      segmento TEXT,
      tipo_fundo TEXT,
      prazo_duracao TEXT,
      tipo_gestao TEXT,
      taxa_adminstracao TEXT,
      vacancia REAL,
      numero_cotistas INTEGER,
      cotas_emitidas INTEGER,
      valor_patrimonial_cota REAL,
      valor_patrimonial REAL,
      ultimo_rendimento REAL,

      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS fund_state (
      fund_code TEXT PRIMARY KEY REFERENCES fund_master(code) ON DELETE CASCADE,
      last_documents_max_id INTEGER,
      last_documents_at TEXT,
      last_details_sync_at TEXT,
      last_indicators_hash TEXT,
      last_indicators_at TEXT,
      last_cotations_today_at TEXT,
      last_historical_cotations_at TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS indicators_snapshot (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      fund_code TEXT NOT NULL REFERENCES fund_master(code) ON DELETE CASCADE,
      fetched_at TEXT NOT NULL,
      data_hash TEXT NOT NULL,
      data_json TEXT NOT NULL,
      UNIQUE(fund_code)
    );

    CREATE TABLE IF NOT EXISTS cotations_today_snapshot (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      fund_code TEXT NOT NULL REFERENCES fund_master(code) ON DELETE CASCADE,
      date_iso TEXT NOT NULL,
      fetched_at TEXT NOT NULL,
      data_json TEXT NOT NULL,
      UNIQUE(fund_code, date_iso)
    );

    CREATE TABLE IF NOT EXISTS cotation (
      fund_code TEXT NOT NULL REFERENCES fund_master(code) ON DELETE CASCADE,
      date_iso TEXT NOT NULL,
      price REAL NOT NULL,
      PRIMARY KEY (fund_code, date_iso)
    );
    CREATE INDEX IF NOT EXISTS idx_cotation_fund_date ON cotation(fund_code, date_iso);

    CREATE TABLE IF NOT EXISTS dividend (
      fund_code TEXT NOT NULL REFERENCES fund_master(code) ON DELETE CASCADE,
      date_iso TEXT NOT NULL,
      payment TEXT NOT NULL,
      type INTEGER NOT NULL,
      value REAL NOT NULL,
      yield REAL NOT NULL,
      PRIMARY KEY (fund_code, date_iso, type)
    );
    CREATE INDEX IF NOT EXISTS idx_dividend_fund_date ON dividend(fund_code, date_iso);

    CREATE TABLE IF NOT EXISTS document (
      fund_code TEXT NOT NULL REFERENCES fund_master(code) ON DELETE CASCADE,
      document_id INTEGER NOT NULL,
      title TEXT NOT NULL,
      category TEXT NOT NULL,
      type TEXT NOT NULL,
      date TEXT NOT NULL,
      date_upload_iso TEXT NOT NULL,
      dateUpload TEXT NOT NULL,
      url TEXT NOT NULL,
      status TEXT NOT NULL,
      version INTEGER NOT NULL,
      created_at TEXT NOT NULL,
      PRIMARY KEY (fund_code, document_id)
    );
    CREATE INDEX IF NOT EXISTS idx_document_fund_upload ON document(fund_code, date_upload_iso);

    CREATE TABLE IF NOT EXISTS telegram_user (
      chat_id TEXT PRIMARY KEY,
      username TEXT,
      first_name TEXT,
      last_name TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS telegram_user_fund (
      chat_id TEXT NOT NULL REFERENCES telegram_user(chat_id) ON DELETE CASCADE,
      fund_code TEXT NOT NULL REFERENCES fund_master(code) ON DELETE CASCADE,
      created_at TEXT NOT NULL,
      PRIMARY KEY (chat_id, fund_code)
    );
    CREATE INDEX IF NOT EXISTS idx_telegram_user_fund_fund ON telegram_user_fund(fund_code, chat_id);

    CREATE TABLE IF NOT EXISTS telegram_pending_action (
      chat_id TEXT PRIMARY KEY REFERENCES telegram_user(chat_id) ON DELETE CASCADE,
      created_at TEXT NOT NULL,
      action_json TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS fund_cotation_stats (
      fund_code TEXT PRIMARY KEY REFERENCES fund_master(code) ON DELETE CASCADE,
      source_last_date_iso TEXT NOT NULL,
      computed_at TEXT NOT NULL,
      data_json TEXT NOT NULL
    );
  `);
}

export function nowIso(): string {
  return new Date().toISOString();
}

export function sha256(value: string): string {
  return crypto.createHash('sha256').update(value).digest('hex');
}

export function toDateIsoFromBr(dateStr: string): string {
  const str = String(dateStr || '').trim();
  const match = str.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (!match) return '';
  const day = Number(match[1]);
  const month = Number(match[2]);
  const year = Number(match[3]);
  if (!day || !month || !year) return '';
  const iso = new Date(Date.UTC(year, month - 1, day, 0, 0, 0, 0)).toISOString().slice(0, 10);
  return iso;
}

export function toDateBrFromIso(dateIso: string): string {
  const str = String(dateIso || '').trim();
  const match = str.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (!match) return '';
  return `${match[3]}/${match[2]}/${match[1]}`;
}

/* ======================================================
   🔥 FNET SESSION (persistida por CNPJ)
====================================================== */

export interface FnetSessionData {
  jsessionId: string | null;
  lastValidAt: number | null;
}

export function getFnetSession(db: Database.Database, cnpj: string): FnetSessionData {
  const stmt = db.prepare('SELECT jsession_id, last_valid_at FROM fnet_session WHERE cnpj = ?');
  const row = stmt.get(cnpj) as { jsession_id: string; last_valid_at: string } | undefined;
  if (!row) return { jsessionId: null, lastValidAt: null };
  return { jsessionId: row.jsession_id, lastValidAt: Date.parse(row.last_valid_at) };
}

export function saveFnetSession(db: Database.Database, cnpj: string, jsessionId: string, lastValidAt: number): void {
  db.prepare(`
    INSERT INTO fnet_session (cnpj, jsession_id, last_valid_at)
    VALUES (?, ?, ?)
    ON CONFLICT(cnpj) DO UPDATE SET
      jsession_id = excluded.jsession_id,
      last_valid_at = excluded.last_valid_at
  `).run(cnpj, jsessionId, new Date(lastValidAt).toISOString());
}
