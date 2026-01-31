import Database from 'better-sqlite3';
import crypto from 'node:crypto';
import path from 'node:path';

let dbSingleton: Database.Database | null = null;

export function getDb(): Database.Database {
  if (dbSingleton) return dbSingleton;

  const dbPath = process.env.DB_PATH?.trim() || path.resolve(process.cwd(), 'data.sqlite');
  const db = new Database(dbPath);

  db.pragma('journal_mode = WAL');
  db.pragma('foreign_keys = ON');

  migrate(db);

  dbSingleton = db;
  return db;
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
      last_details_sync_at TEXT,
      last_indicators_hash TEXT,
      last_indicators_at TEXT,
      last_cotations_today_hash TEXT,
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
      UNIQUE(fund_code, data_hash)
    );

    CREATE TABLE IF NOT EXISTS cotations_today_snapshot (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      fund_code TEXT NOT NULL REFERENCES fund_master(code) ON DELETE CASCADE,
      fetched_at TEXT NOT NULL,
      data_hash TEXT NOT NULL,
      data_json TEXT NOT NULL,
      UNIQUE(fund_code, data_hash)
    );

    CREATE TABLE IF NOT EXISTS cotation (
      fund_code TEXT NOT NULL REFERENCES fund_master(code) ON DELETE CASCADE,
      currency TEXT NOT NULL,
      date_iso TEXT NOT NULL,
      date TEXT NOT NULL,
      price REAL NOT NULL,
      PRIMARY KEY (fund_code, currency, date_iso)
    );
    CREATE INDEX IF NOT EXISTS idx_cotation_fund_date ON cotation(fund_code, date_iso);

    CREATE TABLE IF NOT EXISTS dividend (
      fund_code TEXT NOT NULL REFERENCES fund_master(code) ON DELETE CASCADE,
      date_iso TEXT NOT NULL,
      date TEXT NOT NULL,
      payment TEXT NOT NULL,
      type TEXT NOT NULL,
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
      date_iso TEXT NOT NULL,
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
