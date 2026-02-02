import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { hasDividend, listDividendKeys, listFundCodesForCotationsTodayBatch, listFundCodesForDetailsSyncBatch, listFundCodesForDocumentsBatch, listFundCodesForIndicatorsBatch } from './repo';

test('listFundCodesForCotationsTodayBatch ordena por last_cotations_today_at asc e limita', () => {
  const db = new Database(':memory:');
  db.exec(`
    PRAGMA foreign_keys=ON;
    CREATE TABLE fund_master (code TEXT PRIMARY KEY, id TEXT, cnpj TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
    CREATE TABLE fund_state (
      fund_code TEXT PRIMARY KEY REFERENCES fund_master(code) ON DELETE CASCADE,
      last_cotations_today_at TEXT,
      last_documents_at TEXT,
      last_indicators_at TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
  `);
  const now = new Date().toISOString();
  const insFund = db.prepare('insert into fund_master(code, id, cnpj, created_at, updated_at) values (?, ?, ?, ?, ?)');
  const insState = db.prepare('insert into fund_state(fund_code, last_cotations_today_at, last_documents_at, last_indicators_at, created_at, updated_at) values (?, ?, ?, ?, ?, ?)');
  insFund.run('A', '1', '1', now, now);
  insFund.run('B', '1', '1', now, now);
  insFund.run('C', '1', '1', now, now);
  insState.run('A', '2026-01-02T00:00:00.000Z', null, null, now, now);
  insState.run('B', null, null, null, now, now);
  insState.run('C', '2026-01-01T00:00:00.000Z', null, null, now, now);

  assert.deepEqual(listFundCodesForCotationsTodayBatch(db, 2), ['B', 'C']);
});

test('listFundCodesForIndicatorsBatch filtra por id e ordena por last_indicators_at', () => {
  const db = new Database(':memory:');
  db.exec(`
    PRAGMA foreign_keys=ON;
    CREATE TABLE fund_master (code TEXT PRIMARY KEY, id TEXT, cnpj TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
    CREATE TABLE fund_state (
      fund_code TEXT PRIMARY KEY REFERENCES fund_master(code) ON DELETE CASCADE,
      last_cotations_today_at TEXT,
      last_documents_at TEXT,
      last_indicators_at TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
  `);
  const now = new Date().toISOString();
  db.prepare('insert into fund_master(code, id, cnpj, created_at, updated_at) values (?, ?, ?, ?, ?)').run('A', null, '1', now, now);
  db.prepare('insert into fund_master(code, id, cnpj, created_at, updated_at) values (?, ?, ?, ?, ?)').run('B', '9', '1', now, now);
  db.prepare('insert into fund_master(code, id, cnpj, created_at, updated_at) values (?, ?, ?, ?, ?)').run('C', '8', '1', now, now);
  db.prepare('insert into fund_state(fund_code, last_indicators_at, created_at, updated_at) values (?, ?, ?, ?)').run('A', null, now, now);
  db.prepare('insert into fund_state(fund_code, last_indicators_at, created_at, updated_at) values (?, ?, ?, ?)').run('B', '2026-01-02T00:00:00.000Z', now, now);
  db.prepare('insert into fund_state(fund_code, last_indicators_at, created_at, updated_at) values (?, ?, ?, ?)').run('C', null, now, now);

  assert.deepEqual(listFundCodesForIndicatorsBatch(db, 10), ['C', 'B']);
});

test('listFundCodesForDocumentsBatch filtra por cnpj e ordena por last_documents_at', () => {
  const db = new Database(':memory:');
  db.exec(`
    PRAGMA foreign_keys=ON;
    CREATE TABLE fund_master (code TEXT PRIMARY KEY, id TEXT, cnpj TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
    CREATE TABLE fund_state (
      fund_code TEXT PRIMARY KEY REFERENCES fund_master(code) ON DELETE CASCADE,
      last_cotations_today_at TEXT,
      last_documents_at TEXT,
      last_indicators_at TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
  `);
  const now = new Date().toISOString();
  db.prepare('insert into fund_master(code, id, cnpj, created_at, updated_at) values (?, ?, ?, ?, ?)').run('A', '1', null, now, now);
  db.prepare('insert into fund_master(code, id, cnpj, created_at, updated_at) values (?, ?, ?, ?, ?)').run('B', '1', '1', now, now);
  db.prepare('insert into fund_master(code, id, cnpj, created_at, updated_at) values (?, ?, ?, ?, ?)').run('C', '1', '1', now, now);
  db.prepare('insert into fund_state(fund_code, last_documents_at, created_at, updated_at) values (?, ?, ?, ?)').run('A', null, now, now);
  db.prepare('insert into fund_state(fund_code, last_documents_at, created_at, updated_at) values (?, ?, ?, ?)').run('B', '2026-01-02T00:00:00.000Z', now, now);
  db.prepare('insert into fund_state(fund_code, last_documents_at, created_at, updated_at) values (?, ?, ?, ?)').run('C', null, now, now);

  assert.deepEqual(listFundCodesForDocumentsBatch(db, 10), ['C', 'B']);
});

test('listFundCodesForDetailsSyncBatch ordena por last_details_sync_at asc e limita', () => {
  const db = new Database(':memory:');
  db.exec(`
    PRAGMA foreign_keys=ON;
    CREATE TABLE fund_master (code TEXT PRIMARY KEY, id TEXT, cnpj TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
    CREATE TABLE fund_state (
      fund_code TEXT PRIMARY KEY REFERENCES fund_master(code) ON DELETE CASCADE,
      last_details_sync_at TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
  `);
  const now = new Date().toISOString();
  const insFund = db.prepare('insert into fund_master(code, id, cnpj, created_at, updated_at) values (?, ?, ?, ?, ?)');
  const insState = db.prepare('insert into fund_state(fund_code, last_details_sync_at, created_at, updated_at) values (?, ?, ?, ?)');
  insFund.run('A', null, null, now, now);
  insFund.run('B', null, null, now, now);
  insFund.run('C', null, null, now, now);
  insState.run('A', '2026-01-02T00:00:00.000Z', now, now);
  insState.run('B', null, now, now);
  insState.run('C', '2026-01-01T00:00:00.000Z', now, now);

  assert.deepEqual(listFundCodesForDetailsSyncBatch(db, 2), ['B', 'C']);
});

test('hasDividend retorna true quando existe dividend para o fund/date/type', () => {
  const db = new Database(':memory:');
  db.exec(`
    PRAGMA foreign_keys=ON;
    CREATE TABLE fund_master (code TEXT PRIMARY KEY, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
    CREATE TABLE dividend (
      fund_code TEXT NOT NULL REFERENCES fund_master(code) ON DELETE CASCADE,
      date_iso TEXT NOT NULL,
      date TEXT NOT NULL,
      payment TEXT NOT NULL,
      type TEXT NOT NULL,
      value REAL NOT NULL,
      yield REAL NOT NULL,
      PRIMARY KEY (fund_code, date_iso, type)
    );
  `);
  const now = new Date().toISOString();
  db.prepare('insert into fund_master(code, created_at, updated_at) values (?, ?, ?)').run('ABCD11', now, now);
  db.prepare('insert into dividend(fund_code, date_iso, date, payment, type, value, yield) values (?, ?, ?, ?, ?, ?, ?)').run(
    'ABCD11',
    '2026-01-01',
    '01/01/2026',
    '01/01/2026',
    'Dividendos',
    1,
    1
  );

  assert.equal(hasDividend(db, 'abcd11', '2026-01-01', 'Dividendos'), true);
  assert.equal(hasDividend(db, 'abcd11', '2026-01-01', 'Amortização'), false);
  assert.equal(hasDividend(db, 'abcd11', '2026-01-02', 'Dividendos'), false);
});

test('listDividendKeys lista date_iso e type ordenado por date_iso desc', () => {
  const db = new Database(':memory:');
  db.exec(`
    PRAGMA foreign_keys=ON;
    CREATE TABLE fund_master (code TEXT PRIMARY KEY, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
    CREATE TABLE dividend (
      fund_code TEXT NOT NULL REFERENCES fund_master(code) ON DELETE CASCADE,
      date_iso TEXT NOT NULL,
      date TEXT NOT NULL,
      payment TEXT NOT NULL,
      type TEXT NOT NULL,
      value REAL NOT NULL,
      yield REAL NOT NULL,
      PRIMARY KEY (fund_code, date_iso, type)
    );
  `);
  const now = new Date().toISOString();
  db.prepare('insert into fund_master(code, created_at, updated_at) values (?, ?, ?)').run('ABCD11', now, now);
  db.prepare('insert into dividend(fund_code, date_iso, date, payment, type, value, yield) values (?, ?, ?, ?, ?, ?, ?)').run(
    'ABCD11',
    '2026-01-01',
    '01/01/2026',
    '01/01/2026',
    'Dividendos',
    1,
    1
  );
  db.prepare('insert into dividend(fund_code, date_iso, date, payment, type, value, yield) values (?, ?, ?, ?, ?, ?, ?)').run(
    'ABCD11',
    '2026-02-01',
    '01/02/2026',
    '01/02/2026',
    'Dividendos',
    1,
    1
  );

  assert.deepEqual(listDividendKeys(db, 'abcd11', 10), [
    { date_iso: '2026-02-01', type: 'Dividendos' },
    { date_iso: '2026-01-01', type: 'Dividendos' },
  ]);
});
