import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { hasDividend, listDividendKeys, listFundCodesForCotationsTodayBatch, listFundCodesForDetailsSyncBatch, listFundCodesForDocumentsBatch, listFundCodesForIndicatorsBatch, upsertCotationsTodaySnapshot } from './repo';
import { sha256 } from './index';

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

test('upsertCotationsTodaySnapshot atualiza snapshot do mesmo dia', () => {
  const db = new Database(':memory:');
  db.exec(`
    PRAGMA foreign_keys=ON;
    CREATE TABLE fund_master (code TEXT PRIMARY KEY, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
    CREATE TABLE fund_state (
      fund_code TEXT PRIMARY KEY REFERENCES fund_master(code) ON DELETE CASCADE,
      last_documents_max_id INTEGER,
      last_documents_at TEXT,
      last_details_sync_at TEXT,
      last_indicators_hash TEXT,
      last_indicators_at TEXT,
      last_cotations_today_hash TEXT,
      last_cotations_today_at TEXT,
      last_historical_cotations_at TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE TABLE cotations_today_snapshot (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      fund_code TEXT NOT NULL REFERENCES fund_master(code) ON DELETE CASCADE,
      date_iso TEXT NOT NULL,
      fetched_at TEXT NOT NULL,
      data_hash TEXT NOT NULL,
      data_json TEXT NOT NULL,
      UNIQUE(fund_code, date_iso)
    );
  `);
  const now = '2026-01-01T10:00:00.000Z';
  db.prepare('insert into fund_master(code, created_at, updated_at) values (?, ?, ?)').run('ABCD11', now, now);

  const changed1 = upsertCotationsTodaySnapshot(db, 'abcd11', now, 'h1', [{ price: 1, hour: '10:00' }] as any);
  assert.equal(changed1, true);
  const count1 = db.prepare('select count(1) as c from cotations_today_snapshot').get() as any;
  assert.equal(count1.c, 1);

  const later = '2026-01-01T12:00:00.000Z';
  const changed2 = upsertCotationsTodaySnapshot(db, 'abcd11', later, 'h2', [{ price: 2, hour: '12:00' }] as any);
  assert.equal(changed2, true);
  const count2 = db.prepare('select count(1) as c from cotations_today_snapshot').get() as any;
  assert.equal(count2.c, 1);

  const row = db
    .prepare('select fund_code, date_iso, fetched_at, data_hash, data_json from cotations_today_snapshot')
    .get() as any;
  assert.equal(row.fund_code, 'ABCD11');
  assert.equal(row.date_iso, '2026-01-01');
  assert.equal(row.fetched_at, later);
  assert.equal(
    row.data_hash,
    sha256(JSON.stringify([{ price: 1, hour: '10:00' }, { price: 2, hour: '12:00' }]))
  );
  assert.deepEqual(JSON.parse(row.data_json), [{ price: 1, hour: '10:00' }, { price: 2, hour: '12:00' }]);

  const state = db
    .prepare('select last_cotations_today_hash as h, last_cotations_today_at as at from fund_state where fund_code=?')
    .get('ABCD11') as any;
  assert.equal(
    state.h,
    sha256(JSON.stringify([{ price: 1, hour: '10:00' }, { price: 2, hour: '12:00' }]))
  );
  assert.equal(state.at, later);
});

test('upsertCotationsTodaySnapshot salva ordenado e sem duplicar por hora', () => {
  const db = new Database(':memory:');
  db.exec(`
    PRAGMA foreign_keys=ON;
    CREATE TABLE fund_master (code TEXT PRIMARY KEY, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
    CREATE TABLE fund_state (
      fund_code TEXT PRIMARY KEY REFERENCES fund_master(code) ON DELETE CASCADE,
      last_documents_max_id INTEGER,
      last_documents_at TEXT,
      last_details_sync_at TEXT,
      last_indicators_hash TEXT,
      last_indicators_at TEXT,
      last_cotations_today_hash TEXT,
      last_cotations_today_at TEXT,
      last_historical_cotations_at TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE TABLE cotations_today_snapshot (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      fund_code TEXT NOT NULL REFERENCES fund_master(code) ON DELETE CASCADE,
      date_iso TEXT NOT NULL,
      fetched_at TEXT NOT NULL,
      data_hash TEXT NOT NULL,
      data_json TEXT NOT NULL,
      UNIQUE(fund_code, date_iso)
    );
  `);

  const now = '2026-01-01T10:00:00.000Z';
  db.prepare('insert into fund_master(code, created_at, updated_at) values (?, ?, ?)').run('ABCD11', now, now);

  upsertCotationsTodaySnapshot(db, 'abcd11', now, 'h1', [
    { price: 2, hour: '10:01' },
    { price: 1, hour: '10:00' },
    { price: 3, hour: '10:01' },
  ] as any);

  const row = db.prepare('select data_json from cotations_today_snapshot').get() as any;
  assert.deepEqual(JSON.parse(row.data_json), [
    { price: 1, hour: '10:00' },
    { price: 3, hour: '10:01' },
  ]);
});
