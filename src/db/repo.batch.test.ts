import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { listFundCodesForCotationsTodayBatch, listFundCodesForDocumentsBatch, listFundCodesForIndicatorsBatch } from './repo';

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

