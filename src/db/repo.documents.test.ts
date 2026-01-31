import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { upsertDocuments } from './repo';

test('upsertDocuments usa dateUpload para date_upload_iso mesmo com date mm/aaaa', () => {
  const db = new Database(':memory:');
  db.exec(`
    PRAGMA foreign_keys=ON;
    CREATE TABLE fund_master (
      code TEXT PRIMARY KEY,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE TABLE document (
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
    CREATE INDEX idx_document_fund_upload ON document(fund_code, date_upload_iso);
  `);

  const now = new Date().toISOString();
  db.prepare('insert into fund_master(code, created_at, updated_at) values (?, ?, ?)').run('ABCD11', now, now);

  upsertDocuments(db, 'abcd11', [
    {
      id: 123,
      title: 't',
      category: 'c',
      type: 'Informe',
      date: '07/2024',
      dateUpload: '15/07/2024',
      url: 'u',
      status: 'Ativo',
      version: 1,
    },
  ]);

  const row = db
    .prepare('select date, dateUpload, date_upload_iso from document where fund_code = ? and document_id = ?')
    .get('ABCD11', 123) as any;

  assert.equal(row.date, '07/2024');
  assert.equal(row.dateUpload, '15/07/2024');
  assert.equal(row.date_upload_iso, '2024-07-15');
});
