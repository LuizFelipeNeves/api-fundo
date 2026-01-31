import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { getOrComputeCotationStats } from './cotation-stats';

test('getOrComputeCotationStats calcula e usa cache enquanto a cotação não muda', async () => {
  const db = new Database(':memory:');
  db.pragma('foreign_keys = ON');

  db.exec(`
    CREATE TABLE fund_master (code TEXT PRIMARY KEY, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
    CREATE TABLE cotation (fund_code TEXT NOT NULL REFERENCES fund_master(code) ON DELETE CASCADE, date_iso TEXT NOT NULL, date TEXT NOT NULL, price REAL NOT NULL, PRIMARY KEY (fund_code, date_iso));
    CREATE TABLE fund_cotation_stats (fund_code TEXT PRIMARY KEY REFERENCES fund_master(code) ON DELETE CASCADE, source_last_date_iso TEXT NOT NULL, computed_at TEXT NOT NULL, data_json TEXT NOT NULL);
  `);

  db.prepare('insert into fund_master(code, created_at, updated_at) values (?, ?, ?)').run('ABCD11', '2026-01-01T00:00:00.000Z', '2026-01-01T00:00:00.000Z');

  const ins = db.prepare('insert into cotation(fund_code, date_iso, date, price) values (?, ?, ?, ?)');
  for (let i = 0; i < 120; i++) {
    const date = new Date(Date.UTC(2025, 0, 1 + i));
    const dateIso = date.toISOString().slice(0, 10);
    const dateBr = date.toISOString().slice(8, 10) + '/' + date.toISOString().slice(5, 7) + '/' + date.toISOString().slice(0, 4);
    ins.run('ABCD11', dateIso, dateBr, 100 + i);
  }

  const s1 = getOrComputeCotationStats(db, 'abcd11');
  assert.ok(s1);
  assert.equal(s1?.fundCode, 'ABCD11');
  assert.equal(s1?.asOfDateIso, '2025-04-30');
  assert.equal(s1?.returns.d7 !== null, true);
  assert.equal(s1?.drawdown.max, 0);

  const s2 = getOrComputeCotationStats(db, 'ABCD11');
  assert.ok(s2);
  assert.equal(s2?.computedAt, s1?.computedAt);

  ins.run('ABCD11', '2025-05-01', '01/05/2025', 221);
  await new Promise((r) => setTimeout(r, 2));
  const s3 = getOrComputeCotationStats(db, 'ABCD11');
  assert.ok(s3);
  assert.notEqual(s3?.computedAt, s1?.computedAt);
});
