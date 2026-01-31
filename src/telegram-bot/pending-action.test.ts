import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { clearTelegramPendingAction, getTelegramPendingAction, upsertTelegramPendingAction } from './storage';

test('telegram pending action: upsert/get/clear', () => {
  const db = new Database(':memory:');
  db.pragma('foreign_keys = ON');

  db.exec(`
    CREATE TABLE telegram_user (
      chat_id TEXT PRIMARY KEY,
      username TEXT,
      first_name TEXT,
      last_name TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE TABLE telegram_pending_action (
      chat_id TEXT PRIMARY KEY REFERENCES telegram_user(chat_id) ON DELETE CASCADE,
      created_at TEXT NOT NULL,
      action_json TEXT NOT NULL
    );
  `);

  db.prepare('insert into telegram_user(chat_id, created_at, updated_at) values (?, ?, ?)').run('1', '2026-01-01T00:00:00.000Z', '2026-01-01T00:00:00.000Z');

  upsertTelegramPendingAction(db, '1', { kind: 'set', codes: ['ABCD11'] });
  const got1 = getTelegramPendingAction(db, '1');
  assert.ok(got1);
  assert.deepEqual(got1?.action, { kind: 'set', codes: ['ABCD11'] });

  upsertTelegramPendingAction(db, '1', { kind: 'remove', codes: ['EFGH11'] });
  const got2 = getTelegramPendingAction(db, '1');
  assert.ok(got2);
  assert.deepEqual(got2?.action, { kind: 'remove', codes: ['EFGH11'] });

  clearTelegramPendingAction(db, '1');
  const got3 = getTelegramPendingAction(db, '1');
  assert.equal(got3, null);
});

