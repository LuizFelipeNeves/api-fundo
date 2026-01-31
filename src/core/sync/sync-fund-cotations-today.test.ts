import test from 'node:test';
import assert from 'node:assert/strict';
import { syncFundCotationsToday } from './sync-fund-cotations-today';

test('syncFundCotationsToday calcula hash e persiste snapshot', async () => {
  const db = {};
  const calls: any[] = [];

  const repo = {
    upsertCotationsTodaySnapshot: (_db: any, fundCode: string, fetchedAt: string, dataHash: string, data: any) => {
      calls.push({ fundCode, fetchedAt, dataHash, data });
      return true;
    },
  } as any;

  const fetcher = {
    fetchCotationsToday: async () => ({ real: [{ price: 1, hour: '10:00' }], dolar: [], euro: [] }),
  };

  const clock = {
    nowIso: () => '2026-01-01T00:00:00.000Z',
    sha256: (v: string) => `h:${v.length}`,
  };

  const result = await syncFundCotationsToday(db, 'abcd11', { repo, fetcher, clock } as any);
  assert.deepEqual(result, { status: 'ok', code: 'ABCD11', changed: true });
  assert.equal(calls.length, 1);
  assert.equal(calls[0].fundCode, 'abcd11');
  assert.equal(calls[0].fetchedAt, '2026-01-01T00:00:00.000Z');
  assert.equal(typeof calls[0].dataHash, 'string');
});

