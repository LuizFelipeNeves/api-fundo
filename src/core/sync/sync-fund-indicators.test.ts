import test from 'node:test';
import assert from 'node:assert/strict';
import { syncFundIndicators } from './sync-fund-indicators';

test('syncFundIndicators faz backfill histórico só uma vez', async () => {
  const calls: string[] = [];
  const db = {};

  const repo = {
    getFundIdAndCnpj: () => ({ id: '99', cnpj: '1' }),
    getFundState: () => ({ last_documents_max_id: null, last_historical_cotations_at: null }),
    upsertCotationsHistoricalBrl: () => {
      calls.push('upsertCotationsHistoricalBrl');
      return 1;
    },
    upsertIndicatorsSnapshot: () => {
      calls.push('upsertIndicatorsSnapshot');
      return true;
    },
  } as any;

  const fetcher = {
    fetchFIICotations: async () => {
      calls.push('fetchFIICotations');
      return { real: [{ price: 1, date: '01/01/2026' }] };
    },
    fetchFIIIndicators: async () => {
      calls.push('fetchFIIIndicators');
      return { p_vp: [{ year: 'Atual', value: 1 }] };
    },
  };

  const clock = { nowIso: () => '2026-01-01T00:00:00.000Z', sha256: () => 'hash' };

  const result = await syncFundIndicators(db, 'abcd11', {
    fetcher,
    repo,
    clock,
    options: { enableHistoricalBackfill: true, historicalDays: 365 },
  } as any);

  assert.equal(result.status, 'ok');
  assert.equal(result.didHistoricalBackfill, true);
  assert.equal(result.indicatorsChanged, true);
  assert.deepEqual(calls, ['fetchFIICotations', 'upsertCotationsHistoricalBrl', 'fetchFIIIndicators', 'upsertIndicatorsSnapshot']);
});

test('syncFundIndicators não faz backfill se já existe histórico', async () => {
  const calls: string[] = [];
  const db = {};

  const repo = {
    getFundIdAndCnpj: () => ({ id: '99', cnpj: '1' }),
    getFundState: () => ({ last_documents_max_id: null, last_historical_cotations_at: '2026-01-01T00:00:00.000Z' }),
    upsertCotationsHistoricalBrl: () => {
      calls.push('upsertCotationsHistoricalBrl');
      return 1;
    },
    upsertIndicatorsSnapshot: () => {
      calls.push('upsertIndicatorsSnapshot');
      return false;
    },
  } as any;

  const fetcher = {
    fetchFIICotations: async () => {
      calls.push('fetchFIICotations');
      throw new Error('should not be called');
    },
    fetchFIIIndicators: async () => {
      calls.push('fetchFIIIndicators');
      return {};
    },
  };

  const clock = { nowIso: () => '2026-01-01T00:00:00.000Z', sha256: () => 'hash' };

  const result = await syncFundIndicators(db, 'abcd11', {
    fetcher,
    repo,
    clock,
    options: { enableHistoricalBackfill: true, historicalDays: 365 },
  } as any);

  assert.equal(result.status, 'ok');
  assert.equal(result.didHistoricalBackfill, false);
  assert.equal(result.indicatorsChanged, false);
  assert.deepEqual(calls, ['fetchFIIIndicators', 'upsertIndicatorsSnapshot']);
});

test('syncFundIndicators pula quando falta id', async () => {
  const db = {};
  const repo = { getFundIdAndCnpj: () => ({ id: null, cnpj: '1' }) } as any;
  const fetcher = { fetchFIIIndicators: async () => { throw new Error('should not be called'); } } as any;
  const clock = { nowIso: () => 'x', sha256: () => 'y' };

  const result = await syncFundIndicators(db, 'abcd11', {
    fetcher,
    repo,
    clock,
    options: { enableHistoricalBackfill: true, historicalDays: 365 },
  } as any);

  assert.deepEqual(result, { status: 'skipped', reason: 'missing_id' });
});

