import test from 'node:test';
import assert from 'node:assert/strict';
import { syncFundDocuments } from './sync-fund-documents';

test('syncFundDocuments busca detalhes e dividendos somente quando há documento novo', async () => {
  const calls: string[] = [];

  const db = {};
  const repo = {
    getFundIdAndCnpj: () => ({ id: '1', cnpj: '123' }),
    getFundState: () => ({ last_documents_max_id: 10, last_historical_cotations_at: null }),
    upsertDocuments: () => {
      calls.push('upsertDocuments');
      return { inserted: 1, maxId: 11 };
    },
    updateDocumentsMaxId: () => calls.push('updateDocumentsMaxId'),
    updateFundDetails: () => calls.push('updateFundDetails'),
    upsertDividends: () => {
      calls.push('upsertDividends');
      return 1;
    },
  } as any;

  const fetcher = {
    fetchDocuments: async () => [{ id: 11, title: '', category: '', type: '', date: '01/01/2026', dateUpload: '01/01/2026', url: '', status: '', version: 1 }],
    fetchFIIDetails: async () => {
      calls.push('fetchFIIDetails');
      return {
        id: '1',
        code: 'ABCD11',
        razao_social: '',
        cnpj: '123',
        publico_alvo: '',
        mandato: '',
        segmento: '',
        tipo_fundo: '',
        prazo_duracao: '',
        tipo_gestao: '',
        taxa_adminstracao: '',
        vacancia: 0,
        numero_cotistas: 0,
        cotas_emitidas: 0,
        valor_patrimonial_cota: 0,
        valor_patrimonial: 0,
        ultimo_rendimento: 0,
      };
    },
    fetchDividends: async () => {
      calls.push('fetchDividends');
      return [];
    },
  };

  const result = await syncFundDocuments(db, 'abcd11', { repo, fetcher } as any);
  assert.equal(result.status, 'ok');
  assert.equal(result.hasNewDocument, true);

  assert.deepEqual(calls, ['upsertDocuments', 'updateDocumentsMaxId', 'fetchFIIDetails', 'updateFundDetails', 'fetchDividends', 'upsertDividends']);
});

test('syncFundDocuments não busca detalhes/dividendos quando documento não é novo', async () => {
  const calls: string[] = [];

  const db = {};
  const repo = {
    getFundIdAndCnpj: () => ({ id: '1', cnpj: '123' }),
    getFundState: () => ({ last_documents_max_id: 11, last_historical_cotations_at: null }),
    upsertDocuments: () => {
      calls.push('upsertDocuments');
      return { inserted: 0, maxId: 11 };
    },
    updateDocumentsMaxId: () => calls.push('updateDocumentsMaxId'),
    updateFundDetails: () => calls.push('updateFundDetails'),
    upsertDividends: () => {
      calls.push('upsertDividends');
      return 0;
    },
  } as any;

  const fetcher = {
    fetchDocuments: async () => [],
    fetchFIIDetails: async () => {
      calls.push('fetchFIIDetails');
      throw new Error('should not be called');
    },
    fetchDividends: async () => {
      calls.push('fetchDividends');
      throw new Error('should not be called');
    },
  };

  const result = await syncFundDocuments(db, 'abcd11', { repo, fetcher } as any);
  assert.equal(result.status, 'ok');
  assert.equal(result.hasNewDocument, false);
  assert.deepEqual(calls, ['upsertDocuments', 'updateDocumentsMaxId']);
});

test('syncFundDocuments pula quando falta CNPJ', async () => {
  const db = {};
  const repo = { getFundIdAndCnpj: () => ({ id: '1', cnpj: null }) } as any;
  const fetcher = { fetchDocuments: async () => { throw new Error('should not be called'); } } as any;

  const result = await syncFundDocuments(db, 'abcd11', { repo, fetcher } as any);
  assert.deepEqual(result, { status: 'skipped', reason: 'missing_cnpj' });
});

