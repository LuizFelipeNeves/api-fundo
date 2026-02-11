import type { CollectResult, PersistRequest } from './messages';

function isPersistRequest(value: any): value is PersistRequest {
  if (!value || typeof value !== 'object') return false;
  const type = value.type;
  if (typeof type !== 'string') return false;
  return [
    'fund_list',
    'fund_details',
    'indicators',
    'cotations',
    'cotations_today',
    'dividends',
    'documents',
  ].includes(type);
}

export function normalizeCollectResult(result: CollectResult): PersistRequest[] {
  const payload: any = result.payload;
  if (!payload) return [];

  if (Array.isArray(payload.persist_requests)) {
    return payload.persist_requests.filter(isPersistRequest);
  }

  if (payload.persist_request && isPersistRequest(payload.persist_request)) {
    return [payload.persist_request];
  }

  if (Array.isArray(payload.persist)) {
    return payload.persist.filter(isPersistRequest);
  }

  if (payload.persist && isPersistRequest(payload.persist)) {
    return [payload.persist];
  }

  if (isPersistRequest(payload)) {
    return [payload];
  }

  return [];
}
