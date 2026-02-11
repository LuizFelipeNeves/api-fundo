import type { Collector, CollectRequest, CollectResult, CollectorContext } from '../types';
import { fetchFIIIndicators } from '../../services/client';
import { sha256, nowIso } from '../../utils/date';
import { getFundIdByCode } from '../../pipeline/repo';

export const indicatorsCollector: Collector = {
  name: 'indicators',
  supports(request: CollectRequest) {
    return request.collector === 'indicators' && !!request.fund_code;
  },
  async collect(request: CollectRequest, _ctx: CollectorContext): Promise<CollectResult> {
    const code = String(request.fund_code || '').toUpperCase();
    const id = await getFundIdByCode(code);
    if (!id) throw new Error('FII_NOT_FOUND');

    const data = await fetchFIIIndicators(id);
    const dataJson = JSON.stringify(data);
    const dataHash = sha256(dataJson);
    const fetchedAt = nowIso();

    return {
      collector: 'indicators',
      fetched_at: fetchedAt,
      payload: {
        persist_request: {
          type: 'indicators',
          item: {
            fund_code: code,
            fetched_at: fetchedAt,
            data_json: data,
            data_hash: dataHash,
          },
        },
      },
    };
  },
};
