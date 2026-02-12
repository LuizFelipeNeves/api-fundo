import type { Collector, CollectRequest, CollectResult, CollectorContext } from '../types';
import { fetchFIICotations } from '../../services/client';
import { toDateIsoFromBr } from '../../utils/date';
import { getFundIdByCode } from '../../pipeline/repo';

const BATCH_SIZE = 50;

export const cotationsCollector: Collector = {
  name: 'cotations',
  supports(request: CollectRequest) {
    return request.collector === 'cotations' && !!request.fund_code;
  },
  async collect(request: CollectRequest, _ctx: CollectorContext): Promise<CollectResult> {
    const code = String(request.fund_code || '').toUpperCase();
    const id = await getFundIdByCode(code);
    if (!id) throw new Error('FII_NOT_FOUND');

    const days = request.range?.days ?? 365;
    const data = await fetchFIICotations(id, days);
    const items = (data.real || [])
      .map((item) => {
        const dateIso = toDateIsoFromBr(item.date);
        if (!dateIso) return null;
        return { fund_code: code, date_iso: dateIso, price: item.price };
      })
      .filter((v): v is { fund_code: string; date_iso: string; price: number } => v !== null);

    const persistRequests: Array<{ type: 'cotations'; items: typeof items }> = [];
    for (let i = 0; i < items.length; i += BATCH_SIZE) {
      const batch = items.slice(i, i + BATCH_SIZE);
      persistRequests.push({ type: 'cotations', items: batch });
    }
    return {
      collector: 'cotations',
      fetched_at: new Date().toISOString(),
      payload: { persist_requests: persistRequests },
    };
  },
};
