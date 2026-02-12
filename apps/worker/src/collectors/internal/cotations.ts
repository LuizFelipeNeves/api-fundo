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
    const rawItems = (data.real || [])
      .map((item) => {
        const dateIso = toDateIsoFromBr(item.date);
        if (!dateIso) return null;
        return { fund_code: code, date_iso: dateIso, price: item.price };
      })
      .filter((v): v is { fund_code: string; date_iso: string; price: number } => v !== null);

    const byKey = new Map<string, (typeof rawItems)[number]>();
    for (const item of rawItems) {
      const key = `${item.fund_code}|${item.date_iso}`;
      byKey.set(key, item);
    }
    const items = Array.from(byKey.values());

    const fetchedAt = new Date().toISOString();
    const persistRequests: Array<{
      type: 'cotations';
      items: typeof items;
      fund_code: string;
      fetched_at: string;
      is_last_chunk: boolean;
    }> = [];
    for (let i = 0; i < items.length; i += BATCH_SIZE) {
      const batch = items.slice(i, i + BATCH_SIZE);
      persistRequests.push({
        type: 'cotations',
        items: batch,
        fund_code: code,
        fetched_at: fetchedAt,
        is_last_chunk: i + BATCH_SIZE >= items.length,
      });
    }
    if (persistRequests.length === 0) {
      persistRequests.push({ type: 'cotations', items: [], fund_code: code, fetched_at: fetchedAt, is_last_chunk: true });
    }
    return {
      collector: 'cotations',
      fetched_at: fetchedAt,
      payload: { persist_requests: persistRequests },
    };
  },
};
