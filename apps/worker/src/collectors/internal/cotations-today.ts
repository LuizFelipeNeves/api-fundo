import type { Collector, CollectRequest, CollectResult, CollectorContext } from '../types';
import { fetchCotationsToday } from '../../services/client';
import { nowIso } from '../../utils/date';

export const cotationsTodayCollector: Collector = {
  name: 'cotations_today',
  supports(request: CollectRequest) {
    return request.collector === 'cotations_today' && !!request.fund_code;
  },
  async collect(request: CollectRequest, _ctx: CollectorContext): Promise<CollectResult> {
    const code = String(request.fund_code || '').toUpperCase();
    const data = await fetchCotationsToday(code);
    const fetchedAt = nowIso();
    const dateIso = fetchedAt.slice(0, 10);

    return {
      collector: 'cotations_today',
      fetched_at: fetchedAt,
      payload: {
        persist_request: {
          type: 'cotations_today',
          item: {
            fund_code: code,
            date_iso: dateIso,
            fetched_at: fetchedAt,
            data_json: data,
          },
        },
      },
    };
  },
};
