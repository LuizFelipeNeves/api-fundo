import type { Collector, CollectRequest, CollectResult, CollectorContext } from '../types';
import { fetchFIIList } from '../../services/client';

export const fundListCollector: Collector = {
  name: 'fund_list',
  supports(request: CollectRequest) {
    return request.collector === 'fund_list';
  },
  async collect(_request: CollectRequest, _ctx: CollectorContext): Promise<CollectResult> {
    const data = await fetchFIIList();
    const items = data.data.map((item) => ({
      code: item.code,
      sector: item.sector,
      p_vp: item.p_vp,
      dividend_yield: item.dividend_yield,
      dividend_yield_last_5_years: item.dividend_yield_last_5_years,
      daily_liquidity: item.daily_liquidity,
      net_worth: item.net_worth,
      type: item.type,
    }));
    return {
      collector: 'fund_list',
      fetched_at: new Date().toISOString(),
      payload: {
        persist_request: { type: 'fund_list', items },
      },
    };
  },
};
