import type { Collector, CollectRequest, CollectorContext } from '../types';
import { fetchFIIList } from '../../services/client';
import type { PersistFundListItem } from '../../pipeline/messages';

const BATCH_SIZE = 50;

export const fundListCollector: Collector = {
  name: 'fund_list',
  supports(request: CollectRequest) {
    return request.collector === 'fund_list';
  },
  async collect(_request: CollectRequest, ctx: CollectorContext): Promise<void> {
    const data = await fetchFIIList();
    const items: PersistFundListItem[] = data.data.map((item) => ({
      code: item.code,
      sector: item.sector,
      p_vp: item.p_vp,
      dividend_yield: item.dividend_yield,
      dividend_yield_last_5_years: item.dividend_yield_last_5_years,
      daily_liquidity: item.daily_liquidity,
      net_worth: item.net_worth,
      type: item.type,
    }));

    // Publish in smaller batches to avoid frame size limits
    const persistQueue = process.env.PERSIST_QUEUE_NAME || 'persistence.write';
    for (let i = 0; i < items.length; i += BATCH_SIZE) {
      const batch = items.slice(i, i + BATCH_SIZE);
      const body = Buffer.from(JSON.stringify({ type: 'fund_list', items: batch }));
      ctx.publish?.(persistQueue, body);
    }
  },
};
