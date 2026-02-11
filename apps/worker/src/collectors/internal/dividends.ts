import type { Collector, CollectRequest, CollectResult, CollectorContext } from '../types';
import { fetchDividends, fetchFIIDetails } from '../../services/client';
import { toDateIsoFromBr } from '../../utils/date';
import { dividendTypeToCode } from '../../utils/dividend-type';

export const dividendsCollector: Collector = {
  name: 'dividends',
  supports(request: CollectRequest) {
    return request.collector === 'dividends' && !!request.fund_code;
  },
  async collect(request: CollectRequest, _ctx: CollectorContext): Promise<CollectResult> {
    const code = String(request.fund_code || '').toUpperCase();
    const { details, dividendsHistory } = await fetchFIIDetails(code);
    const data = await fetchDividends(code, { id: details.id, dividendsHistory });

    const items = data
      .map((d) => {
        const dateIso = toDateIsoFromBr(d.date);
        const paymentIso = toDateIsoFromBr(d.payment);
        if (!dateIso || !paymentIso) return null;
        return {
          fund_code: code,
          date_iso: dateIso,
          payment: paymentIso,
          type: dividendTypeToCode(d.type),
          value: d.value,
          yield: d.yield,
        };
      })
      .filter((v): v is { fund_code: string; date_iso: string; payment: string; type: number; value: number; yield: number } => v !== null);

    return {
      collector: 'dividends',
      fetched_at: new Date().toISOString(),
      payload: {
        persist_request: { type: 'dividends', items },
      },
    };
  },
};
