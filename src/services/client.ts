import { BASE_URL } from '../config';
import { FII_LIST_PARAMS } from '../config/fii-list';
import { get, post, fetchText } from '../http/client';
import { extractFIIDetails, extractFIIId, normalizeIndicators, normalizeCotations, normalizeDividends, normalizeFIIDetails, normalizeCotationsToday } from '../parsers';
import type { FIIResponse, FIIDetails } from '../types';
import type { NormalizedIndicators } from '../parsers/indicators';
import type { NormalizedCotations } from '../parsers/cotations';
import type { DividendData } from '../parsers/dividends';
import type { ContationsTodayData } from '../parsers/today';

export { extractFIIDetails, extractFIIId };

const FII_LIST_URL = `${BASE_URL}/api/fii/advanced-search`;

export function buildFIIListParams() {
  return new URLSearchParams(FII_LIST_PARAMS);
}

export function mapFIIListData(raw: any): FIIResponse {
  return {
    total: raw.total,
    data: raw.data.map((item: any) => ({
      code: item.name,
      sector: item.sector,
      p_vp: item.p_vp,
      dividend_yield: item.dividend_yield,
      dividend_yield_last_5_years: item.dividend_yield_last_5_years,
      daily_liquidity: item.daily_liquidity,
      net_worth: item.net_worth,
      type: item.type,
    })),
  };
}

export async function fetchFIIList(): Promise<FIIResponse> {
  const params = buildFIIListParams();
  const raw: any = await post(FII_LIST_URL, params.toString());
  return mapFIIListData(raw);
}

export async function fetchFIIDetails(code: string): Promise<FIIDetails> {
  const html = await fetchText(`${BASE_URL}/fiis/${code}/`);
  const raw = extractFIIDetails(code, html);
  const id = extractFIIId(html);
  return normalizeFIIDetails(raw, code, id);
}

export async function fetchFIIIndicators(id: string): Promise<NormalizedIndicators> {
  const raw = await get<Record<string, any[]>>(`${BASE_URL}/api/fii/historico-indicadores/${id}/5`);
  return normalizeIndicators(raw);
}

export async function fetchFIICotations(id: string, days: number = 1825): Promise<NormalizedCotations> {
  const raw = await get<Record<string, any[]>>(`${BASE_URL}/api/fii/cotacoes/chart/${id}/${days}/true`);
  return normalizeCotations(raw);
}

export async function fetchDividends(id: string, days: number = 1825): Promise<DividendData[]> {
  const [dividends, dividendYield] = await Promise.all([
    get<any[]>(`${BASE_URL}/api/fii/dividendos/chart/${id}/${days}/mes`),
    get<any[]>(`${BASE_URL}/api/fii/dividend-yield/chart/${id}/${days}/mes`),
  ]);
  return normalizeDividends(dividends, dividendYield);
}

export async function fetchCotationsToday(code: string): Promise<ContationsTodayData> {
  const raw = await get<Record<string, any[]>>(`${BASE_URL}/api/quotations/one-day/${code}/`);
  return normalizeCotationsToday(raw);
}
