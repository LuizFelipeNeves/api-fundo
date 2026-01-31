import { BASE_URL } from '../config';
import { FII_LIST_PARAMS } from '../config/fii-list';
import { get, post, fetchText, fetchWithSession } from '../http/client';
import { extractFIIDetails, extractFIIId, extractDividendsHistory, normalizeIndicators, normalizeCotations, normalizeDividends, normalizeFIIDetails, normalizeCotationsToday, normalizeDocuments, type DividendItem } from '../parsers';
import type { FIIResponse, FIIDetails } from '../types';
import type { NormalizedIndicators } from '../parsers/indicators';
import type { NormalizedCotations } from '../parsers/cotations';
import type { DividendData } from '../parsers/dividends';
import type { ContationsTodayData } from '../parsers/today';
import type { DocumentData } from '../parsers/documents';

const MAX_DAYS = 1825;
export const FNET_BASE = 'https://fnet.bmfbovespa.com.br/fnet/publico';

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

export async function fetchFIIDetails(code: string): Promise<{ details: FIIDetails; dividendsHistory: DividendItem[] }> {
  const html = await fetchText(`${BASE_URL}/fiis/${code}/`);
  const raw = extractFIIDetails(code, html);
  const id = extractFIIId(html);
  const details = normalizeFIIDetails(raw, code, id);
  const dividendsHistory = extractDividendsHistory(html);
  return { details, dividendsHistory };
}

export async function fetchFIIIndicators(id: string): Promise<NormalizedIndicators> {
  const raw = await get<Record<string, any[]>>(`${BASE_URL}/api/fii/historico-indicadores/${id}/5`);
  return normalizeIndicators(raw);
}

export async function fetchFIICotations(id: string, days: number = MAX_DAYS): Promise<NormalizedCotations> {
  const raw = await get<Record<string, any[]>>(`${BASE_URL}/api/fii/cotacoes/chart/${id}/${days}/true`);
  return normalizeCotations(raw);
}

export async function fetchDividends(
  code: string,
  input?: { id?: string; dividendsHistory?: DividendItem[] }
): Promise<DividendData[]> {
  let id = input?.id ?? null;
  let dividendsHistory = input?.dividendsHistory ?? null;

  if (!id || !dividendsHistory) {
    const html = await fetchText(`${BASE_URL}/fiis/${code}/`);
    if (!id) id = extractFIIId(html);
    if (!dividendsHistory) dividendsHistory = extractDividendsHistory(html);
  }

  if (!id) throw new Error('Could not extract FII id for dividends');

  const dividendYield = await get<any[]>(`${BASE_URL}/api/fii/dividend-yield/chart/${id}/${MAX_DAYS}/mes`);
  return normalizeDividends(dividendsHistory ?? [], dividendYield);
}

export async function fetchCotationsToday(code: string): Promise<ContationsTodayData> {
  const raw = await get<Record<string, any[]>>(`${BASE_URL}/api/quotations/one-day/${code}/`);
  return normalizeCotationsToday(raw);
}

export async function fetchDocuments(cnpj: string): Promise<DocumentData[]> {
  const initUrl = `${FNET_BASE}/abrirGerenciadorDocumentosCVM?cnpjFundo=${cnpj}`;
  const dataUrl = `${FNET_BASE}/pesquisarGerenciadorDocumentosDados?d=1&s=0&l=100&o%5B0%5D%5BdataReferencia%5D=desc&idCategoriaDocumento=0&idTipoDocumento=0&idEspecieDocumento=0&isSession=true`;

  const raw = await fetchWithSession<{ data: any[] }>(initUrl, dataUrl, { timeout: 15000 });
  return normalizeDocuments(raw.data);
}
