import { BASE_URL } from '../config';
import { FII_LIST_PARAMS } from '../config/fii-list';
import { get, post, fetchText, fetchFnetWithSession } from '../http/client';
import {
  extractFIIDetails,
  extractFIIId,
  extractDividendsHistory,
  normalizeIndicators,
  normalizeCotations,
  normalizeDividends,
  normalizeFIIDetails,
  normalizeCotationsToday,
  normalizeDocuments,
  type DividendItem,
} from '../parsers';
import type { FIIResponse, FIIDetails } from '../types';
import type { NormalizedIndicators } from '../parsers/indicators';
import type { NormalizedCotations } from '../parsers/cotations';
import type { DividendData } from '../parsers/dividends';
import type { CotationsTodayData } from '../parsers/today';
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
      code: normalizeFundCode(item.name),
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

function normalizeFundCode(code: string): string {
  return String(code ?? '')
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, '');
}

const INVESTIDOR10_HTML_TIMEOUT_MS = (() => {
  const parsed = Number.parseInt(process.env.INVESTIDOR10_HTML_TIMEOUT_MS || '15000', 10);
  return Number.isFinite(parsed) && parsed > 0 ? Math.min(parsed, 120000) : 15000;
})();

const INVESTIDOR10_API_TIMEOUT_MS = (() => {
  const parsed = Number.parseInt(process.env.INVESTIDOR10_API_TIMEOUT_MS || '25000', 10);
  return Number.isFinite(parsed) && parsed > 0 ? Math.min(parsed, 120000) : 25000;
})();

const FNET_TIMEOUT_MS = (() => {
  const parsed = Number.parseInt(process.env.FNET_TIMEOUT_MS || '25000', 10);
  return Number.isFinite(parsed) && parsed > 0 ? Math.min(parsed, 120000) : 25000;
})();

export async function fetchFIIList(): Promise<FIIResponse> {
  const params = buildFIIListParams();
  const raw: any = await post(FII_LIST_URL, params.toString());
  return mapFIIListData(raw);
}

export async function fetchFIIDetails(code: string): Promise<{ details: FIIDetails; dividendsHistory: DividendItem[] }> {
  const safeCode = normalizeFundCode(code);
  const html = await fetchText(`${BASE_URL}/fiis/${safeCode}/`, { timeout: INVESTIDOR10_HTML_TIMEOUT_MS });
  const raw = extractFIIDetails(safeCode, html);
  const id = extractFIIId(html);
  const details = normalizeFIIDetails(raw, safeCode, id);
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
  const safeCode = normalizeFundCode(code);
  let id = input?.id ?? null;
  let dividendsHistory = input?.dividendsHistory ?? null;

  if (!id || !dividendsHistory) {
    const html = await fetchText(`${BASE_URL}/fiis/${safeCode}/`, { timeout: INVESTIDOR10_HTML_TIMEOUT_MS });
    if (!id) id = extractFIIId(html);
    if (!dividendsHistory) dividendsHistory = extractDividendsHistory(html);
  }

  if (!id) throw new Error('Could not extract FII id for dividends');

  const dividendYield = await get<any[]>(`${BASE_URL}/api/fii/dividend-yield/chart/${id}/${MAX_DAYS}/mes`);
  return normalizeDividends(dividendsHistory ?? [], dividendYield);
}

export async function fetchCotationsToday(code: string): Promise<CotationsTodayData> {
  const safeCode = normalizeFundCode(code);
  const params = new URLSearchParams();
  params.set('ticker', safeCode);
  params.set('type', '-1');
  params.append('currences[]', '1');

  const statusInvestBase = 'https://statusinvest.com.br';
  const raw = await post<any>(`${statusInvestBase}/fii/tickerprice`, params.toString(), {
    timeout: INVESTIDOR10_API_TIMEOUT_MS,
    retryMax: 8,
    retryBaseMs: 400,
    headers: {
      accept: '*/*',
      'accept-language': 'pt-BR,pt;q=0.8',
      'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
      origin: statusInvestBase,
      referer: `${statusInvestBase}/fundos-imobiliarios/${safeCode.toLowerCase()}`,
      'user-agent':
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
    },
  });
  return normalizeCotationsToday(raw);
}

export async function fetchDocuments(cnpj: string): Promise<DocumentData[]> {
  const initUrl = `${FNET_BASE}/abrirGerenciadorDocumentosCVM?cnpjFundo=${cnpj}`;
  const dataUrl = `${FNET_BASE}/pesquisarGerenciadorDocumentosDados?d=1&s=0&l=100&o%5B0%5D%5BdataReferencia%5D=desc&idCategoriaDocumento=0&idTipoDocumento=0&idEspecieDocumento=0&isSession=true`;

  const raw = await fetchFnetWithSession<{ data: any[] }>(initUrl, dataUrl, { timeout: FNET_TIMEOUT_MS });
  return normalizeDocuments(raw.data);
}
