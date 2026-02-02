import { FIIDetails } from '../types';
export { normalizeIndicators } from './indicators';
export { normalizeCotations } from './cotations';
export { normalizeDividends, type DividendData } from './dividends';
export { normalizeFIIDetails } from './fii-details';
export { normalizeCotationsToday, type CotationsTodayData, type CotationTodayItem } from './today';
export { normalizeDocuments, type DocumentData } from './documents';

export function extractFIIId(html: string): string {
  const match = html.match(/data-company-id="(\d+)"/);
  if (!match) {
    throw new Error('Could not extract FII id from HTML');
  }
  return match[1];
}

export function extractFIIDetails(code: string, html: string): Partial<FIIDetails> {
  const details: Partial<FIIDetails> = {
    id: extractFIIId(html),
    code,
  };

  const fieldMap: Record<string, keyof FIIDetails> = {
    'Razão Social': 'razao_social',
    'CNPJ': 'cnpj',
    'PÚBLICO-ALVO': 'publico_alvo',
    'MANDATO': 'mandato',
    'SEGMENTO': 'segmento',
    'TIPO DE FUNDO': 'tipo_fundo',
    'PRAZO DE DURAÇÃO': 'prazo_duracao',
    'TIPO DE GESTÃO': 'tipo_gestao',
    'TAXA DE ADMINISTRAÇÃO': 'taxa_adminstracao',
    'VACÂNCIA': 'vacancia',
    'NUMERO DE COTISTAS': 'numero_cotistas',
    'COTAS EMITIDAS': 'cotas_emitidas',
    'VAL. PATRIMONIAL P/ COTA': 'valor_patrimonial_cota',
    'VALOR PATRIMONIAL': 'valor_patrimonial',
    'ÚLTIMO RENDIMENTO': 'ultimo_rendimento',
  };

  const cellRegex = /<div class=['"]cell['"]([\s\S]*?)<\/div>\s*<\/div>/g;
  let cellMatch;

  while ((cellMatch = cellRegex.exec(html)) !== null) {
    const cellHtml = cellMatch[1];

    const nameMatch = cellHtml.match(
      /<span[^>]*class="[^"]*name[^"]*"[^>]*>([\s\S]*?)<\/span>/
    );

    const valueMatch = cellHtml.match(
      /<div class="value">[\s\S]*?<span>([\s\S]*?)<\/span>/
    );

    if (!nameMatch || !valueMatch) continue;

    const fieldName = normalizeText(nameMatch[1]);
    const fieldValue = normalizeText(valueMatch[1]);

    const key = fieldMap[fieldName];
    if (key) {
      (details as any)[key] = fieldValue;
    }
  }

  return details;
}

function normalizeText(text: string): string {
  return text
    .replace(/\s+/g, ' ')
    .replace(/&nbsp;/g, ' ')
    .trim();
}

export interface DividendItem {
  value: number;
  date: string;
  payment: string;
  type: 'Dividendos' | 'Amortização';
}

export function extractDividendsHistory(html: string): DividendItem[] {
  const tableMatch = html.match(/<table[^>]*\bid=["']table-dividends-history["'][\s\S]*?<\/table>/i);
  const tableHtml = tableMatch?.[0] ?? '';
  const tbodyMatch = tableHtml.match(/<tbody[^>]*>([\s\S]*?)<\/tbody>/i);
  const tbodyHtml = tbodyMatch?.[1] ?? '';
  if (!tbodyHtml) return [];

  const strip = (value: string) =>
    normalizeText(String(value || '').replace(/<[^>]*>/g, '').replace(/&nbsp;/g, ' ').trim());

  const dateMatch = (value: string) => {
    const m = strip(value).match(/^\d{1,2}\/\d{1,2}\/\d{4}$/);
    return m ? m[0] : '';
  };

  const parseBrlNumber = (value: string) => {
    const cleaned = strip(value).replace(/\./g, '').replace(',', '.');
    const n = Number.parseFloat(cleaned);
    return Number.isFinite(n) ? n : 0;
  };

  const items: DividendItem[] = [];
  const seen = new Set<string>();

  const rowRegex =
    /<tr[^>]*>\s*<td[^>]*>\s*([\s\S]*?)\s*<\/td>\s*<td[^>]*>\s*([\s\S]*?)\s*<\/td>\s*<td[^>]*>\s*([\s\S]*?)\s*<\/td>\s*<td[^>]*>\s*([\s\S]*?)\s*<\/td>\s*<\/tr>/gi;

  let match: RegExpExecArray | null;
  while ((match = rowRegex.exec(tbodyHtml)) !== null) {
    const typeRaw = strip(match[1]);
    const type = typeRaw === 'Dividendos' || typeRaw === 'Amortização' ? typeRaw : null;
    if (!type) continue;

    const dateCom = dateMatch(match[2]);
    const payment = dateMatch(match[3]);
    if (!dateCom || !payment) continue;

    const value = parseBrlNumber(match[4]);
    if (value <= 0) continue;

    const key = `${type}|${dateCom}|${payment}|${value}`;
    if (seen.has(key)) continue;
    seen.add(key);

    items.push({ type, date: dateCom, payment, value });
  }

  return items;
}
