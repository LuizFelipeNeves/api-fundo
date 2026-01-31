import { FIIDetails } from '../types';
export { normalizeIndicators } from './indicators';
export { normalizeCotations } from './cotations';
export { normalizeDividends, type DividendData } from './dividends';
export { normalizeFIIDetails } from './fii-details';
export { normalizeCotationsToday, type ContationsTodayData, type CotationTodayItem } from './today';

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
