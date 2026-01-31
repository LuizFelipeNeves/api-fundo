import { FIIDetails } from '../types';

export function normalizeFIIDetails(
  raw: Partial<FIIDetails>,
  code: string,
  id: string
): FIIDetails {
  return {
    id,
    code,
    razao_social: raw.razao_social ?? '',
    cnpj: cleanCNPJ(raw.cnpj),
    publico_alvo: raw.publico_alvo ?? '',
    mandato: raw.mandato ?? '',
    segmento: raw.segmento ?? '',
    tipo_fundo: raw.tipo_fundo ?? '',
    prazo_duracao: raw.prazo_duracao ?? '',
    tipo_gestao: raw.tipo_gestao ?? '',
    taxa_adminstracao: raw.taxa_adminstracao ?? '',
    vacancia: parsePercent(raw.vacancia),
    numero_cotistas: parseNumber(raw.numero_cotistas),
    cotas_emittidas: parseNumber(raw.cotas_emittidas),
    valor_patrimonial_cota: parseNumber(raw.valor_patrimonial_cota),
    valor_patrimonial: parseNumber(raw.valor_patrimonial),
    ultimo_rendimento: parseNumber(raw.ultimo_rendimento),
  };
}

function parseNumber(value: number | string | undefined): number {
  if (value === undefined || value === null) return 0;
  if (typeof value === 'number') return value;

  let multiplier = 1;
  if (/milhões/i.test(value)) {
    multiplier = 1_000_000;
  } else if (/bilhões/i.test(value)) {
    multiplier = 1_000_000_000;
  } else if (/\bmil\b/i.test(value) && !/milhões/i.test(value)) {
    multiplier = 1_000;
  }

  const cleaned = value
    .replace(/R\$\s?/g, '')
    .replace(/[^\d,.-]/g, '')
    .replace(/\./g, '')
    .replace(/,/g, '.');
  return (parseFloat(cleaned) || 0) * multiplier;
}

function parsePercent(value: number | string | undefined): number {
  if (value === undefined || value === null) return 0;
  if (typeof value === 'number') return value;

  // Remove %, espaços, e converte vírgula para ponto
  const cleaned = value
    .replace(/%/g, '')
    .replace(/\s/g, '')
    .replace(/,/g, '.');

  return parseFloat(cleaned) || 0;
}

function cleanCNPJ(value: string | undefined): string {
  if (!value) return '';
  return value.replace(/[.\-/]/g, '');
}
