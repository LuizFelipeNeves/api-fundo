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
    vacancia: roundTo(parsePercent(raw.vacancia), 2),
    numero_cotistas: toInt(parseNumber(raw.numero_cotistas)),
    cotas_emitidas: toInt(parseNumber(raw.cotas_emitidas)),
    valor_patrimonial_cota: roundTo(parseNumber(raw.valor_patrimonial_cota), 2),
    valor_patrimonial: roundTo(parseNumber(raw.valor_patrimonial), 2),
    ultimo_rendimento: roundTo(parseNumber(raw.ultimo_rendimento), 4),
  };
}

function roundTo(value: number, decimals: number): number {
  if (!Number.isFinite(value)) return 0;
  const p = Math.pow(10, Math.max(0, Math.min(12, Math.floor(decimals))));
  return Math.round(value * p) / p;
}

function toInt(value: number): number {
  if (!Number.isFinite(value)) return 0;
  return Math.round(value);
}

function parseNumber(value: number | string | undefined): number {
  if (value === undefined || value === null) return 0;
  if (typeof value === 'number') return value;

  const lower = value.toLowerCase();
  let multiplier = 1;
  if (/\d\s*(bilh|bi|b)\b/.test(lower)) {
    multiplier = 1_000_000_000;
  } else if (/\d\s*(milh|mi|m)\b/.test(lower)) {
    multiplier = 1_000_000;
  } else if (/\bmil\b/.test(lower) && !/\bmilh/.test(lower)) {
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
