export interface IndicatorData {
  year: string;
  value: number | null;
  // key: string;
  // type: string;
}

export interface NormalizedIndicators {
  [key: string]: IndicatorData[];
}

const INDICATOR_KEY_MAP: Record<string, string> = {
  'COTAS EMITIDAS': 'cotas_emitidas',
  "NÚMERO DE COTISTAS": 'numero_de_cotistas',
  "VACÂNCIA": 'vacancia',
  "VAL. PATRIMONIAL P/ COTA": 'valor_patrimonial_cota',
  "VALOR PATRIMONIAL": 'valor_patrimonial',
  "LIQUIDEZ DIÁRIA": 'liquidez_diaria',
  "DIVIDEND YIELD (DY)": 'dividend_yield',
  "P/VP": 'pvp',
  "VALOR DE MERCADO": 'valor_mercado',
};

export function normalizeIndicators(raw: Record<string, any[]>): NormalizedIndicators {
  const normalized: NormalizedIndicators = {};

  for (const [indicatorName, values] of Object.entries(raw)) {
    const normalizedName = (INDICATOR_KEY_MAP[indicatorName] || indicatorName) as string
    normalized[normalizedName] = values.map((item) => ({
      year: item.year,
      value: item.value,
      // type: item.type,
      // key: item.key,
    }));
  }

  return normalized;
}
