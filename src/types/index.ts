export interface FII {
  code: string;
  sector: string;
  p_vp: number;
  dividend_yield: number;
  dividend_yield_last_5_years: number;
  daily_liquidity: number;
  net_worth: number;
  type: string;
}

export interface FIIResponse {
  total: number;
  data: FII[];
}

export interface FIIDetails {
  id: string;
  code: string;
  razao_social: string;
  cnpj: string;
  publico_alvo: string;
  mandato: string;
  segmento: string;
  tipo_fundo: string;
  prazo_duracao: string;
  tipo_gestao: string;
  taxa_adminstracao: string;
  vacancia: number;
  numero_cotistas: number;
  cotas_emitidas: number;
  valor_patrimonial_cota: number;
  valor_patrimonial: number;
  ultimo_rendimento: number;
}
