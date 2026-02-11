export type PersistFundListItem = {
  code: string;
  sector?: string | null;
  p_vp?: number | null;
  dividend_yield?: number | null;
  dividend_yield_last_5_years?: number | null;
  daily_liquidity?: number | null;
  net_worth?: number | null;
  type?: string | null;
};

export type PersistFundDetailsItem = {
  code: string;
  id?: string | null;
  cnpj?: string | null;
  razao_social?: string | null;
  publico_alvo?: string | null;
  mandato?: string | null;
  segmento?: string | null;
  tipo_fundo?: string | null;
  prazo_duracao?: string | null;
  tipo_gestao?: string | null;
  taxa_adminstracao?: string | null;
  daily_liquidity?: number | null;
  vacancia?: number | null;
  numero_cotistas?: number | null;
  cotas_emitidas?: number | null;
  valor_patrimonial_cota?: number | null;
  valor_patrimonial?: number | null;
  ultimo_rendimento?: number | null;
};

export type PersistIndicators = {
  fund_code: string;
  fetched_at: string;
  data_json: unknown;
  data_hash: string;
};

export type PersistCotation = {
  fund_code: string;
  date_iso: string;
  price: number;
};

export type PersistCotationsToday = {
  fund_code: string;
  date_iso: string;
  fetched_at: string;
  data_json: unknown;
};

type BaseDividend = {
  fund_code: string;
  date_iso: string;
  payment: string;
  type: number;
  value: number;
};

export type PersistDividend = BaseDividend & {
  yield: number;
};

export type Dividend = BaseDividend;

export type PersistDocument = {
  fund_code: string;
  document_id: number;
  title: string;
  category: string;
  type: string;
  date: string;
  date_upload_iso: string;
  dateUpload: string;
  url: string;
  status: string;
  version: number;
};

export type PersistRequest =
  | { type: 'fund_list'; items: PersistFundListItem[] }
  | { type: 'fund_details'; item: PersistFundDetailsItem; dividends?: Dividend[] }
  | { type: 'indicators'; item: PersistIndicators }
  | { type: 'cotations'; items: PersistCotation[] }
  | { type: 'cotations_today'; item: PersistCotationsToday }
  | { type: 'documents'; items: PersistDocument[] };

export type CollectResult = {
  collector: string;
  fetched_at: string;
  payload: unknown;
  meta?: Record<string, unknown>;
};
