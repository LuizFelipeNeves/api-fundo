-- Read-model schema for high-performance API queries
-- This schema is populated by workers; the API is read-only.

CREATE TABLE IF NOT EXISTS fund_list_read (
  code TEXT PRIMARY KEY,
  sector TEXT,
  p_vp DOUBLE PRECISION,
  dividend_yield DOUBLE PRECISION,
  dividend_yield_last_5_years DOUBLE PRECISION,
  daily_liquidity DOUBLE PRECISION,
  net_worth DOUBLE PRECISION,
  type TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fund_details_read (
  code TEXT PRIMARY KEY,
  id TEXT,
  razao_social TEXT,
  cnpj TEXT,
  publico_alvo TEXT,
  mandato TEXT,
  segmento TEXT,
  tipo_fundo TEXT,
  prazo_duracao TEXT,
  tipo_gestao TEXT,
  taxa_adminstracao TEXT,
  daily_liquidity DOUBLE PRECISION,
  vacancia DOUBLE PRECISION,
  numero_cotistas INTEGER,
  cotas_emitidas BIGINT,
  valor_patrimonial_cota DOUBLE PRECISION,
  valor_patrimonial DOUBLE PRECISION,
  ultimo_rendimento DOUBLE PRECISION,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS indicators_read (
  fund_code TEXT PRIMARY KEY,
  fetched_at TIMESTAMPTZ NOT NULL,
  data_json JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS indicators_snapshot_read (
  fund_code TEXT NOT NULL,
  fetched_at TIMESTAMPTZ NOT NULL,
  data_json JSONB NOT NULL,
  PRIMARY KEY (fund_code, fetched_at)
);
CREATE INDEX IF NOT EXISTS idx_indicators_snapshot_read_fund_date ON indicators_snapshot_read(fund_code, fetched_at DESC);

CREATE TABLE IF NOT EXISTS cotations_read (
  fund_code TEXT NOT NULL,
  date_iso DATE NOT NULL,
  price DOUBLE PRECISION NOT NULL,
  PRIMARY KEY (fund_code, date_iso)
);
CREATE INDEX IF NOT EXISTS idx_cotations_read_fund_date ON cotations_read(fund_code, date_iso DESC);

CREATE TABLE IF NOT EXISTS cotations_today_read (
  fund_code TEXT NOT NULL,
  date_iso DATE NOT NULL,
  fetched_at TIMESTAMPTZ NOT NULL,
  data_json JSONB NOT NULL,
  PRIMARY KEY (fund_code, date_iso, fetched_at)
);
CREATE INDEX IF NOT EXISTS idx_cotations_today_read_fund_date ON cotations_today_read(fund_code, fetched_at DESC);

CREATE TABLE IF NOT EXISTS dividends_read (
  fund_code TEXT NOT NULL,
  date_iso DATE NOT NULL,
  payment DATE NOT NULL,
  type INTEGER NOT NULL,
  value DOUBLE PRECISION NOT NULL,
  yield DOUBLE PRECISION NOT NULL,
  PRIMARY KEY (fund_code, date_iso, type)
);
CREATE INDEX IF NOT EXISTS idx_dividends_read_fund_date ON dividends_read(fund_code, date_iso DESC);

CREATE TABLE IF NOT EXISTS documents_read (
  fund_code TEXT NOT NULL,
  document_id INTEGER NOT NULL,
  title TEXT NOT NULL,
  category TEXT NOT NULL,
  type TEXT NOT NULL,
  date TEXT NOT NULL,
  date_upload_iso DATE NOT NULL,
  "dateUpload" TEXT NOT NULL,
  url TEXT NOT NULL,
  status TEXT NOT NULL,
  version INTEGER NOT NULL,
  PRIMARY KEY (fund_code, document_id)
);
CREATE INDEX IF NOT EXISTS idx_documents_read_fund_upload ON documents_read(fund_code, date_upload_iso DESC);
