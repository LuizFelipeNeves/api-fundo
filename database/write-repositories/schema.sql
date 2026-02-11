-- Canonical write-side schema (PostgreSQL)
-- Workers write here, then project into read models.

CREATE TABLE IF NOT EXISTS fund_master (
  code TEXT PRIMARY KEY,
  id TEXT,
  cnpj TEXT,
  sector TEXT,
  p_vp DOUBLE PRECISION,
  dividend_yield DOUBLE PRECISION,
  dividend_yield_last_5_years DOUBLE PRECISION,
  daily_liquidity DOUBLE PRECISION,
  net_worth DOUBLE PRECISION,
  type TEXT,

  razao_social TEXT,
  publico_alvo TEXT,
  mandato TEXT,
  segmento TEXT,
  tipo_fundo TEXT,
  prazo_duracao TEXT,
  tipo_gestao TEXT,
  taxa_adminstracao TEXT,
  vacancia DOUBLE PRECISION,
  numero_cotistas INTEGER,
  cotas_emitidas BIGINT,
  valor_patrimonial_cota DOUBLE PRECISION,
  valor_patrimonial DOUBLE PRECISION,
  ultimo_rendimento DOUBLE PRECISION,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fund_state (
  fund_code TEXT PRIMARY KEY REFERENCES fund_master(code) ON DELETE CASCADE,
  last_documents_max_id INTEGER,
  last_documents_at TIMESTAMPTZ,
  last_details_sync_at TIMESTAMPTZ,
  last_indicators_hash TEXT,
  last_indicators_at TIMESTAMPTZ,
  last_cotations_today_at TIMESTAMPTZ,
  last_historical_cotations_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS indicators_snapshot (
  id BIGSERIAL PRIMARY KEY,
  fund_code TEXT NOT NULL REFERENCES fund_master(code) ON DELETE CASCADE,
  fetched_at TIMESTAMPTZ NOT NULL,
  data_hash TEXT NOT NULL,
  data_json JSONB NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_indicators_snapshot_unique ON indicators_snapshot(fund_code);
CREATE INDEX IF NOT EXISTS idx_indicators_snapshot_fund_date ON indicators_snapshot(fund_code, fetched_at DESC);

CREATE TABLE IF NOT EXISTS cotations_today_snapshot (
  id BIGSERIAL PRIMARY KEY,
  fund_code TEXT NOT NULL REFERENCES fund_master(code) ON DELETE CASCADE,
  date_iso DATE NOT NULL,
  fetched_at TIMESTAMPTZ NOT NULL,
  data_json JSONB NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_cotations_today_unique ON cotations_today_snapshot(fund_code, date_iso);
CREATE INDEX IF NOT EXISTS idx_cotations_today_fund_date ON cotations_today_snapshot(fund_code, fetched_at DESC);

CREATE TABLE IF NOT EXISTS cotation (
  fund_code TEXT NOT NULL REFERENCES fund_master(code) ON DELETE CASCADE,
  date_iso DATE NOT NULL,
  price DOUBLE PRECISION NOT NULL,
  PRIMARY KEY (fund_code, date_iso)
);
CREATE INDEX IF NOT EXISTS idx_cotation_fund_date ON cotation(fund_code, date_iso DESC);

CREATE TABLE IF NOT EXISTS dividend (
  fund_code TEXT NOT NULL REFERENCES fund_master(code) ON DELETE CASCADE,
  date_iso DATE NOT NULL,
  payment DATE NOT NULL,
  type INTEGER NOT NULL,
  value DOUBLE PRECISION NOT NULL,
  yield DOUBLE PRECISION NOT NULL,
  PRIMARY KEY (fund_code, date_iso, type)
);
CREATE INDEX IF NOT EXISTS idx_dividend_fund_date ON dividend(fund_code, date_iso DESC);

CREATE TABLE IF NOT EXISTS document (
  fund_code TEXT NOT NULL REFERENCES fund_master(code) ON DELETE CASCADE,
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
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (fund_code, document_id)
);
CREATE INDEX IF NOT EXISTS idx_document_fund_upload ON document(fund_code, date_upload_iso DESC);

CREATE TABLE IF NOT EXISTS telegram_user (
  chat_id TEXT PRIMARY KEY,
  username TEXT,
  first_name TEXT,
  last_name TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS telegram_user_fund (
  chat_id TEXT NOT NULL REFERENCES telegram_user(chat_id) ON DELETE CASCADE,
  fund_code TEXT NOT NULL REFERENCES fund_master(code) ON DELETE CASCADE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (chat_id, fund_code)
);
CREATE INDEX IF NOT EXISTS idx_telegram_user_fund_fund ON telegram_user_fund(fund_code, chat_id);

CREATE TABLE IF NOT EXISTS telegram_pending_action (
  chat_id TEXT PRIMARY KEY REFERENCES telegram_user(chat_id) ON DELETE CASCADE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  action_json JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS fund_cotation_stats (
  fund_code TEXT PRIMARY KEY REFERENCES fund_master(code) ON DELETE CASCADE,
  source_last_date_iso DATE NOT NULL,
  computed_at TIMESTAMPTZ NOT NULL,
  data_json JSONB NOT NULL
);
