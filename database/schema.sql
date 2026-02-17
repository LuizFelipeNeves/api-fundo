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
  fund_code TEXT NOT NULL REFERENCES fund_master(code) ON DELETE CASCADE,
  ano SMALLINT NOT NULL,
  cotas_emitidas DOUBLE PRECISION,
  numero_de_cotistas DOUBLE PRECISION,
  vacancia DOUBLE PRECISION,
  valor_patrimonial_cota DOUBLE PRECISION,
  valor_patrimonial DOUBLE PRECISION,
  liquidez_diaria DOUBLE PRECISION,
  dividend_yield DOUBLE PRECISION,
  pvp DOUBLE PRECISION,
  valor_mercado DOUBLE PRECISION,
  fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (fund_code, ano)
);

CREATE TABLE IF NOT EXISTS cotation_today (
  fund_code TEXT NOT NULL REFERENCES fund_master(code) ON DELETE CASCADE,
  date_iso DATE NOT NULL,
  hour TIME NOT NULL,
  price REAL NOT NULL,
  fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (fund_code, date_iso, hour)
);

CREATE TABLE IF NOT EXISTS cotation (
  fund_code TEXT NOT NULL REFERENCES fund_master(code) ON DELETE CASCADE,
  date_iso DATE NOT NULL,
  price REAL NOT NULL,
  PRIMARY KEY (fund_code, date_iso)
);

CREATE TABLE IF NOT EXISTS dividend (
  fund_code TEXT NOT NULL REFERENCES fund_master(code) ON DELETE CASCADE,
  date_iso DATE NOT NULL,
  payment DATE NOT NULL,
  type INTEGER NOT NULL,
  value REAL NOT NULL,
  yield REAL NOT NULL,
  PRIMARY KEY (fund_code, date_iso, type)
);

CREATE TABLE IF NOT EXISTS document (
  fund_code TEXT NOT NULL REFERENCES fund_master(code) ON DELETE CASCADE,
  document_id INTEGER NOT NULL,
  title TEXT NOT NULL,
  category TEXT NOT NULL,
  type TEXT NOT NULL,
  date TIMESTAMPTZ NOT NULL,
  "dateUpload" TIMESTAMPTZ NOT NULL,
  url TEXT NOT NULL,
  status TEXT NOT NULL,
  version INTEGER NOT NULL,
  send BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (fund_code, document_id)
);
CREATE INDEX IF NOT EXISTS idx_document_fund_upload ON document(fund_code, "dateUpload" DESC, document_id DESC);

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
  computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  data_json JSONB NOT NULL
);
