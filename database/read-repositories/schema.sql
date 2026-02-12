-- Read-model schema for high-performance API queries
-- This schema is populated by workers; the API is read-only.

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
