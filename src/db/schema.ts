import { sqliteTable, text, real, integer, primaryKey, uniqueIndex, index } from 'drizzle-orm/sqlite-core';

export const fundMaster = sqliteTable('fund_master', {
  code: text('code').primaryKey(),
  id: text('id'),
  cnpj: text('cnpj'),
  sector: text('sector'),
  p_vp: real('p_vp'),
  dividend_yield: real('dividend_yield'),
  dividend_yield_last_5_years: real('dividend_yield_last_5_years'),
  daily_liquidity: real('daily_liquidity'),
  net_worth: real('net_worth'),
  type: text('type'),

  razao_social: text('razao_social'),
  publico_alvo: text('publico_alvo'),
  mandato: text('mandato'),
  segmento: text('segmento'),
  tipo_fundo: text('tipo_fundo'),
  prazo_duracao: text('prazo_duracao'),
  tipo_gestao: text('tipo_gestao'),
  taxa_adminstracao: text('taxa_adminstracao'),
  vacancia: real('vacancia'),
  numero_cotistas: integer('numero_cotistas'),
  cotas_emitidas: integer('cotas_emitidas'),
  valor_patrimonial_cota: real('valor_patrimonial_cota'),
  valor_patrimonial: real('valor_patrimonial'),
  ultimo_rendimento: real('ultimo_rendimento'),

  created_at: text('created_at').notNull(),
  updated_at: text('updated_at').notNull(),
});

export const fundState = sqliteTable('fund_state', {
  fund_code: text('fund_code')
    .primaryKey()
    .references(() => fundMaster.code, { onDelete: 'cascade' }),
  last_documents_max_id: integer('last_documents_max_id'),
  last_details_sync_at: text('last_details_sync_at'),
  last_indicators_hash: text('last_indicators_hash'),
  last_indicators_at: text('last_indicators_at'),
  last_cotations_today_hash: text('last_cotations_today_hash'),
  last_cotations_today_at: text('last_cotations_today_at'),
  last_historical_cotations_at: text('last_historical_cotations_at'),
  created_at: text('created_at').notNull(),
  updated_at: text('updated_at').notNull(),
});

export const indicatorsSnapshot = sqliteTable(
  'indicators_snapshot',
  {
    id: integer('id').primaryKey({ autoIncrement: true }),
    fund_code: text('fund_code')
      .notNull()
      .references(() => fundMaster.code, { onDelete: 'cascade' }),
    fetched_at: text('fetched_at').notNull(),
    data_hash: text('data_hash').notNull(),
    data_json: text('data_json').notNull(),
  },
  (t: any) => ({
    uniq: uniqueIndex('indicators_snapshot_fund_hash').on(t.fund_code, t.data_hash),
  })
);

export const cotationsTodaySnapshot = sqliteTable(
  'cotations_today_snapshot',
  {
    id: integer('id').primaryKey({ autoIncrement: true }),
    fund_code: text('fund_code')
      .notNull()
      .references(() => fundMaster.code, { onDelete: 'cascade' }),
    fetched_at: text('fetched_at').notNull(),
    data_hash: text('data_hash').notNull(),
    data_json: text('data_json').notNull(),
  },
  (t: any) => ({
    uniq: uniqueIndex('cotations_today_snapshot_fund_hash').on(t.fund_code, t.data_hash),
  })
);

export const cotation = sqliteTable(
  'cotation',
  {
    fund_code: text('fund_code')
      .notNull()
      .references(() => fundMaster.code, { onDelete: 'cascade' }),
    currency: text('currency').notNull(),
    date_iso: text('date_iso').notNull(),
    date: text('date').notNull(),
    price: real('price').notNull(),
  },
  (t: any) => ({
    pk: primaryKey({ columns: [t.fund_code, t.currency, t.date_iso] }),
    idxFundDate: index('idx_cotation_fund_date').on(t.fund_code, t.date_iso),
  })
);

export const dividend = sqliteTable(
  'dividend',
  {
    fund_code: text('fund_code')
      .notNull()
      .references(() => fundMaster.code, { onDelete: 'cascade' }),
    date_iso: text('date_iso').notNull(),
    date: text('date').notNull(),
    payment: text('payment').notNull(),
    type: text('type', { enum: ['Dividendos', 'Amortização'] }).notNull(),
    value: real('value').notNull(),
    yield: real('yield').notNull(),
  },
  (t: any) => ({
    pk: primaryKey({ columns: [t.fund_code, t.date_iso, t.type] }),
    idxFundDate: index('idx_dividend_fund_date').on(t.fund_code, t.date_iso),
  })
);

export const document = sqliteTable(
  'document',
  {
    fund_code: text('fund_code')
      .notNull()
      .references(() => fundMaster.code, { onDelete: 'cascade' }),
    document_id: integer('document_id').notNull(),
    title: text('title').notNull(),
    category: text('category').notNull(),
    type: text('type').notNull(),
    date_iso: text('date_iso').notNull(),
    date: text('date').notNull(),
    date_upload_iso: text('date_upload_iso').notNull(),
    dateUpload: text('dateUpload').notNull(),
    url: text('url').notNull(),
    status: text('status').notNull(),
    version: integer('version').notNull(),
    created_at: text('created_at').notNull(),
  },
  (t: any) => ({
    pk: primaryKey({ columns: [t.fund_code, t.document_id] }),
    idxFundUpload: index('idx_document_fund_upload').on(t.fund_code, t.date_upload_iso),
  })
);
