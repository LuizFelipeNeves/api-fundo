import { pgTable, text, doublePrecision, integer, bigint, timestamp, jsonb, serial, primaryKey } from 'drizzle-orm/pg-core';
import { sql } from 'drizzle-orm';

export const fundMaster = pgTable('fund_master', {
  code: text('code').primaryKey(),
  id: text('id'),
  cnpj: text('cnpj'),
  sector: text('sector'),
  pVp: doublePrecision('p_vp'),
  dividendYield: doublePrecision('dividend_yield'),
  dividendYieldLast5Years: doublePrecision('dividend_yield_last_5_years'),
  dailyLiquidity: doublePrecision('daily_liquidity'),
  netWorth: doublePrecision('net_worth'),
  type: text('type'),
  razaoSocial: text('razao_social'),
  publicAlvo: text('publico_alvo'),
  mandato: text('mandato'),
  segmento: text('segmento'),
  tipoFundo: text('tipo_fundo'),
  prazoDuracao: text('prazo_duracao'),
  tipoGestao: text('tipo_gestao'),
  taxaAdministracao: text('taxa_adminstracao'),
  vacancia: doublePrecision('vacancia'),
  numeroCotistas: integer('numero_cotistas'),
  cotasEmitidas: bigint('cotas_emitidas', { mode: 'bigint' }),
  valorPatrimonialCota: doublePrecision('valor_patrimonial_cota'),
  valorPatrimonial: doublePrecision('valor_patrimonial'),
  ultimoRendimento: doublePrecision('ultimo_rendimento'),
  createdAt: timestamp('created_at', { withTimezone: true }).default(sql`now()`).notNull(),
  updatedAt: timestamp('updated_at', { withTimezone: true }).default(sql`now()`).notNull(),
});

export const fundState = pgTable('fund_state', {
  fundCode: text('fund_code').primaryKey().references(() => fundMaster.code, { onDelete: 'cascade' }),
  lastDocumentsMaxId: integer('last_documents_max_id'),
  lastDocumentsAt: timestamp('last_documents_at', { withTimezone: true }),
  lastDetailsSyncAt: timestamp('last_details_sync_at', { withTimezone: true }),
  lastIndicatorsHash: text('last_indicators_hash'),
  lastIndicatorsAt: timestamp('last_indicators_at', { withTimezone: true }),
  lastCotationsTodayAt: timestamp('last_cotations_today_at', { withTimezone: true }),
  lastHistoricalCotationsAt: timestamp('last_historical_cotations_at', { withTimezone: true }),
  createdAt: timestamp('created_at', { withTimezone: true }).default(sql`now()`).notNull(),
  updatedAt: timestamp('updated_at', { withTimezone: true }).default(sql`now()`).notNull(),
});

export const cotation = pgTable('cotation', {
  id: serial('id').primaryKey(),
  fundCode: text('fund_code').references(() => fundMaster.code, { onDelete: 'cascade' }).notNull(),
  dateIso: text('date_iso').notNull(),
  price: doublePrecision('price').notNull(),
  createdAt: timestamp('created_at', { withTimezone: true }).default(sql`now()`).notNull(),
});

export const dividend = pgTable('dividend', {
  id: serial('id').primaryKey(),
  fundCode: text('fund_code').references(() => fundMaster.code, { onDelete: 'cascade' }).notNull(),
  dateIso: text('date_iso').notNull(),
  payment: text('payment').notNull(),
  type: integer('type').notNull(),
  value: doublePrecision('value').notNull(),
  yield: doublePrecision('yield'),
  createdAt: timestamp('created_at', { withTimezone: true }).default(sql`now()`).notNull(),
});

export const document = pgTable('document', {
  id: serial('id').primaryKey(),
  fundCode: text('fund_code').references(() => fundMaster.code, { onDelete: 'cascade' }).notNull(),
  category: text('category'),
  type: text('type'),
  title: text('title'),
  dateUpload: text('date_upload'),
  date: text('date'),
  status: text('status'),
  version: integer('version'),
  url: text('url'),
  createdAt: timestamp('created_at', { withTimezone: true }).default(sql`now()`).notNull(),
});

export const indicatorsSnapshot = pgTable('indicators_snapshot', {
  id: serial('id').primaryKey(),
  fundCode: text('fund_code').references(() => fundMaster.code, { onDelete: 'cascade' }).notNull(),
  fetchedAt: timestamp('fetched_at', { withTimezone: true }).notNull(),
  dataJson: jsonb('data_json').notNull(),
});

export const cotationsTodaySnapshot = pgTable('cotations_today_snapshot', {
  id: serial('id').primaryKey(),
  fundCode: text('fund_code').references(() => fundMaster.code, { onDelete: 'cascade' }).notNull(),
  dateIso: text('date_iso').notNull(),
  fetchedAt: timestamp('fetched_at', { withTimezone: true }).notNull(),
  dataJson: jsonb('data_json').notNull(),
});

export const fundCotationStats = pgTable('fund_cotation_stats', {
  id: serial('id').primaryKey(),
  fundCode: text('fund_code').references(() => fundMaster.code, { onDelete: 'cascade' }).notNull(),
  dateIso: text('date_iso').notNull(),
  d7: doublePrecision('d7'),
  d30: doublePrecision('d30'),
  d90: doublePrecision('d90'),
  drawdown: doublePrecision('drawdown'),
  volatility30: doublePrecision('volatility_30'),
  volatility90: doublePrecision('volatility_90'),
  createdAt: timestamp('created_at', { withTimezone: true }).default(sql`now()`).notNull(),
});

export const telegramUser = pgTable('telegram_user', {
  id: serial('id').primaryKey(),
  telegramId: text('telegram_id').unique().notNull(),
  chatId: text('chat_id').unique().notNull(),
  username: text('username'),
  firstName: text('first_name'),
  lastName: text('last_name'),
  createdAt: timestamp('created_at', { withTimezone: true }).default(sql`now()`).notNull(),
  updatedAt: timestamp('updated_at', { withTimezone: true }).default(sql`now()`).notNull(),
});

export const telegramUserFund = pgTable('telegram_user_fund', {
  id: serial('id').primaryKey(),
  userId: integer('user_id').references(() => telegramUser.id, { onDelete: 'cascade' }).notNull(),
  fundCode: text('fund_code').references(() => fundMaster.code, { onDelete: 'cascade' }).notNull(),
  createdAt: timestamp('created_at', { withTimezone: true }).default(sql`now()`).notNull(),
});

export const telegramPendingAction = pgTable('telegram_pending_action', {
  id: serial('id').primaryKey(),
  chatId: text('chat_id').notNull(),
  actionType: text('action_type').notNull(),
  payload: jsonb('payload').notNull(),
  createdAt: timestamp('created_at', { withTimezone: true }).default(sql`now()`).notNull(),
});

// Read tables - matching database schema
export const fundListRead = pgTable('fund_list_read', {
  code: text('code').primaryKey(),
  sector: text('sector'),
  pVp: doublePrecision('p_vp'),
  dividendYield: doublePrecision('dividend_yield'),
  dividendYieldLast5Years: doublePrecision('dividend_yield_last_5_years'),
  dailyLiquidity: doublePrecision('daily_liquidity'),
  netWorth: doublePrecision('net_worth'),
  type: text('type'),
  updatedAt: timestamp('updated_at', { withTimezone: true }).notNull(),
});

export const fundDetailsRead = pgTable('fund_details_read', {
  code: text('code').primaryKey(),
  id: text('id'),
  razaoSocial: text('razao_social'),
  cnpj: text('cnpj'),
  publicAlvo: text('publico_alvo'),
  mandato: text('mandato'),
  segmento: text('segmento'),
  tipoFundo: text('tipo_fundo'),
  prazoDuracao: text('prazo_duracao'),
  tipoGestao: text('tipo_gestao'),
  taxaAdministracao: text('taxa_adminstracao'),
  dailyLiquidity: doublePrecision('daily_liquidity'),
  vacancia: doublePrecision('vacancia'),
  numeroCotistas: integer('numero_cotistas'),
  cotasEmitidas: bigint('cotas_emitidas', { mode: 'bigint' }),
  valorPatrimonialCota: doublePrecision('valor_patrimonial_cota'),
  valorPatrimonial: doublePrecision('valor_patrimonial'),
  ultimoRendimento: doublePrecision('ultimo_rendimento'),
  updatedAt: timestamp('updated_at', { withTimezone: true }).notNull(),
});

export const indicatorsRead = pgTable('indicators_read', {
  fundCode: text('fund_code').primaryKey(),
  fetchedAt: timestamp('fetched_at', { withTimezone: true }).notNull(),
  dataJson: jsonb('data_json').notNull(),
});

export const indicatorsSnapshotRead = pgTable('indicators_snapshot_read', {
  fundCode: text('fund_code').notNull(),
  fetchedAt: timestamp('fetched_at', { withTimezone: true }).notNull(),
  dataJson: jsonb('data_json').notNull(),
}, (table) => ({
  pk: primaryKey({ columns: [table.fundCode, table.fetchedAt] }),
}));

export const cotationsRead = pgTable('cotations_read', {
  fundCode: text('fund_code').notNull(),
  dateIso: text('date_iso').notNull(),
  price: doublePrecision('price').notNull(),
}, (table) => ({
  pk: primaryKey({ columns: [table.fundCode, table.dateIso] }),
}));

export const cotationsTodayRead = pgTable('cotations_today_read', {
  fundCode: text('fund_code').notNull(),
  dateIso: text('date_iso').notNull(),
  fetchedAt: timestamp('fetched_at', { withTimezone: true }).notNull(),
  dataJson: jsonb('data_json').notNull(),
}, (table) => ({
  pk: primaryKey({ columns: [table.fundCode, table.dateIso, table.fetchedAt] }),
}));

export const dividendsRead = pgTable('dividends_read', {
  fundCode: text('fund_code').notNull(),
  dateIso: text('date_iso').notNull(),
  payment: text('payment').notNull(),
  type: integer('type').notNull(),
  value: doublePrecision('value').notNull(),
  yield: doublePrecision('yield').notNull(),
}, (table) => ({
  pk: primaryKey({ columns: [table.fundCode, table.dateIso, table.type] }),
}));

export const documentsRead = pgTable('documents_read', {
  fundCode: text('fund_code').notNull(),
  id: integer('document_id').notNull(),
  title: text('title').notNull(),
  category: text('category').notNull(),
  type: text('type').notNull(),
  date: text('date').notNull(),
  dateUpload: text('date_upload_iso').notNull(),
  url: text('url').notNull(),
  status: text('status').notNull(),
  version: integer('version').notNull(),
}, (table) => ({
  pk: primaryKey({ columns: [table.fundCode, table.id] }),
}));
