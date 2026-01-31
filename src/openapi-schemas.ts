import { z } from '@hono/zod-openapi';

// Common schemas
export const ErrorSchema = z.object({
  error: z.string().openapi({ example: 'Failed to fetch FII list' }),
});

export const FIIParamsSchema = z.object({
  code: z.string().openapi({ param: { name: 'code', in: 'path', required: true }, example: 'binc11' }),
});

export const CotationsQuerySchema = z.object({
  days: z.string().optional().openapi({ param: { name: 'days', in: 'query' }, example: '1825' }),
});

// FII List schemas
export const FIISchema = z.object({
  code: z.string().openapi({ example: 'ZIFI11' }),
  sector: z.string().openapi({ example: 'Híbrido' }),
  p_vp: z.number().openapi({ example: 0.51 }),
  dividend_yield: z.number().openapi({ example: 0 }),
  dividend_yield_last_5_years: z.number().openapi({ example: 0.69 }),
  daily_liquidity: z.number().openapi({ example: 4939 }),
  net_worth: z.number().openapi({ example: 83537343.23 }),
  type: z.string().openapi({ example: 'Fundo de Desenvolvimento' }),
});

export const FIIResponseSchema = z.object({
  total: z.number().openapi({ example: 489 }),
  data: z.array(FIISchema),
});

// FII Details schemas
export const FIIDetailsSchema = z.object({
  id: z.string().openapi({ example: '474' }),
  code: z.string().openapi({ example: 'ZAVI11' }),
  razao_social: z.string().openapi({ example: 'FUNDO DE INVESTIMENTO IMOBILIÁRIO ZAVIT REAL ESTATE' }),
  cnpj: z.string().openapi({ example: '40575940000123' }),
  publiko_alvo: z.string().openapi({ example: 'Geral' }),
  mandato: z.string().openapi({ example: 'Renda' }),
  segmento: z.string().openapi({ example: 'Híbrido' }),
  tipo_fundo: z.string().openapi({ example: 'Fundo Misto' }),
  prazo_duracao: z.string().openapi({ example: 'Indeterminado' }),
  tipo_gestao: z.string().openapi({ example: 'Ativa' }),
  taxa_adminstracao: z.string().openapi({ example: '1,25% a.a.' }),
  vacancia: z.number().openapi({ example: 0.9 }),
  numero_cotistas: z.number().openapi({ example: 2405 }),
  cotas_emittidas: z.number().openapi({ example: 11733209 }),
  valor_patrimonial_cota: z.number().openapi({ example: 13.57 }),
  valor_patrimonial: z.number().openapi({ example: 159180000 }),
  ultimo_rendimento: z.number().openapi({ example: 0.11 }),
});

// Cotations schemas
export const CotationItemSchema = z.object({
  price: z.number().openapi({ example: 10.21 }),
  date: z.string().openapi({ example: '23/01/2026' }),
});

export const CotationsSchema = z.object({
  real: z.array(CotationItemSchema),
  dolar: z.array(CotationItemSchema),
  euro: z.array(CotationItemSchema),
});

// Dividend schemas
export const DividendItemSchema = z.object({
  value: z.number().openapi({ example: 1.11 }),
  yield: z.number().openapi({ example: 1.04 }),
  date: z.string().openapi({ example: '11/2025' }),
});

// Indicators schemas
export const IndicatorItemSchema = z.object({
  year: z.string().openapi({ example: 'Atual' }),
  value: z.number().nullable().openapi({ example: 0.51 }),
});

export const IndicatorsSchema = z.record(z.array(IndicatorItemSchema));

// Cotations today schemas
export const CotationTodayItemSchema = z.object({
  price: z.number().openapi({ example: 10.67 }),
  date: z.string().openapi({ example: '30/01/2026 10:01' }),
});

export const CotationsTodaySchema = z.object({
  real: z.array(CotationTodayItemSchema),
  dolar: z.array(CotationTodayItemSchema),
  euro: z.array(CotationTodayItemSchema),
});
