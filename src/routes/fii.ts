import { OpenAPIHono, createRoute, z } from '@hono/zod-openapi';
import {
  fetchFIIList,
  fetchFIIDetails,
  fetchFIIIndicators,
  fetchFIICotations,
  fetchDividends,
  fetchCotationsToday,
} from '../services/client';
import { createHandler } from '../helpers';
import {
  ErrorSchema,
  FIIParamsSchema,
  CotationsQuerySchema,
  FIIResponseSchema,
  FIIDetailsSchema,
  CotationsSchema,
  DividendItemSchema,
  IndicatorsSchema,
  CotationsTodaySchema,
} from '../openapi-schemas';

const app = new OpenAPIHono();

// Rota: Lista todos os FIIs
const listFIIRoute = createRoute({
  method: 'get',
  path: '/',
  tags: ['FII'],
  summary: 'Lista todos os FIIs',
  responses: {
    200: { description: 'Lista de FIIs', content: { 'application/json': { schema: FIIResponseSchema } } },
    500: { content: { 'application/json': { schema: ErrorSchema } }, description: 'Erro' },
  },
});

app.openapi(
  listFIIRoute,
  createHandler(async () => {
    const data = await fetchFIIList();
    return { data };
  }, 'fetchFIIList') as any
);

// Rota: Dados básicos do FII
const getFIIDetailsRoute = createRoute({
  method: 'get',
  path: '/{code}',
  tags: ['FII'],
  summary: 'Dados básicos do FII',
  request: { params: FIIParamsSchema },
  responses: {
    200: { description: 'Dados do FII', content: { 'application/json': { schema: FIIDetailsSchema } } },
    500: { content: { 'application/json': { schema: ErrorSchema } }, description: 'Erro' },
  },
});

app.openapi(
  getFIIDetailsRoute,
  createHandler(async (c) => {
    const code = c.req.param('code');
    const data = await fetchFIIDetails(code);
    return { data };
  }, 'fetchFIIDetails') as any
);

// Rota: Indicadores históricos
const getIndicatorsRoute = createRoute({
  method: 'get',
  path: '/{code}/indicators',
  tags: ['Indicadores'],
  summary: 'Indicadores históricos do FII',
  request: { params: FIIParamsSchema },
  responses: {
    200: { description: 'Indicadores do FII', content: { 'application/json': { schema: IndicatorsSchema } } },
    500: { content: { 'application/json': { schema: ErrorSchema } }, description: 'Erro' },
  },
});

app.openapi(
  getIndicatorsRoute,
  createHandler(async (c) => {
    const code = c.req.param('code');
    const data = await fetchFIIDetails(code);
    const indicators = await fetchFIIIndicators(data.id);
    return { data: indicators };
  }, 'fetchFIIIndicators') as any
);

// Rota: Cotações
const getCotationsRoute = createRoute({
  method: 'get',
  path: '/{code}/cotations',
  tags: ['Cotações'],
  summary: 'Cotações do FII',
  request: {
    params: FIIParamsSchema,
    query: CotationsQuerySchema,
  },
  responses: {
    200: { description: 'Cotações do FII', content: { 'application/json': { schema: CotationsSchema } } },
    500: { content: { 'application/json': { schema: ErrorSchema } }, description: 'Erro' },
  },
});

app.openapi(
  getCotationsRoute,
  createHandler(async (c) => {
    const code = c.req.param('code');
    const days = parseInt(c.req.query('days') || '1825');
    const data = await fetchFIIDetails(code);
    const cotations = await fetchFIICotations(data.id, days);
    return { data: cotations };
  }, 'fetchFIICotations') as any
);

// Rota: Dividendos
const getDividendsRoute = createRoute({
  method: 'get',
  path: '/{code}/dividends',
  tags: ['Dividendos'],
  summary: 'Dividendos do FII',
  request: {
    params: FIIParamsSchema,
    query: CotationsQuerySchema,
  },
  responses: {
    200: { description: 'Dividendos do FII', content: { 'application/json': { schema: z.array(DividendItemSchema) } } },
    500: { content: { 'application/json': { schema: ErrorSchema } }, description: 'Erro' },
  },
});

app.openapi(
  getDividendsRoute,
  createHandler(async (c) => {
    const code = c.req.param('code');
    const days = parseInt(c.req.query('days') || '1825');
    const data = await fetchFIIDetails(code);
    const dividends = await fetchDividends(data.id, days);
    return { data: dividends };
  }, 'fetchDividends') as any
);

// Rota: Cotações do Dia
const getCotationsTodayRoute = createRoute({
  method: 'get',
  path: '/{code}/cotations-today',
  tags: ['Cotações'],
  summary: 'Cotações do dia',
  request: { params: FIIParamsSchema },
  responses: {
    200: { description: 'Cotações do dia do FII', content: { 'application/json': { schema: CotationsTodaySchema } } },
    500: { content: { 'application/json': { schema: ErrorSchema } }, description: 'Erro' },
  },
});

app.openapi(
  getCotationsTodayRoute,
  createHandler(async (c) => {
    const code = c.req.param('code');
    const data = await fetchCotationsToday(code);
    return { data };
  }, 'fetchCotationsToday') as any
);

export default app;
