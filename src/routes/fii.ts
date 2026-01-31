import { OpenAPIHono, createRoute, z } from '@hono/zod-openapi';
import { createHandler } from '../helpers';
import { getDb } from '../db';
import {
  getCotations,
  getDividends,
  getDocuments,
  getFundDetails,
  getLatestCotationsToday,
  getLatestIndicators,
  listFunds,
} from '../db/repo';
import {
  ErrorSchema,
  FIIParamsSchema,
  FII_CODE_REGEX,
  CotationsQuerySchema,
  FIIResponseSchema,
  FIIDetailsSchema,
  CotationsSchema,
  DividendItemSchema,
  IndicatorsSchema,
  CotationsTodaySchema,
  DocumentItemSchema,
} from '../openapi-schemas';

const INVALID_CODE_RESPONSE = {
  error: 'Código inválido',
  message: 'Código deve ter formato XXXX11 (4 letras + 11)',
  example: 'binc11',
};

function getValidatedCode(c: any): { valid: boolean; code?: string } {
  const codeRaw = c.req.param('code');
  if (!codeRaw || !FII_CODE_REGEX.test(codeRaw)) {
    return { valid: false };
  }
  return { valid: true, code: codeRaw.toUpperCase() };
}

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
    const data = listFunds(getDb());
    return { data };
  }, 'listFunds') as any
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
    400: { description: 'Código inválido' },
    404: { description: 'FII não encontrado' },
    500: { content: { 'application/json': { schema: ErrorSchema } }, description: 'Erro' },
  },
});

app.openapi(
  getFIIDetailsRoute,
  createHandler(async (c: any) => {
    const { valid, code } = getValidatedCode(c);
    if (!valid || !code) {
      return c.json(INVALID_CODE_RESPONSE, 400);
    }
    const data = getFundDetails(getDb(), code);
    if (!data) {
      return c.json({ error: 'FII não encontrado' }, 404);
    }
    return { data };
  }, 'getFundDetails') as any
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
    400: { description: 'Código inválido' },
    404: { description: 'FII não encontrado' },
    500: { content: { 'application/json': { schema: ErrorSchema } }, description: 'Erro' },
  },
});

app.openapi(
  getIndicatorsRoute,
  createHandler(async (c: any) => {
    const { valid, code } = getValidatedCode(c);
    if (!valid || !code) {
      return c.json(INVALID_CODE_RESPONSE, 400);
    }
    const indicators = getLatestIndicators(getDb(), code);
    if (!indicators) {
      return c.json({ error: 'FII não encontrado' }, 404);
    }
    return { data: indicators };
  }, 'getLatestIndicators') as any
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
    400: { description: 'Código inválido' },
    404: { description: 'FII não encontrado' },
    500: { content: { 'application/json': { schema: ErrorSchema } }, description: 'Erro' },
  },
});

app.openapi(
  getCotationsRoute,
  createHandler(async (c: any) => {
    const { valid, code } = getValidatedCode(c);
    if (!valid || !code) {
      return c.json(INVALID_CODE_RESPONSE, 400);
    }
    const days = parseInt(c.req.query('days') || '1825');
    const cotations = getCotations(getDb(), code, days);
    if (!cotations) {
      return c.json({ error: 'FII não encontrado' }, 404);
    }
    return { data: cotations };
  }, 'getCotations') as any
);

// Rota: Dividendos
const getDividendsRoute = createRoute({
  method: 'get',
  path: '/{code}/dividends',
  tags: ['Dividendos'],
  summary: 'Dividendos e amortizações do FII',
  request: {
    params: FIIParamsSchema,
  },
  responses: {
    200: { description: 'Dividendos e amortizações do FII', content: { 'application/json': { schema: z.array(DividendItemSchema) } } },
    400: { description: 'Código inválido' },
    404: { description: 'FII não encontrado' },
    500: { content: { 'application/json': { schema: ErrorSchema } }, description: 'Erro' },
  },
});

app.openapi(
  getDividendsRoute,
  createHandler(async (c: any) => {
    const { valid, code } = getValidatedCode(c);
    if (!valid || !code) {
      return c.json(INVALID_CODE_RESPONSE, 400);
    }
    const dividends = getDividends(getDb(), code);
    if (!dividends) {
      return c.json({ error: 'FII não encontrado' }, 404);
    }
    return { data: dividends };
  }, 'getDividends') as any
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
    400: { description: 'Código inválido' },
    404: { description: 'FII não encontrado' },
    500: { content: { 'application/json': { schema: ErrorSchema } }, description: 'Erro' },
  },
});

app.openapi(
  getCotationsTodayRoute,
  createHandler(async (c: any) => {
    const { valid, code } = getValidatedCode(c);
    if (!valid || !code) {
      return c.json(INVALID_CODE_RESPONSE, 400);
    }
    const data = getLatestCotationsToday(getDb(), code);
    if (!data) {
      return c.json({ error: 'FII não encontrado' }, 404);
    }
    return { data };
  }, 'getLatestCotationsToday') as any
);

// Rota: Documentos
const getDocumentsRoute = createRoute({
  method: 'get',
  path: '/{code}/documents',
  tags: ['Documentos'],
  summary: 'Documentos do FII (FNET)',
  request: { params: FIIParamsSchema },
  responses: {
    200: { description: 'Documentos do FII', content: { 'application/json': { schema: z.array(DocumentItemSchema) } } },
    400: { description: 'Código inválido' },
    404: { description: 'FII não encontrado' },
    500: { content: { 'application/json': { schema: ErrorSchema } }, description: 'Erro' },
  },
});

app.openapi(
  getDocumentsRoute,
  createHandler(async (c: any) => {
    const { valid, code } = getValidatedCode(c);
    if (!valid || !code) {
      return c.json(INVALID_CODE_RESPONSE, 400);
    }
    const documents = getDocuments(getDb(), code);
    if (!documents) {
      return c.json({ error: 'FII não encontrado' }, 404);
    }
    return { data: documents };
  }, 'getDocuments') as any
);

export default app;
