import { OpenAPIHono, createRoute, z } from '@hono/zod-openapi';
import {
  fetchFIIList,
  fetchFIIDetails,
  fetchFIIIndicators,
  fetchFIICotations,
  fetchFIIDividends,
  fetchFIIDividendYield,
} from './client';

// Define OpenAPI schema
const FIIParamsSchema = z.object({
  code: z.string().openapi({ param: { name: 'code', in: 'path', required: true }, example: 'binc11' }),
  id: z.string().openapi({ param: { name: 'id', in: 'path', required: true }, example: '631' }),
});

const CotationsQuerySchema = z.object({
  days: z.string().optional().openapi({ param: { name: 'days', in: 'query' }, example: '1825' }),
});

const ErrorSchema = z.object({
  error: z.string().openapi({ example: 'Failed to fetch FII list' }),
});

// Create OpenAPI app
const app = new OpenAPIHono();

// OpenAPI metadata
app.doc('/openapi.json', {
  openapi: '3.1.0',
  info: {
    title: 'FII API',
    version: '1.0.0',
    description: 'API para consultar dados de Fundos de Investimento Imobiliário',
  },
});

// Swagger UI
app.get('/swagger', (c) => {
  return c.html(`
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0">
      <title>FII API - Swagger UI</title>
      <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5.11.0/swagger-ui.css">
    </head>
    <body>
      <div id="swagger-ui"></div>
      <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5.11.0/swagger-ui-bundle.js"></script>
      <script>
        SwaggerUIBundle({
          url: '/openapi.json',
          dom_id: '#swagger-ui',
        });
      </script>
    </body>
    </html>
  `);
});

// Rota: Lista todos os FIIs
const listFIIRoute = createRoute({
  method: 'get',
  path: '/api/fii',
  tags: ['FII'],
  summary: 'Lista todos os FIIs',
  responses: {
    200: { description: 'Lista de FIIs' },
    500: { content: { 'application/json': { schema: ErrorSchema } }, description: 'Erro' },
  },
});

app.openapi(listFIIRoute, async (c) => {
  try {
    const data = await fetchFIIList();
    return c.json(data);
  } catch (error) {
    console.log(error);
    return c.json({ error: 'Failed to fetch FII list' }, 500);
  }
});

// Rota: Dados básicos do FII
const getFIIDetailsRoute = createRoute({
  method: 'get',
  path: '/api/fii/{code}',
  tags: ['FII'],
  summary: 'Dados básicos do FII',
  request: { params: FIIParamsSchema },
  responses: {
    200: { description: 'HTML com dados do FII' },
    500: { content: { 'application/json': { schema: ErrorSchema } }, description: 'Erro' },
  },
});

app.openapi(getFIIDetailsRoute, async (c) => {
  const code = c.req.param('code');
  try {
    const data = await fetchFIIDetails(code);
    return c.text(data);
  } catch (error) {
    return c.json({ error: 'Failed to fetch FII details' }, 500);
  }
});

// Rota: Indicadores históricos
const getIndicatorsRoute = createRoute({
  method: 'get',
  path: '/api/fii/{id}/indicators',
  tags: ['Indicadores'],
  summary: 'Indicadores históricos do FII',
  request: { params: FIIParamsSchema },
  responses: {
    200: { description: 'Indicadores do FII' },
    500: { content: { 'application/json': { schema: ErrorSchema } }, description: 'Erro' },
  },
});

app.openapi(getIndicatorsRoute, async (c) => {
  const id = c.req.param('id');
  try {
    const data = await fetchFIIIndicators(id);
    return c.json(data);
  } catch (error) {
    return c.json({ error: 'Failed to fetch FII indicators' }, 500);
  }
});

// Rota: Cotações
const getCotationsRoute = createRoute({
  method: 'get',
  path: '/api/fii/{id}/cotations',
  tags: ['Cotações'],
  summary: 'Cotações do FII',
  request: {
    params: FIIParamsSchema,
    query: CotationsQuerySchema,
  },
  responses: {
    200: { description: 'Cotações do FII' },
    500: { content: { 'application/json': { schema: ErrorSchema } }, description: 'Erro' },
  },
});

app.openapi(getCotationsRoute, async (c) => {
  const id = c.req.param('id');
  const days = c.req.query('days') || '1825';
  try {
    const data = await fetchFIICotations(id, parseInt(days));
    return c.json(data);
  } catch (error) {
    return c.json({ error: 'Failed to fetch FII cotations' }, 500);
  }
});

// Rota: Dividendos
const getDividendsRoute = createRoute({
  method: 'get',
  path: '/api/fii/{id}/dividends',
  tags: ['Dividendos'],
  summary: 'Dividendos do FII',
  request: {
    params: FIIParamsSchema,
    query: CotationsQuerySchema,
  },
  responses: {
    200: { description: 'Dividendos do FII' },
    500: { content: { 'application/json': { schema: ErrorSchema } }, description: 'Erro' },
  },
});

app.openapi(getDividendsRoute, async (c) => {
  const id = c.req.param('id');
  const days = c.req.query('days') || '1825';
  try {
    const data = await fetchFIIDividends(id, parseInt(days));
    return c.json(data);
  } catch (error) {
    return c.json({ error: 'Failed to fetch FII dividends' }, 500);
  }
});

// Rota: Dividend Yield
const getDividendYieldRoute = createRoute({
  method: 'get',
  path: '/api/fii/{id}/dividend-yield',
  tags: ['Dividendos'],
  summary: 'Dividend Yield do FII',
  request: {
    params: FIIParamsSchema,
    query: CotationsQuerySchema,
  },
  responses: {
    200: { description: 'Dividend Yield do FII' },
    500: { content: { 'application/json': { schema: ErrorSchema } }, description: 'Erro' },
  },
});

app.openapi(getDividendYieldRoute, async (c) => {
  const id = c.req.param('id');
  const days = c.req.query('days') || '1825';
  try {
    const data = await fetchFIIDividendYield(id, parseInt(days));
    return c.json(data);
  } catch (error) {
    return c.json({ error: 'Failed to fetch FII dividend yield' }, 500);
  }
});

export default {
  port: 3000,
  fetch: app.fetch,
};
