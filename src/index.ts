import { Hono } from 'hono';
import {
  fetchFIIList,
  fetchFIIDetails,
  fetchFIIIndicators,
  fetchFIICotations,
  fetchFIIDividends,
  fetchFIIDividendYield,
} from './client';

const app = new Hono();

// Lista todos os FIIs
app.get('/api/fii', async (c) => {
  try {
    const data = await fetchFIIList();
    return c.json(data);
  } catch (error) {
    return c.json({ error: 'Failed to fetch FII list' }, 500);
  }
});

// Dados básicos do FII (HTML)
app.get('/api/fii/:code', async (c) => {
  const code = c.req.param('code');
  try {
    const data = await fetchFIIDetails(code);
    return c.text(data);
  } catch (error) {
    return c.json({ error: 'Failed to fetch FII details' }, 500);
  }
});

// Indicadores históricos do FII
app.get('/api/fii/:id/indicators', async (c) => {
  const id = c.req.param('id');
  try {
    const data = await fetchFIIIndicators(id);
    return c.json(data);
  } catch (error) {
    return c.json({ error: 'Failed to fetch FII indicators' }, 500);
  }
});

// Cotações do FII
app.get('/api/fii/:id/cotations', async (c) => {
  const id = c.req.param('id');
  const days = c.req.query('days') || '1825';
  try {
    const data = await fetchFIICotations(id, parseInt(days));
    return c.json(data);
  } catch (error) {
    return c.json({ error: 'Failed to fetch FII cotations' }, 500);
  }
});

// Dividendos do FII
app.get('/api/fii/:id/dividends', async (c) => {
  const id = c.req.param('id');
  const days = c.req.query('days') || '1825';
  try {
    const data = await fetchFIIDividends(id, parseInt(days));
    return c.json(data);
  } catch (error) {
    return c.json({ error: 'Failed to fetch FII dividends' }, 500);
  }
});

// Dividend Yield do FII
app.get('/api/fii/:id/dividend-yield', async (c) => {
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
