import { OpenAPIHono } from '@hono/zod-openapi';
import { serve } from '@hono/node-server';
import { pathToFileURL } from 'node:url';
import fiiRouter from './routes/fii';
import telegramRouter from './routes/telegram';
import { errorToMeta, logger } from './helpers';

const app = new OpenAPIHono();
const port = Number.parseInt(process.env.PORT || '8080', 10);

const logRequests = String(process.env.LOG_REQUESTS ?? '1').trim() !== '0';

app.use('*', async (c, next) => {
  if (!logRequests) return next();
  const startedAt = Date.now();
  try {
    await next();
  } finally {
    const durationMs = Date.now() - startedAt;
    const status = c.res.status;
    const meta = { method: c.req.method, path: c.req.path, status, duration_ms: durationMs };
    if (status >= 500) logger.error('http', meta);
    else if (status >= 400) logger.warn('http', meta);
    else logger.info('http', meta);
  }
});

app.onError((err, c) => {
  logger.error('unhandled_error', { method: c.req.method, path: c.req.path, err: errorToMeta(err) });
  return c.json({ error: 'internal_error' }, 500);
});

app.doc('/openapi.json', {
  openapi: '3.1.0',
  info: {
    title: 'FII API',
    version: '1.0.0',
    description: 'API para consultar dados de Fundos de Investimento Imobiliário',
  },
});

// Swagger UI
app.get('/', (c) => {
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

app.route('/api/fii', fiiRouter);
app.route('/api/telegram', telegramRouter);

export { app, port };

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint && import.meta.url === entrypoint) {
  process.on('unhandledRejection', (reason) => {
    logger.error('unhandled_rejection', { err: errorToMeta(reason) });
  });
  process.on('uncaughtException', (err) => {
    logger.error('uncaught_exception', { err: errorToMeta(err) });
  });
  logger.info('server_start', { port, log_requests: logRequests, log_level: process.env.LOG_LEVEL ?? 'info', log_format: process.env.LOG_FORMAT ?? 'text' });
  serve({ fetch: app.fetch, port });
}
