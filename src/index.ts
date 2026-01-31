import { OpenAPIHono } from '@hono/zod-openapi';
import fiiRouter from './routes/fii';

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

// Mount FII router
app.route('/api/fii', fiiRouter);

export default {
  port: 3000,
  fetch: app.fetch,
};
