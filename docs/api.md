# go-api

API HTTP de leitura para dados de FIIs, com Swagger em `/docs/`.

## Rotas

- `GET /` → redireciona para `/docs/`
- `GET /docs/` → Swagger UI
- `GET /openapi.json` → OpenAPI

### FIIs

- `GET /api/fii/` → lista fundos
- `GET /api/fii/{code}` → detalhes do fundo
- `GET /api/fii/{code}/indicators` → último snapshot de indicadores
- `GET /api/fii/{code}/cotations?days=1825` → cotações históricas (limite 5000)
- `GET /api/fii/{code}/cotations-today` → snapshot intraday
- `GET /api/fii/{code}/dividends` → dividendos
- `GET /api/fii/{code}/documents` → documentos
- `GET /api/fii/{code}/export?cotationsDays=1825&indicatorsSnapshotsLimit=365` → export agregado

## Códigos (uppercase)

- O `code` aceito nas rotas é case-insensitive, mas a API sempre normaliza e retorna em uppercase (ex: `binc11` → `BINC11`).

## Variáveis de ambiente

- `PORT` (default `8080`)
- `DATABASE_URL` (Postgres)
- `PG_POOL_MAX` (default `2`)
- `LOG_REQUESTS` (default `1`)
- `TELEGRAM_BOT_TOKEN` (opcional, para o bot responder)
- `TELEGRAM_WEBHOOK_TOKEN` (opcional, protege a rota do webhook via path)
- `API_ENDPOINT` (default `http://localhost:8080`, usado só para imprimir URL de exemplo)

