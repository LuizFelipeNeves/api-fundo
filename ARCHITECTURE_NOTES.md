# Architecture Notes (CQRS + Bun + RabbitMQ + Postgres)

## Objetivo
Manter a API atual sem mudanças de rota/retorno e migrar o pipeline de coleta para um modelo assíncrono com CQRS.

## Princípios
- API = leitura (read side) e **somente consulta** Postgres.
- Workers = escrita/processamento (write side) e consomem filas.
- Coleta e processamento são 100% assíncronos.
- Coletores são plugins internos, sem lógica de fila ou DB.
- Serialização preserva o formato legado.

## Layout (paralelo ao legado)
- `apps/api` → nova API de leitura em Bun.
- `apps/worker` → worker principal (fila + pipeline + cron + telegram).
- `database/read-repositories/schema.sql` → read models
- `database/write-repositories/schema.sql` → canonical write schema

## Fluxos principais

### 1) HTTP → Telegram webhook
1. `apps/api` recebe `/api/telegram/webhook`
2. Enfileira em `telegram.updates` (RabbitMQ)
3. `apps/worker` consome e executa handlers
4. Storage do Telegram grava em Postgres (write side)

### 2) Scheduler → Collector → Pipeline
1. Cron (`apps/worker/src/scheduler/cron.ts`) publica em `collector.requests`
2. Runner (`collector-runner`) executa coletor interno
3. Coletor retorna `collector.results` com `persist_request`
4. Pipeline converte para `persistence.write`
5. Persistência grava no write side e projeta para read models

### 3) API Read Side
- Endpoints seguem formato legado
- `apps/api/src/db/repo.ts` consulta tabelas `*_read`

## Filas e Retry
- Queues: `telegram.updates`, `collector.requests`, `collector.results`, `persistence.write`
- DLQ via `*.dlq` e exchange `*.dlx`
- Retry exponencial com filas `*.retry.<delay>`
- Variáveis:
  - `*_RETRY_BASE_MS`, `*_MAX_RETRIES`

## Migrations
- Worker executa migrations automaticamente ao subir:
  - `apps/worker/src/migrations/runner.ts`
  - Aplica `database/write-repositories/schema.sql` e `database/read-repositories/schema.sql`

## Estado de sincronização
- Tabela `fund_state` controla últimas execuções:
  - `last_details_sync_at`
  - `last_indicators_at`
  - `last_cotations_today_at`
  - `last_historical_cotations_at`
  - `last_documents_at`

Cron usa esses timestamps para reduzir tráfego.

## Dividend Yield via Cotation
- Persistência calcula yield apenas se não existir no DB
- Usa `cotation.price` do mesmo `fund_code` e `date_iso`

## Tecnologias
- Bun (API + worker)
- RabbitMQ (fila, DLQ, retry)
- PostgreSQL (write side + read models)
- Hono (API)
- Drizzle (legado)

## Decisões chave
- API nunca escreve em DB
- Coletores não conhecem filas/DB
- Persistência é única responsável por writes + projeções
- Paralelismo com legado: nada foi removido
