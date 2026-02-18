# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Visão geral

- **go-worker**: coleta dados (Investidor10 + FNET) e persiste no Postgres.
- **go-api**: API HTTP read-only e webhook do Telegram.
- **Go**: `1.22`

## Comandos úteis

```bash
# Build e run
cd go-worker && go build -o bin/worker ./cmd/worker && ./bin/worker
cd go-api && go build -o bin/api ./cmd/api && ./bin/api

# Tests
cd go-worker && go test ./...          # todos os testes
cd go-worker && go test -v ./internal/collectors -run TestName
cd go-api && go test ./...

# Lint
cd go-worker && go vet ./...
cd go-api && go vet ./...
```

## Docker

```bash
docker-compose up -d postgres
docker-compose up -d go-worker
docker-compose up -d go-api
# Acessar: http://localhost:8080/docs/
```

## Estrutura

```
go-api/
  cmd/api/main.go
  internal/httpapi/    # rotas + swagger
  internal/telegram/   # bot e webhook
  internal/fii/        # queries e export agregado

go-worker/
  cmd/worker/main.go
  internal/scheduler/  # normal/backfill e janelas
  internal/collectors/ # coletores por fonte (investidor10, fnet)
  internal/parsers/    # normalização de dados
  internal/persistence/ # writes e fund_state
  internal/httpclient/ # clients externos
  internal/db/         # queries e helpers
```

## Agendamento (go-worker)

| Job | Janela (America/Sao_Paulo) |
|-----|---------------------------|
| fund_details, cotations_today, documents | Dias úteis 10:00–18:30 |
| fund_list, indicators | Dias úteis 09:00–09:10 e 19:00–19:10 |
| EOD cotation | Dias úteis 19:00–19:10 (1x/dia, lock transacional) |

**Modos**: `WORKER_MODE=normal` (default, contínuo) ou `backfill` (preenche e encerra).

## Pontos de atenção

- Webhook do Telegram faz ack rápido e processa em background.
- Comandos de rank podem ser caros (`ExportFund` por fundo).
- `fund_state` controla agendamento incremental (sem filas Redis).
- EOD cotation usa lock transacional no Postgres para garantir 1x/dia.
