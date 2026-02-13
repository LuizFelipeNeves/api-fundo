# CLAUDE.md

Este arquivo é voltado para ferramentas de assistência (Claude Code) ao trabalhar neste repositório.

## Visão geral

- `go-worker`: coleta dados (Investidor10 + FNET) e persiste no Postgres.
- `go-api`: API HTTP read-only e webhook do Telegram.

## Comandos úteis

```bash
cd go-worker && go test ./... && go vet ./...
cd go-api && go test ./... && go vet ./...
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
  internal/collectors/ # coletores por fonte
  internal/persistence/# writes e fund_state
```

## Pontos de atenção

- O webhook do Telegram faz ack rápido e processa em background.
- Comandos de rank podem ser caros (`ExportFund` por fundo).
