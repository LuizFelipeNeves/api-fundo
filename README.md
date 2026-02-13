# api-fundo2

Monorepo com:

- **go-worker**: coleta dados e escreve no Postgres.
- **go-api**: API HTTP read-only para consulta e bot do Telegram (webhook).

## Como subir (docker-compose)

```bash
docker-compose up -d postgres
docker-compose up -d go-worker
docker-compose up -d go-api
```

Acesse:

- http://localhost:8080/docs/

## Documentação

- [docs/README.md](docs/README.md)
