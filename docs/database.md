# Banco de dados (PostgreSQL)

O schema está em [database/schema.sql](../database/schema.sql).

## Tabelas principais

- `fund_master`: dados do fundo.
- `fund_state`: timestamps/estado para agendamento incremental.
- `indicators_snapshot`: último snapshot de indicadores (1 por fundo).
- `cotation_today`: série intraday por data/hora.
- `cotation`: histórico diário (BRL).
- `dividend`: dividendos e amortizações.
- `document`: documentos da CVM/FNET.
- `telegram_*`: usuários, lista de fundos e ações pendentes.

## Como subir

- Suba o Postgres via `docker-compose` e aplique o schema uma vez.
- `go-worker` popula/atualiza os dados.
- `go-api` apenas lê.
