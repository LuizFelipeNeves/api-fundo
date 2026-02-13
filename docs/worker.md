# go-worker

Worker de coleta que escreve no Postgres e controla agendamento pela tabela `fund_state` (sem filas).

## Fluxo

`collect → normalize → persist → update fund_state`

## Modos

- `WORKER_MODE=normal` (default): roda continuamente, respeitando janelas/horários.
- `WORKER_MODE=backfill`: preenche o banco em etapas e finaliza.

## Agendamento (normal)

- `fund_details`, `cotations_today`, `documents`: dias úteis 10:00–18:30 (America/Sao_Paulo), a partir do `fund_state` + intervalos.
- `fund_list` e `indicators`: dias úteis apenas nas janelas 09:00–09:10 e 19:00–19:10.
- EOD cotation: dias úteis 19:00–19:10 (1x/dia por lock transacional no Postgres).

## Backfill (ordem)

1) `fund_list`
2) `fund_details` + `cotations_today`
3) `documents` + `cotations`
4) recomputa `dividend.yield` via join em `cotation`
5) `indicators`

## Variáveis de ambiente

- `DATABASE_URL`
- `WORKER_MODE` (`normal|backfill`)
- `WORKER_POOL_SIZE`
- `SCHEDULER_INTERVAL_MS`
- `BATCH_SIZE`
- `FORCE_RUN_JOBS=true` (ignora janelas/horários)
- `INTERVAL_FUND_LIST_MIN`
- `INTERVAL_FUND_DETAILS_MIN`
- `INTERVAL_COTATIONS_TODAY_MIN`
- `INTERVAL_INDICATORS_MIN`
- `INTERVAL_DOCUMENTS_MIN`

