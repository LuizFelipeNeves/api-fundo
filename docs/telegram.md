# Telegram

## Webhook

O receiver está em:

- `POST /api/telegram/webhook/{token}` (quando `TELEGRAM_WEBHOOK_TOKEN` estiver definido)
- `POST /api/telegram/webhook` (quando `TELEGRAM_WEBHOOK_TOKEN` estiver vazio)

### Segurança

- `TELEGRAM_WEBHOOK_TOKEN`: valida o `{token}` do path.

### URL de exemplo (startup)

Ao iniciar, a API imprime algo como:

`telegram_webhook_example_url=http://localhost:8080/api/telegram/webhook/<TOKEN>`

O host é configurado por `API_ENDPOINT` (default `http://localhost:8080`).

### Configurar o Webhook do Telegram

- Configure o webhook do Telegram para o endpoint `/api/telegram/webhook` da sua API.
- Se você usa `TELEGRAM_WEBHOOK_TOKEN`, inclua `/{token}` no final do endpoint.
- Substitua `<SEU_TOKEN_DO_BOTFATHER>` pelo token do seu bot.
- Substitua `https://decide-functioning-strengthen-cordless.trycloudflare.com` pela URL pública da sua API.

```bash
https://api.telegram.org/bot<SEU_TOKEN_DO_BOTFATHER>/setWebhook?url=https://monthly-net-appear-specialists.trycloudflare.com/api/telegram/webhook
```

## Comandos suportados

- `/lista`
- `/set CODE1 CODE2 ...`
- `/add CODE1 CODE2 ...`
- `/remove CODE1 CODE2 ...`
- `/documentos [CODE] [LIMITE]`
- `/rank hoje [CODE1 CODE2 ...]`
- `/rankv [CODE1 CODE2 ...]`
