import { OpenAPIHono } from '@hono/zod-openapi';
import { publishTelegramUpdate } from '../queue/rabbit';

const app = new OpenAPIHono();

app.post('/webhook', async (c) => {
  const secret = (process.env.TELEGRAM_WEBHOOK_SECRET || '').trim();
  if (secret) {
    const header = (c.req.header('x-telegram-bot-api-secret-token') || '').trim();
    if (header !== secret) return c.json({ ok: true });
  }

  const token = (process.env.TELEGRAM_BOT_TOKEN || '').trim();
  if (!token) return c.json({ ok: true });

  const update = await c.req.json();
  await publishTelegramUpdate({ update, received_at: new Date().toISOString() });

  return c.json({ ok: true });
});

export default app;
