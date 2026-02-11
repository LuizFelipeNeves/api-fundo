import amqplib from 'amqplib';
import { processTelegramUpdate } from './telegram/processor';
import { startPipelineConsumers } from './pipeline/consumer';
import { startCollectorRunner } from './runner/collector-runner';
import { startCronScheduler } from './scheduler/cron';
import { runMigrations } from './migrations/runner';

const telegramQueue = String(process.env.TELEGRAM_QUEUE_NAME || 'telegram.updates').trim() || 'telegram.updates';
const url = String(process.env.RABBITMQ_URL || '').trim();
if (!url) {
  throw new Error('RABBITMQ_URL is required for the worker');
}

const telegramPrefetchRaw = Number.parseInt(process.env.TELEGRAM_QUEUE_PREFETCH || '4', 10);
const telegramPrefetch = Number.isFinite(telegramPrefetchRaw) && telegramPrefetchRaw > 0 ? Math.min(telegramPrefetchRaw, 50) : 4;

async function main() {
  const connection = await amqplib.connect(url);
  await runMigrations();
  const channel = await connection.createChannel();

  await channel.assertQueue(telegramQueue, { durable: true });
  await channel.prefetch(telegramPrefetch);

  await startPipelineConsumers(connection);
  await startCollectorRunner(connection);
  await startCronScheduler(connection);

  const shutdown = async () => {
    try {
      await channel.close();
    } catch {
      // ignore
    }
    try {
      await connection.close();
    } catch {
      // ignore
    }
    process.exit(0);
  };

  process.once('SIGINT', shutdown);
  process.once('SIGTERM', shutdown);

  channel.consume(telegramQueue, async (msg) => {
    if (!msg) return;
    try {
      const payload = JSON.parse(msg.content.toString());
      const update = payload?.update ?? payload;
      await processTelegramUpdate(update);
      channel.ack(msg);
    } catch (err) {
      const message = err instanceof Error ? err.stack || err.message : String(err);
      process.stderr.write(`[telegram-worker] error ${message.replace(/\n/g, '\\n')}\n`);
      channel.nack(msg, false, true);
    }
  });
}

main().catch((err) => {
  const message = err instanceof Error ? err.stack || err.message : String(err);
  process.stderr.write(`[telegram-worker] fatal ${message.replace(/\n/g, '\\n')}\n`);
  process.exit(1);
});
