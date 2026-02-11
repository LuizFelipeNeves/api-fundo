import amqplib, { type ConsumeMessage, type ChannelModel } from 'amqplib';
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

// Global unhandled rejection handler to prevent crashes
process.on('unhandledRejection', (reason: unknown) => {
  const message = reason instanceof Error ? reason.message : String(reason);
  // Ignore channel-related errors during shutdown/reconnect
  if (message.includes('Channel ended') || message.includes('Channel closed') || message.includes('Connection closed')) {
    return;
  }
  process.stderr.write(`[worker] unhandled rejection: ${message}\n`);
});

let isShuttingDown = false;

async function connectWithRetry(maxRetries = 10, delayMs = 2000): Promise<ChannelModel> {
  let lastError: Error | null = null;
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const heartbeat = Number.parseInt(process.env.RABBITMQ_HEARTBEAT || '60', 10);
      const frameMax = Number.parseInt(process.env.RABBITMQ_FRAME_MAX || '131072', 10);
      return await amqplib.connect(url, { heartbeat, frameMax });
    } catch (err) {
      lastError = err instanceof Error ? err : new Error(String(err));
      process.stderr.write(`[worker] connection attempt ${attempt}/${maxRetries} failed: ${lastError.message}\n`);
      if (attempt < maxRetries) {
        await new Promise(resolve => setTimeout(resolve, delayMs));
      }
    }
  }
  throw lastError;
}

async function runWithConnection(connection: ChannelModel) {
  connection.on('error', (err: Error) => {
    if (isShuttingDown) return;
    process.stderr.write(`[worker] connection error: ${err.message}\n`);
  });
  connection.on('close', () => {
    if (isShuttingDown) return;
    process.stderr.write('[worker] connection closed, attempting reconnect...\n');
    runMain(); // Restart instead of exit
  });

  await runMigrations();
  const channel = await connection.createChannel();

  channel.on('error', (err: Error) => {
    if (isShuttingDown) return;
    process.stderr.write(`[worker] channel error: ${err.message}\n`);
  });
  channel.on('close', () => {
    if (isShuttingDown) return;
    process.stderr.write('[worker] channel closed\n');
  });

  await channel.assertQueue(telegramQueue, { durable: true });
  await channel.prefetch(telegramPrefetch);

  await startPipelineConsumers(connection);
  await startCollectorRunner(connection);
  await startCronScheduler(connection);

  const shutdown = async () => {
    if (isShuttingDown) return;
    isShuttingDown = true;
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

  channel.consume(telegramQueue, async (msg: ConsumeMessage | null) => {
    if (!msg || isShuttingDown) return;
    try {
      const payload = JSON.parse(msg.content.toString());
      const update = payload?.update ?? payload;
      await processTelegramUpdate(update);
      channel.ack(msg);
    } catch (err) {
      const message = err instanceof Error ? err.stack || err.message : String(err);
      process.stderr.write(`[telegram-worker] error ${message.replace(/\n/g, '\\n')}\n`);
      try {
        channel.nack(msg, false, true);
      } catch {
        // ignore if channel closed
      }
    }
  });
}

async function runMain() {
  if (isShuttingDown) return;
  try {
    const connection = await connectWithRetry();
    await runWithConnection(connection);
  } catch (err) {
    if (isShuttingDown) return;
    const message = err instanceof Error ? err.stack || err.message : String(err);
    process.stderr.write(`[telegram-worker] fatal ${message.replace(/\n/g, '\\n')}\n`);
    process.stderr.write('[worker] restarting in 5 seconds...\n');
    setTimeout(runMain, 5000);
  }
}

runMain();
