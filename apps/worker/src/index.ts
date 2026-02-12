import amqplib, { type ConsumeMessage, type ChannelModel } from 'amqplib';
import { processTelegramUpdate } from './telegram/processor';
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
let activeSessionToken = 0;
let runMainInFlight: Promise<void> | null = null;
let restartTimer: ReturnType<typeof setTimeout> | null = null;

function scheduleRestart(delayMs: number) {
  if (isShuttingDown) return;
  if (restartTimer) return;
  restartTimer = setTimeout(() => {
    restartTimer = null;
    runMain();
  }, Math.max(0, delayMs));
}

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
  const sessionToken = Date.now() + Math.floor(Math.random() * 1_000_000);
  activeSessionToken = sessionToken;
  const isActive = () => !isShuttingDown && activeSessionToken === sessionToken;

  connection.on('error', (err: Error) => {
    if (isShuttingDown) return;
    process.stderr.write(`[worker] connection error: ${err.message}\n`);
  });
  connection.on('close', () => {
    if (isShuttingDown) return;
    process.stderr.write('[worker] connection closed, attempting reconnect...\n');
    if (activeSessionToken === sessionToken) activeSessionToken = 0;
  });

  try {
    await runMigrations();
    const channel = await connection.createChannel();
    const handledKey = Symbol.for('worker.telegram.handled');
    const handledDeliveryTags = new Set<number>();

    function isChannelOpen(ch: unknown): boolean {
      const c: any = ch as any;
      if (!c) return false;
      if (c.closed) return false;
      if (c.connection === null) return false;
      return typeof c.ack === 'function' && typeof c.nack === 'function';
    }

    function safeAck(msg: ConsumeMessage) {
      try {
        const tag = msg.fields?.deliveryTag;
        if (typeof tag === 'number') {
          if (handledDeliveryTags.has(tag)) return;
          handledDeliveryTags.add(tag);
        }
        if ((msg as any)[handledKey]) return;
        (msg as any)[handledKey] = 'ack';
        if (isChannelOpen(channel)) {
          channel.ack(msg);
        }
      } catch {
        // ignore
      }
    }

    function safeNack(msg: ConsumeMessage, requeue: boolean) {
      try {
        const tag = msg.fields?.deliveryTag;
        if (typeof tag === 'number') {
          if (handledDeliveryTags.has(tag)) return;
          handledDeliveryTags.add(tag);
        }
        if ((msg as any)[handledKey]) return;
        (msg as any)[handledKey] = requeue ? 'nack_requeue' : 'nack';
        if (isChannelOpen(channel)) {
          channel.nack(msg, false, requeue);
        }
      } catch {
        // ignore
      }
    }

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

    await startCollectorRunner(connection, isActive);
    await startCronScheduler(connection, isActive);

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
      if (!msg) return;
      if (!isActive()) {
        return;
      }
      try {
        const payload = JSON.parse(msg.content.toString());
        const update = payload?.update ?? payload;
        await processTelegramUpdate(update);
        safeAck(msg);
      } catch (err) {
        const message = err instanceof Error ? err.stack || err.message : String(err);
        process.stderr.write(`[telegram-worker] error ${message.replace(/\n/g, '\\n')}\n`);
        safeNack(msg, true);
      }
    }, { noAck: false });

    // Keep the session alive until the AMQP connection closes.
    await new Promise<void>((resolve) => {
      connection.once('close', () => resolve());
    });
  } catch (err) {
    if (activeSessionToken === sessionToken) activeSessionToken = 0;
    try {
      await connection.close();
    } catch {
      // ignore
    }
    throw err;
  }
}

async function runMain() {
  if (isShuttingDown) return;
  if (runMainInFlight) return;
  runMainInFlight = (async () => {
    let connection: ChannelModel | null = null;
    try {
      connection = await connectWithRetry();
      await runWithConnection(connection);
      if (!isShuttingDown) {
        scheduleRestart(1000);
      }
    } catch (err) {
      if (isShuttingDown) return;
      const message = err instanceof Error ? err.stack || err.message : String(err);
      const lower = message.toLowerCase();
      const transient =
        lower.includes('connection closed') ||
        lower.includes('channel ended') ||
        lower.includes('reply is not a function') ||
        lower.includes('invalid frame') ||
        lower.includes('frame size exceeds frame max');
      if (transient) {
        process.stderr.write('[worker] transient connection failure, restarting in 5 seconds...\n');
      } else {
        process.stderr.write(`[telegram-worker] fatal ${message.replace(/\n/g, '\\n')}\n`);
        process.stderr.write('[worker] restarting in 5 seconds...\n');
      }
      if (connection) {
        try {
          await connection.close();
        } catch {
          // ignore
        }
      }
      scheduleRestart(5000);
    }
  })().finally(() => {
    runMainInFlight = null;
  });
}

runMain();
