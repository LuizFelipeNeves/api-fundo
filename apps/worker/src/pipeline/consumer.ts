import type { ChannelModel, ConsumeMessage } from 'amqplib';
import { processPersistRequest } from './processor';
import { normalizeCollectResult } from './normalizers';
import type { PersistRequest, CollectResult } from './messages';
import { setupQueue } from '../queues/rabbit';
import { publishWithRetry } from '../queues/retry';

function isChannelOpen(channel: unknown): boolean {
  return !((channel as { closed?: boolean }).closed);
}

export async function startPipelineConsumers(connection: ChannelModel) {
  const consumeChannel = await connection.createChannel();
  const publishChannel = await connection.createChannel();

  const persistQueue = String(process.env.PERSIST_QUEUE_NAME || 'persistence.write').trim() || 'persistence.write';
  const collectResultsQueue = String(process.env.COLLECT_RESULTS_QUEUE || 'collector.results').trim() || 'collector.results';

  await setupQueue(publishChannel, persistQueue, { dlx: 'pipeline.dlx', dlq: 'pipeline.dlq' });
  await setupQueue(publishChannel, collectResultsQueue, { dlx: 'collector.dlx', dlq: 'collector.dlq' });

  const prefetchRaw = Number.parseInt(process.env.PIPELINE_PREFETCH || '8', 10);
  const prefetch = Number.isFinite(prefetchRaw) && prefetchRaw > 0 ? Math.min(prefetchRaw, 100) : 8;
  await consumeChannel.prefetch(prefetch);

  async function safeAck(msg: ConsumeMessage) {
    try {
      if (isChannelOpen(consumeChannel)) {
        consumeChannel.ack(msg);
      }
    } catch {
      // ignore
    }
  }

  async function safeNack(msg: ConsumeMessage, requeue = false) {
    try {
      if (isChannelOpen(consumeChannel)) {
        consumeChannel.nack(msg, false, requeue);
      }
    } catch {
      // ignore
    }
  }

  consumeChannel.consume(persistQueue, async (msg: ConsumeMessage | null) => {
    if (!msg) return;
    try {
      const payload = JSON.parse(msg.content.toString()) as PersistRequest;
      await processPersistRequest(payload);
      await safeAck(msg);
    } catch (err) {
      const message = err instanceof Error ? err.stack || err.message : String(err);
      process.stderr.write(`[pipeline] persist error ${message.replace(/\n/g, '\\n')}\n`);
      if (!isChannelOpen(consumeChannel)) {
        process.stderr.write('[pipeline] channel closed, discarding message\n');
        return;
      }
      const retryBaseMs = Number.parseInt(process.env.PERSIST_RETRY_BASE_MS || '500', 10);
      const maxRetries = Number.parseInt(process.env.PERSIST_MAX_RETRIES || '6', 10);
      const outcome = await publishWithRetry(publishChannel, persistQueue, msg, {
        maxRetries: Number.isFinite(maxRetries) ? maxRetries : 6,
        retryBaseMs: Number.isFinite(retryBaseMs) ? retryBaseMs : 500,
        retryQueuePrefix: 'persistence.write',
      });
      if (outcome === 'dlq') {
        await safeNack(msg, false);
      } else {
        await safeAck(msg);
      }
    }
  });

  consumeChannel.consume(collectResultsQueue, async (msg: ConsumeMessage | null) => {
    if (!msg) return;
    try {
      const payload = JSON.parse(msg.content.toString()) as CollectResult;
      const persistRequests = normalizeCollectResult(payload);
      if (persistRequests.length === 0) {
        process.stdout.write(`[pipeline] collect-result skipped collector=${payload.collector} fetched_at=${payload.fetched_at}\n`);
        await safeAck(msg);
        return;
      }
      for (const request of persistRequests) {
        const body = Buffer.from(JSON.stringify(request));
        publishChannel.sendToQueue(persistQueue, body, { contentType: 'application/json', persistent: true });
      }
      process.stdout.write(`[pipeline] collect-result published=${persistRequests.length} collector=${payload.collector} fetched_at=${payload.fetched_at}\n`);
      await safeAck(msg);
    } catch (err) {
      const message = err instanceof Error ? err.stack || err.message : String(err);
      process.stderr.write(`[pipeline] collect-result error ${message.replace(/\n/g, '\\n')}\n`);
      if (!isChannelOpen(consumeChannel)) {
        process.stderr.write('[pipeline] channel closed, discarding message\n');
        return;
      }
      const retryBaseMs = Number.parseInt(process.env.RESULTS_RETRY_BASE_MS || '500', 10);
      const maxRetries = Number.parseInt(process.env.RESULTS_MAX_RETRIES || '6', 10);
      const outcome = await publishWithRetry(publishChannel, collectResultsQueue, msg, {
        maxRetries: Number.isFinite(maxRetries) ? maxRetries : 6,
        retryBaseMs: Number.isFinite(retryBaseMs) ? retryBaseMs : 500,
        retryQueuePrefix: 'collector.results',
      });
      if (outcome === 'dlq') {
        await safeNack(msg, false);
      } else {
        await safeAck(msg);
      }
    }
  });

  return consumeChannel;
}
