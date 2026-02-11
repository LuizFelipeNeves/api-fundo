import type amqplib from 'amqplib';
import { processPersistRequest } from './processor';
import { normalizeCollectResult } from './normalizers';
import type { PersistRequest, CollectResult } from './messages';
import { setupQueue } from '../queues/rabbit';
import { publishWithRetry } from '../queues/retry';

export async function startPipelineConsumers(connection: amqplib.Connection) {
  const channel = await connection.createChannel();

  const persistQueue = String(process.env.PERSIST_QUEUE_NAME || 'persistence.write').trim() || 'persistence.write';
  const collectResultsQueue = String(process.env.COLLECT_RESULTS_QUEUE || 'collector.results').trim() || 'collector.results';

  await setupQueue(channel, persistQueue, { dlx: 'pipeline.dlx', dlq: 'pipeline.dlq' });
  await setupQueue(channel, collectResultsQueue, { dlx: 'collector.dlx', dlq: 'collector.dlq' });

  const prefetchRaw = Number.parseInt(process.env.PIPELINE_PREFETCH || '8', 10);
  const prefetch = Number.isFinite(prefetchRaw) && prefetchRaw > 0 ? Math.min(prefetchRaw, 100) : 8;
  await channel.prefetch(prefetch);

  channel.consume(persistQueue, async (msg) => {
    if (!msg) return;
    try {
      const payload = JSON.parse(msg.content.toString()) as PersistRequest;
      await processPersistRequest(payload);
      channel.ack(msg);
    } catch (err) {
      const message = err instanceof Error ? err.stack || err.message : String(err);
      process.stderr.write(`[pipeline] persist error ${message.replace(/\n/g, '\\n')}\n`);
      const retryBaseMs = Number.parseInt(process.env.PERSIST_RETRY_BASE_MS || '500', 10);
      const maxRetries = Number.parseInt(process.env.PERSIST_MAX_RETRIES || '6', 10);
      const outcome = await publishWithRetry(channel, persistQueue, msg, {
        maxRetries: Number.isFinite(maxRetries) ? maxRetries : 6,
        retryBaseMs: Number.isFinite(retryBaseMs) ? retryBaseMs : 500,
        retryQueuePrefix: 'persistence.write',
      });
      if (outcome === 'dlq') {
        channel.nack(msg, false, false);
      } else {
        channel.ack(msg);
      }
    }
  });

  channel.consume(collectResultsQueue, async (msg) => {
    if (!msg) return;
    try {
      const payload = JSON.parse(msg.content.toString()) as CollectResult;
      const persistRequests = normalizeCollectResult(payload);
      if (persistRequests.length === 0) {
        process.stdout.write(`[pipeline] collect-result skipped collector=${payload.collector} fetched_at=${payload.fetched_at}\n`);
        channel.ack(msg);
        return;
      }
      for (const request of persistRequests) {
        const body = Buffer.from(JSON.stringify(request));
        channel.sendToQueue(persistQueue, body, { contentType: 'application/json', persistent: true });
      }
      process.stdout.write(`[pipeline] collect-result published=${persistRequests.length} collector=${payload.collector} fetched_at=${payload.fetched_at}\n`);
      channel.ack(msg);
    } catch (err) {
      const message = err instanceof Error ? err.stack || err.message : String(err);
      process.stderr.write(`[pipeline] collect-result error ${message.replace(/\n/g, '\\n')}\n`);
      const retryBaseMs = Number.parseInt(process.env.RESULTS_RETRY_BASE_MS || '500', 10);
      const maxRetries = Number.parseInt(process.env.RESULTS_MAX_RETRIES || '6', 10);
      const outcome = await publishWithRetry(channel, collectResultsQueue, msg, {
        maxRetries: Number.isFinite(maxRetries) ? maxRetries : 6,
        retryBaseMs: Number.isFinite(retryBaseMs) ? retryBaseMs : 500,
        retryQueuePrefix: 'collector.results',
      });
      if (outcome === 'dlq') {
        channel.nack(msg, false, false);
      } else {
        channel.ack(msg);
      }
    }
  });

  return channel;
}
