import type amqplib from 'amqplib';
import { findCollector } from '../collectors/registry';
import type { CollectRequest, CollectResult, CollectorContext } from '../collectors/types';
import { getJson, getText, postForm } from '../http/client';
import { setupQueue } from '../queues/rabbit';
import { publishWithRetry } from '../queues/retry';

const requestQueue = String(process.env.COLLECT_REQUESTS_QUEUE || 'collector.requests').trim() || 'collector.requests';
const resultsQueue = String(process.env.COLLECT_RESULTS_QUEUE || 'collector.results').trim() || 'collector.results';

export async function startCollectorRunner(connection: amqplib.Connection) {
  const channel = await connection.createChannel();
  await setupQueue(channel, requestQueue, { dlx: 'collector.dlx', dlq: 'collector.dlq' });
  await setupQueue(channel, resultsQueue, { dlx: 'collector.dlx', dlq: 'collector.dlq' });

  const prefetchRaw = Number.parseInt(process.env.COLLECTOR_PREFETCH || '6', 10);
  const prefetch = Number.isFinite(prefetchRaw) && prefetchRaw > 0 ? Math.min(prefetchRaw, 50) : 6;
  await channel.prefetch(prefetch);

  const ctx: CollectorContext = {
    http: { getJson, getText, postForm },
  };

  channel.consume(requestQueue, async (msg) => {
    if (!msg) return;
    let request: CollectRequest | null = null;
    try {
      request = JSON.parse(msg.content.toString()) as CollectRequest;
      const collector = request?.collector ? findCollector(request.collector) : null;
      if (!collector || !collector.supports(request)) {
        channel.ack(msg);
        return;
      }

      const result = await collector.collect(request, ctx);
      const body = Buffer.from(JSON.stringify(result satisfies CollectResult));
      channel.sendToQueue(resultsQueue, body, { contentType: 'application/json', persistent: true });
      channel.ack(msg);
    } catch (err) {
      const message = err instanceof Error ? err.stack || err.message : String(err);
      process.stderr.write(`[collector-runner] error ${message.replace(/\n/g, '\\n')}\n`);
      const retryBaseMs = Number.parseInt(process.env.COLLECTOR_RETRY_BASE_MS || '500', 10);
      const maxRetries = Number.parseInt(process.env.COLLECTOR_MAX_RETRIES || '5', 10);
      const outcome = await publishWithRetry(channel, requestQueue, msg, {
        maxRetries: Number.isFinite(maxRetries) ? maxRetries : 5,
        retryBaseMs: Number.isFinite(retryBaseMs) ? retryBaseMs : 500,
        retryQueuePrefix: 'collector.requests',
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
