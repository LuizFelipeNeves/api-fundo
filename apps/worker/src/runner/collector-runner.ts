import type { ChannelModel, ConsumeMessage } from 'amqplib';
import { findCollector } from '../collectors/registry';
import type { CollectRequest, CollectorContext } from '../collectors/types';
import { getJson, getText, postForm } from '../http/client';
import { setupQueue } from '../queues/rabbit';
import { publishWithRetry } from '../queues/retry';

const requestQueue = String(process.env.COLLECT_REQUESTS_QUEUE || 'collector.requests').trim() || 'collector.requests';
const resultsQueue = String(process.env.COLLECT_RESULTS_QUEUE || 'collector.results').trim() || 'collector.results';

function isChannelOpen(channel: unknown): boolean {
  const ch: any = channel as any;
  if (!ch) return false;
  if (ch.closed) return false;
  if (ch.connection === null) return false;
  return typeof ch.ack === 'function' && typeof ch.nack === 'function';
}

function withTimeout<T>(promise: Promise<T>, timeoutMs: number, label: string): Promise<T> {
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) return promise;
  return new Promise<T>((resolve, reject) => {
    const tid = setTimeout(() => reject(new Error(`timeout label=${label} after_ms=${timeoutMs}`)), timeoutMs);
    promise.then(
      (v) => {
        clearTimeout(tid);
        resolve(v);
      },
      (e) => {
        clearTimeout(tid);
        reject(e);
      }
    );
  });
}

export async function startCollectorRunner(connection: ChannelModel, isActive: () => boolean = () => true) {
  const consumeChannel = await connection.createChannel();
  const publishChannel = await connection.createChannel();
  const handledKey = Symbol.for('worker.collector.handled');

  await setupQueue(publishChannel, requestQueue, { dlx: 'collector.dlx', dlq: 'collector.dlq' });
  await setupQueue(publishChannel, resultsQueue, { dlx: 'collector.dlx', dlq: 'collector.dlq' });

  const prefetchRaw = Number.parseInt(process.env.COLLECTOR_PREFETCH || '6', 10);
  const prefetch = Number.isFinite(prefetchRaw) && prefetchRaw > 0 ? Math.min(prefetchRaw, 50) : 6;
  await consumeChannel.prefetch(prefetch);

  const requestTimeoutRaw = Number.parseInt(process.env.COLLECTOR_MESSAGE_TIMEOUT_MS || '180000', 10);
  const requestTimeoutMs = Number.isFinite(requestTimeoutRaw) && requestTimeoutRaw > 0 ? requestTimeoutRaw : 180000;

  async function safeAck(msg: ConsumeMessage) {
    try {
      if ((msg as any)[handledKey]) return;
      (msg as any)[handledKey] = 'ack';
      if (isChannelOpen(consumeChannel)) {
        consumeChannel.ack(msg);
      }
    } catch {
      // ignore
    }
  }

  async function safeNack(msg: ConsumeMessage, requeue = false) {
    try {
      if ((msg as any)[handledKey]) return;
      (msg as any)[handledKey] = requeue ? 'nack_requeue' : 'nack';
      if (isChannelOpen(consumeChannel)) {
        consumeChannel.nack(msg, false, requeue);
      }
    } catch {
      // ignore
    }
  }

  const ctx: CollectorContext = {
    http: { getJson, getText, postForm },
    publish(queue: string, body: Buffer) {
      try {
        if (isChannelOpen(publishChannel)) {
          publishChannel.sendToQueue(queue, body, { contentType: 'application/json', persistent: true });
        }
      } catch {
        // ignore
      }
    },
  };

  consumeChannel.consume(requestQueue, async (msg: ConsumeMessage | null) => {
    if (!msg) return;
    if (!isActive()) {
      await safeNack(msg, true);
      return;
    }
    let request: CollectRequest | null = null;
    try {
      request = JSON.parse(msg.content.toString()) as CollectRequest;
      const collector = request?.collector ? findCollector(request.collector) : null;
      if (!collector || !collector.supports(request)) {
        await safeAck(msg);
        return;
      }

      const result = await withTimeout(
        collector.collect(request, ctx),
        requestTimeoutMs,
        `collector.request collector=${request.collector} fund_code=${request.fund_code ?? ''}`
      );
      // If collector returns a result, publish to results queue
      if (result) {
        try {
          if (isChannelOpen(publishChannel)) {
            const body = Buffer.from(JSON.stringify(result));
            publishChannel.sendToQueue(resultsQueue, body, { contentType: 'application/json', persistent: true });
          }
        } catch {
          // ignore
        }
      }
      await safeAck(msg);
    } catch (err) {
      const message = err instanceof Error ? err.stack || err.message : String(err);
      process.stderr.write(`[collector-runner] error ${message.replace(/\n/g, '\\n')}\n`);
      if (!isChannelOpen(consumeChannel)) {
        process.stderr.write('[collector-runner] channel closed, discarding message\n');
        return;
      }
      const retryBaseMs = Number.parseInt(process.env.COLLECTOR_RETRY_BASE_MS || '500', 10);
      const maxRetries = Number.parseInt(process.env.COLLECTOR_MAX_RETRIES || '5', 10);
      let outcome: 'acked' | 'retried' | 'dlq' = 'dlq';
      try {
        outcome = await publishWithRetry(publishChannel, requestQueue, msg, {
          maxRetries: Number.isFinite(maxRetries) ? maxRetries : 5,
          retryBaseMs: Number.isFinite(retryBaseMs) ? retryBaseMs : 500,
          retryQueuePrefix: 'collector.requests',
        });
      } catch (retryErr) {
        const retryMessage = retryErr instanceof Error ? retryErr.stack || retryErr.message : String(retryErr);
        process.stderr.write(`[collector-runner] retry-publish error ${retryMessage.replace(/\n/g, '\\n')}\n`);
        await safeNack(msg, true);
        return;
      }
      if (outcome === 'dlq') {
        await safeNack(msg, false);
      } else {
        await safeAck(msg);
      }
    }
  });

  return consumeChannel;
}
