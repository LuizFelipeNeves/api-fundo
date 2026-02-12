import type { ChannelModel, ConsumeMessage } from 'amqplib';
import { findCollector } from '../collectors/registry';
import type { CollectRequest, CollectorContext } from '../collectors/types';
import { getJson, getText, postForm } from '../http/client';
import { normalizeCollectResult } from '../pipeline/normalizers';
import { processPersistRequest } from '../pipeline/processor';
import { setupQueue } from '../queues/rabbit';
import { ensureRetryQueues, publishWithRetry } from '../queues/retry';

const requestQueue = String(process.env.COLLECT_REQUESTS_QUEUE || 'collector.requests').trim() || 'collector.requests';

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
  const handledDeliveryTags = new Set<number>();
  let lastPruneTag = 0;

  function shouldSkipByDeliveryTag(tag: unknown): boolean {
    if (typeof tag !== 'number') return false;
    if (handledDeliveryTags.has(tag)) return true;
    handledDeliveryTags.add(tag);

    const maxTags = 5000;
    const pruneEveryTags = 1000;
    const keepWindowTags = 20000;
    if (handledDeliveryTags.size > maxTags && tag - lastPruneTag >= pruneEveryTags) {
      lastPruneTag = tag;
      const cutoff = tag - keepWindowTags;
      for (const t of handledDeliveryTags) {
        if (t < cutoff) handledDeliveryTags.delete(t);
      }
      if (handledDeliveryTags.size > maxTags * 3) {
        handledDeliveryTags.clear();
        handledDeliveryTags.add(tag);
      }
    }
    return false;
  }

  await setupQueue(publishChannel, requestQueue, { dlx: 'collector.dlx', dlq: 'collector.dlq' });

  const prefetchRaw = Number.parseInt(process.env.COLLECTOR_PREFETCH || '6', 10);
  const prefetch = Number.isFinite(prefetchRaw) && prefetchRaw > 0 ? Math.min(prefetchRaw, 50) : 6;
  await consumeChannel.prefetch(prefetch);

  const requestTimeoutRaw = Number.parseInt(process.env.COLLECTOR_MESSAGE_TIMEOUT_MS || '180000', 10);
  const requestTimeoutMs = Number.isFinite(requestTimeoutRaw) && requestTimeoutRaw > 0 ? requestTimeoutRaw : 180000;
  const persistTimeoutRaw = Number.parseInt(process.env.PERSIST_MESSAGE_TIMEOUT_MS || '120000', 10);
  const persistTimeoutMs = Number.isFinite(persistTimeoutRaw) && persistTimeoutRaw > 0 ? persistTimeoutRaw : 120000;
  const collectResultTimeoutRaw = Number.parseInt(process.env.RESULTS_MESSAGE_TIMEOUT_MS || '120000', 10);
  const collectResultTimeoutMs =
    Number.isFinite(collectResultTimeoutRaw) && collectResultTimeoutRaw > 0 ? collectResultTimeoutRaw : 120000;
  const retryBaseMsRaw = Number.parseInt(process.env.COLLECTOR_RETRY_BASE_MS || '500', 10);
  const retryBaseMs = Number.isFinite(retryBaseMsRaw) ? retryBaseMsRaw : 500;
  const maxRetriesRaw = Number.parseInt(process.env.COLLECTOR_MAX_RETRIES || '5', 10);
  const maxRetries = Number.isFinite(maxRetriesRaw) ? maxRetriesRaw : 5;

  await ensureRetryQueues(publishChannel, requestQueue, {
    maxRetries,
    retryBaseMs,
    retryQueuePrefix: 'collector.requests',
  });

  async function safeAck(msg: ConsumeMessage) {
    try {
      const tag = msg.fields?.deliveryTag;
      if (shouldSkipByDeliveryTag(tag)) return;
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
      const tag = msg.fields?.deliveryTag;
      if (shouldSkipByDeliveryTag(tag)) return;
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
      // If collector returns a result, persist directly in this worker process.
      if (result) {
        const persistRequests = await withTimeout(
          Promise.resolve(normalizeCollectResult(result)),
          collectResultTimeoutMs,
          `collector.normalize collector=${result.collector}`
        );
        for (const persistRequest of persistRequests) {
          await withTimeout(
            processPersistRequest(persistRequest),
            persistTimeoutMs,
            `collector.persist type=${persistRequest.type} collector=${result.collector}`
          );
        }
        process.stdout.write(
          `[collector-runner] persisted=${persistRequests.length} collector=${result.collector} fetched_at=${result.fetched_at}\n`
        );
      }
      await safeAck(msg);
    } catch (err) {
      const message = err instanceof Error ? err.stack || err.message : String(err);
      process.stderr.write(`[collector-runner] error ${message.replace(/\n/g, '\\n')}\n`);
      if (!isChannelOpen(consumeChannel)) {
        process.stderr.write('[collector-runner] channel closed, discarding message\n');
        return;
      }
      if (!isChannelOpen(publishChannel)) {
        await safeNack(msg, true);
        return;
      }
      let outcome: 'acked' | 'retried' | 'dlq' = 'dlq';
      try {
        outcome = await publishWithRetry(publishChannel, requestQueue, msg, {
          maxRetries,
          retryBaseMs,
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
  }, { noAck: false });

  return consumeChannel;
}
