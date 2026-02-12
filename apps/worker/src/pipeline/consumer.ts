import type { ChannelModel, ConsumeMessage } from 'amqplib';
import { processPersistRequest } from './processor';
import { normalizeCollectResult } from './normalizers';
import type { PersistRequest, CollectResult } from './messages';
import { setupQueue } from '../queues/rabbit';
import { publishWithRetry } from '../queues/retry';

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

export async function startPipelineConsumers(connection: ChannelModel, isActive: () => boolean = () => true) {
  const consumeChannel = await connection.createChannel();
  const publishChannel = await connection.createChannel();
  const handledKey = Symbol.for('worker.pipeline.handled');

  const persistQueue = String(process.env.PERSIST_QUEUE_NAME || 'persistence.write').trim() || 'persistence.write';
  const collectResultsQueue = String(process.env.COLLECT_RESULTS_QUEUE || 'collector.results').trim() || 'collector.results';

  await setupQueue(publishChannel, persistQueue, { dlx: 'pipeline.dlx', dlq: 'pipeline.dlq' });
  await setupQueue(publishChannel, collectResultsQueue, { dlx: 'collector.dlx', dlq: 'collector.dlq' });

  const prefetchRaw = Number.parseInt(process.env.PIPELINE_PREFETCH || '8', 10);
  const prefetch = Number.isFinite(prefetchRaw) && prefetchRaw > 0 ? Math.min(prefetchRaw, 100) : 8;
  await consumeChannel.prefetch(prefetch);

  const persistTimeoutRaw = Number.parseInt(process.env.PERSIST_MESSAGE_TIMEOUT_MS || '120000', 10);
  const persistTimeoutMs = Number.isFinite(persistTimeoutRaw) && persistTimeoutRaw > 0 ? persistTimeoutRaw : 120000;
  const collectResultTimeoutRaw = Number.parseInt(process.env.RESULTS_MESSAGE_TIMEOUT_MS || '120000', 10);
  const collectResultTimeoutMs =
    Number.isFinite(collectResultTimeoutRaw) && collectResultTimeoutRaw > 0 ? collectResultTimeoutRaw : 120000;

  async function safeAck(msg: ConsumeMessage) {
    try {
      if (!isActive()) return;
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
      if (!isActive()) return;
      if ((msg as any)[handledKey]) return;
      (msg as any)[handledKey] = requeue ? 'nack_requeue' : 'nack';
      if (isChannelOpen(consumeChannel)) {
        consumeChannel.nack(msg, false, requeue);
      }
    } catch {
      // ignore
    }
  }

  consumeChannel.consume(persistQueue, async (msg: ConsumeMessage | null) => {
    if (!msg || !isActive()) return;
    try {
      const payload = JSON.parse(msg.content.toString()) as PersistRequest;
      await withTimeout(processPersistRequest(payload), persistTimeoutMs, `pipeline.persist type=${payload.type}`);
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
      let outcome: 'acked' | 'retried' | 'dlq' = 'dlq';
      try {
        outcome = await publishWithRetry(publishChannel, persistQueue, msg, {
          maxRetries: Number.isFinite(maxRetries) ? maxRetries : 6,
          retryBaseMs: Number.isFinite(retryBaseMs) ? retryBaseMs : 500,
          retryQueuePrefix: 'persistence.write',
        });
      } catch (retryErr) {
        const retryMessage = retryErr instanceof Error ? retryErr.stack || retryErr.message : String(retryErr);
        process.stderr.write(`[pipeline] persist retry-publish error ${retryMessage.replace(/\n/g, '\\n')}\n`);
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

  consumeChannel.consume(collectResultsQueue, async (msg: ConsumeMessage | null) => {
    if (!msg || !isActive()) return;
    try {
      const payload = JSON.parse(msg.content.toString()) as CollectResult;
      const hasEnvelope =
        payload &&
        typeof payload === 'object' &&
        typeof (payload as any).collector === 'string' &&
        typeof (payload as any).fetched_at === 'string' &&
        Object.prototype.hasOwnProperty.call(payload as any, 'payload');
      if (!hasEnvelope) {
        throw new Error('invalid_collect_result_message');
      }
      const persistRequests = await withTimeout(
        Promise.resolve(normalizeCollectResult(payload)),
        collectResultTimeoutMs,
        `pipeline.collect-result collector=${payload.collector}`
      );
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
      let outcome: 'acked' | 'retried' | 'dlq' = 'dlq';
      try {
        outcome = await publishWithRetry(publishChannel, collectResultsQueue, msg, {
          maxRetries: Number.isFinite(maxRetries) ? maxRetries : 6,
          retryBaseMs: Number.isFinite(retryBaseMs) ? retryBaseMs : 500,
          retryQueuePrefix: 'collector.results',
        });
      } catch (retryErr) {
        const retryMessage = retryErr instanceof Error ? retryErr.stack || retryErr.message : String(retryErr);
        process.stderr.write(`[pipeline] collect-result retry-publish error ${retryMessage.replace(/\n/g, '\\n')}\n`);
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
