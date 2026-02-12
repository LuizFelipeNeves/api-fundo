import type { ChannelModel, ConsumeMessage } from 'amqplib';
import { findCollector } from '../collectors/registry';
import type { CollectRequest, CollectorContext } from '../collectors/types';
import { getJson, getText, postForm } from '../http/client';
import { normalizeCollectResult } from '../pipeline/normalizers';
import { processPersistRequest } from '../pipeline/processor';
import { setupQueue } from '../queues/rabbit';
import { ensureRetryQueues, publishWithRetry } from '../queues/retry';

const requestQueue =
  String(process.env.COLLECT_REQUESTS_QUEUE || 'collector.requests').trim() ||
  'collector.requests';

function isChannelOpen(channel: unknown): boolean {
  const ch: any = channel;
  return !!(ch && !ch.closed && ch.connection && typeof ch.ack === 'function');
}

async function withTimeout<T>(
  promise: Promise<T>,
  timeoutMs: number,
  label: string
): Promise<T> {
  if (!timeoutMs) return promise;

  let tid: ReturnType<typeof setTimeout>;

  return new Promise<T>((resolve, reject) => {
    tid = setTimeout(() => {
      reject(new Error(`timeout label=${label} after_ms=${timeoutMs}`));
    }, timeoutMs);

    promise.then(
      v => {
        clearTimeout(tid);
        resolve(v);
      },
      e => {
        clearTimeout(tid);
        reject(e);
      }
    );
  });
}


export async function startCollectorRunner(
  connection: ChannelModel,
  isActive: () => boolean = () => true
) {
  const consumeChannel = await connection.createChannel();
  const publishChannel = await connection.createChannel();

  const handledKey = Symbol.for('worker.collector.handled');

  await setupQueue(publishChannel, requestQueue, {
    dlx: 'collector.dlx',
    dlq: 'collector.dlq',
  });

  const prefetchRaw = Number.parseInt(process.env.COLLECTOR_PREFETCH || '6', 10);
  const prefetch =
    Number.isFinite(prefetchRaw) && prefetchRaw > 0
      ? Math.min(prefetchRaw, 50)
      : 6;

  await consumeChannel.prefetch(prefetch);

  const requestTimeoutMs =
    Number.parseInt(process.env.COLLECTOR_MESSAGE_TIMEOUT_MS || '180000', 10) ||
    180000;

  const persistTimeoutMs =
    Number.parseInt(process.env.PERSIST_MESSAGE_TIMEOUT_MS || '120000', 10) ||
    120000;

  const collectResultTimeoutMs =
    Number.parseInt(process.env.RESULTS_MESSAGE_TIMEOUT_MS || '120000', 10) ||
    120000;

  const retryBaseMs =
    Number.parseInt(process.env.COLLECTOR_RETRY_BASE_MS || '500', 10) || 500;

  const maxRetries =
    Number.parseInt(process.env.COLLECTOR_MAX_RETRIES || '5', 10) || 5;

  await ensureRetryQueues(publishChannel, requestQueue, {
    maxRetries,
    retryBaseMs,
    retryQueuePrefix: 'collector.requests',
  });

  const ctx: CollectorContext = {
    http: { getJson, getText, postForm },
  };

  async function handlerMessage(msg: ConsumeMessage | null) {
    if (!msg) return;

      try {
        if (!isActive()) {
          if (isChannelOpen(consumeChannel))
            consumeChannel.nack(msg, false, true);
          return;
        }

        if ((msg as any)[handledKey]) return;

        // 🔥 parse direto do buffer (menos memória)
        const request = JSON.parse(msg.content.toString('utf8')) as CollectRequest;

        const collector =
          request?.collector && findCollector(request.collector);

        if (!collector || !collector.supports(request)) {
          (msg as any)[handledKey] = true;
          if (isChannelOpen(consumeChannel)) consumeChannel.ack(msg);
          return;
        }

        const result = await withTimeout(
          collector.collect(request, ctx),
          requestTimeoutMs,
          'collector.request'
        );

        if (result) {
          const persistRequests = await withTimeout(
            Promise.resolve(normalizeCollectResult(result)),
            collectResultTimeoutMs,
            'collector.normalize'
          );

          // 🔥 execução mais rápida e menor retenção
          for (let i = 0; i < persistRequests.length; i++) {
            await withTimeout(
              processPersistRequest(persistRequests[i]),
              persistTimeoutMs,
              'collector.persist'
            );
          }
        }

        (msg as any)[handledKey] = true;
        if (isChannelOpen(consumeChannel)) consumeChannel.ack(msg);
      } catch (err) {
        const message =
          err instanceof Error ? err.stack || err.message : String(err);

        process.stderr.write(
          `[collector-runner] error ${message.replace(/\n/g, '\\n')}\n`
        );

        if (!isChannelOpen(consumeChannel)) return;

        if (!isChannelOpen(publishChannel)) {
          consumeChannel.nack(msg, false, true);
          return;
        }

        try {
          const outcome = await publishWithRetry(
            publishChannel,
            requestQueue,
            msg,
            {
              maxRetries,
              retryBaseMs,
              retryQueuePrefix: 'collector.requests',
            }
          );

          if (outcome === 'dlq') {
            consumeChannel.nack(msg, false, false);
          } else {
            (msg as any)[handledKey] = true;
            consumeChannel.ack(msg);
          }
        } catch {
          consumeChannel.nack(msg, false, true);
        }
      }
    }

  consumeChannel.consume(
    requestQueue,
    handlerMessage,
    { noAck: false }
  );

  return consumeChannel;
}
