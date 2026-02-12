import type amqplib from 'amqplib';

function computeDelayMs(retryBaseMs: number, attempts: number): number {
  const base = Number.isFinite(retryBaseMs) && retryBaseMs > 0 ? retryBaseMs : 500;
  const pow = attempts >= 0 ? attempts : 0;
  return Math.min(base * Math.pow(2, pow), 60_000);
}

export async function ensureRetryQueues(
  channel: amqplib.Channel,
  queue: string,
  opts: { maxRetries: number; retryBaseMs: number; retryQueuePrefix?: string }
): Promise<void> {
  const maxRetries = Math.max(0, Math.floor(opts.maxRetries));
  const uniqueDelays = new Set<number>();
  for (let attempts = 0; attempts < maxRetries; attempts++) {
    uniqueDelays.add(computeDelayMs(opts.retryBaseMs, attempts));
  }

  for (const delayMs of uniqueDelays) {
    const retryQueue = `${opts.retryQueuePrefix ?? queue}.retry.${delayMs}`;
    await channel.assertQueue(retryQueue, {
      durable: true,
      arguments: {
        'x-message-ttl': delayMs,
        'x-dead-letter-exchange': '',
        'x-dead-letter-routing-key': queue,
      },
    });
  }
}

export async function publishWithRetry(
  channel: amqplib.Channel,
  queue: string,
  msg: amqplib.Message,
  opts: { maxRetries: number; retryBaseMs: number; retryQueuePrefix?: string }
): Promise<'acked' | 'retried' | 'dlq'> {
  const headers = msg.properties.headers ?? {};
  const attempts = Number.parseInt(String(headers['x-attempts'] ?? '0'), 10) || 0;
  const maxRetries = Math.max(0, Math.floor(opts.maxRetries));
  if (attempts >= maxRetries) {
    return 'dlq';
  }

  const nextAttempt = attempts + 1;
  const delayMs = computeDelayMs(opts.retryBaseMs, attempts);
  const retryQueue = `${opts.retryQueuePrefix ?? queue}.retry.${delayMs}`;

  channel.sendToQueue(retryQueue, msg.content, {
    ...msg.properties,
    headers: { ...headers, 'x-attempts': nextAttempt },
    persistent: true,
  });

  return 'retried';
}
