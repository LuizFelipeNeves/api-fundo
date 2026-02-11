import type amqplib from 'amqplib';

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
  const delayMs = Math.min(opts.retryBaseMs * Math.pow(2, attempts), 60_000);
  const retryQueue = `${opts.retryQueuePrefix ?? queue}.retry.${delayMs}`;

  await channel.assertQueue(retryQueue, {
    durable: true,
    arguments: {
      'x-message-ttl': delayMs,
      'x-dead-letter-exchange': '',
      'x-dead-letter-routing-key': queue,
    },
  });

  channel.sendToQueue(retryQueue, msg.content, {
    ...msg.properties,
    headers: { ...headers, 'x-attempts': nextAttempt },
    persistent: true,
  });

  return 'retried';
}
