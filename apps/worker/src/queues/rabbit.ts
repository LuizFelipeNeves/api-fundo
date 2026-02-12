import amqplib from "amqplib";

export async function setupQueue(
  channel: amqplib.Channel,
  name: string,
  opts?: { dlx?: string; dlq?: string; ttlMs?: number }
) {
  if (opts?.dlx && opts?.dlq) {
    await channel.assertExchange(opts.dlx, 'direct', { durable: true });
    await channel.assertQueue(opts.dlq, { durable: true });
    await channel.bindQueue(opts.dlq, opts.dlx, opts.dlq);
  }

  const args: Record<string, any> = {};
  if (opts?.dlx && opts?.dlq) {
    args['x-dead-letter-exchange'] = opts.dlx;
    args['x-dead-letter-routing-key'] = opts.dlq;
  }
  if (opts?.ttlMs && opts.ttlMs > 0) {
    args['x-message-ttl'] = opts.ttlMs;
  }

  await channel.assertQueue(name, { durable: true, arguments: Object.keys(args).length ? args : undefined });
}
