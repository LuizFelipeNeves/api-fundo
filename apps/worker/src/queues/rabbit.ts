import amqplib, { Connection } from "amqplib";

export async function createRabbitConnection(): Promise<Connection> {
  const url = String(process.env.RABBITMQ_URL || "").trim();
  if (!url) throw new Error("RABBITMQ_URL is required");

  const heartbeat = Number.parseInt(process.env.RABBITMQ_HEARTBEAT || "60", 10);
  const frameMax = Number.parseInt(process.env.RABBITMQ_FRAME_MAX || "131072", 10);

  return amqplib.connect(url, { heartbeat, frameMax });
}

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

export function getQueueNames() {
  return {
    telegram: String(process.env.TELEGRAM_QUEUE_NAME || 'telegram.updates').trim() || 'telegram.updates',
    collectRequests: String(process.env.COLLECT_REQUESTS_QUEUE || 'collector.requests').trim() || 'collector.requests',
    collectResults: String(process.env.COLLECT_RESULTS_QUEUE || 'collector.results').trim() || 'collector.results',
    persist: String(process.env.PERSIST_QUEUE_NAME || 'persistence.write').trim() || 'persistence.write',
  };
}
