import amqplib from 'amqplib';

let connection: amqplib.ChannelModel | null = null;
let channel: amqplib.Channel | null = null;

async function getChannel(): Promise<amqplib.Channel> {
  if (channel) return channel;
  const url = String(process.env.RABBITMQ_URL || '').trim();
  if (!url) throw new Error('RABBITMQ_URL is required for telegram queue publishing');
  const heartbeat = Number.parseInt(process.env.RABBITMQ_HEARTBEAT || '60', 10);
  const frameMax = Number.parseInt(process.env.RABBITMQ_FRAME_MAX || '131072', 10);

  connection = await amqplib.connect(url, { heartbeat, frameMax });
  channel = await connection.createChannel();

  const queueName = getTelegramQueueName();
  const dlx = String(process.env.TELEGRAM_DLX || 'telegram.dlx').trim() || 'telegram.dlx';
  const dlq = String(process.env.TELEGRAM_DLQ || 'telegram.dlq').trim() || 'telegram.dlq';
  await channel.assertExchange(dlx, 'direct', { durable: true });
  await channel.assertQueue(dlq, { durable: true });
  await channel.bindQueue(dlq, dlx, dlq);
  await channel.assertQueue(queueName, {
    durable: true,
    arguments: {
      'x-dead-letter-exchange': dlx,
      'x-dead-letter-routing-key': dlq,
    },
  });

  const shutdown = async () => {
    try {
      await channel?.close();
    } catch {
      // ignore
    }
    try {
      await connection?.close();
    } catch {
      // ignore
    }
    channel = null;
    connection = null;
  };

  process.once('SIGINT', shutdown);
  process.once('SIGTERM', shutdown);

  return channel;
}

export function getTelegramQueueName(): string {
  return String(process.env.TELEGRAM_QUEUE_NAME || 'telegram.updates').trim() || 'telegram.updates';
}

export async function publishTelegramUpdate(payload: unknown): Promise<void> {
  const ch = await getChannel();
  const queueName = getTelegramQueueName();
  const body = Buffer.from(JSON.stringify(payload));

  const ok = ch.sendToQueue(queueName, body, {
    contentType: 'application/json',
    persistent: true,
  });

  if (!ok) {
    await new Promise<void>((resolve) => ch.once('drain', () => resolve()));
  }
}
