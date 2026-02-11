import type { ChannelModel } from 'amqplib';
import type { CollectRequest } from '../collectors/types';
import { setupQueue } from './rabbit';

const requestQueue = String(process.env.COLLECT_REQUESTS_QUEUE || 'collector.requests').trim() || 'collector.requests';

export async function createCollectorPublisher(connection: ChannelModel) {
  const channel = await connection.createChannel();
  await setupQueue(channel, requestQueue, { dlx: 'collector.dlx', dlq: 'collector.dlq' });

  async function publish(request: CollectRequest) {
    const body = Buffer.from(JSON.stringify(request));
    channel.sendToQueue(requestQueue, body, { contentType: 'application/json', persistent: true });
  }

  return { publish, channel };
}
