import { Consumer } from 'kafkajs';
import { createConsumer } from '../common/kafkaClient';
import { config } from '../common/config';
import { logger } from '../common/logger';

export class MessageConsumer {
  private consumer!: Consumer;

  async initialize(): Promise<void> {
    this.consumer = await createConsumer();
    await this.consumer.subscribe({ topic: config.kafkaTopic, fromBeginning: true });
    logger.info(`Subscribed to topic: ${config.kafkaTopic}`);
  }

  async startConsuming(): Promise<void> {
    await this.consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        logger.info('Received message:', {
          topic,
          partition,
          value: message.value?.toString(),
          timestamp: message.timestamp
        });
      }
    });
  }

  async stop(): Promise<void> {
    if (this.consumer) {
      await this.consumer.disconnect();
      logger.info('Consumer disconnected');
    }
  }
}