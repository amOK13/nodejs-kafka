import { Producer } from 'kafkajs';
import { createProducer } from '../common/kafkaClient';
import { config } from '../common/config';
import { logger } from '../common/logger';

export class MessageProducer {
  private producer!: Producer;

  async initialize(): Promise<void> {
    this.producer = await createProducer();
  }

  async sendMessage(message: string): Promise<void> {
    await this.producer.send({
      topic: config.kafkaTopic,
      messages: [{
        value: message,
        timestamp: Date.now().toString()
      }]
    });
    logger.info('Message sent:', message);
  }

  async disconnect(): Promise<void> {
    if (this.producer) {
      await this.producer.disconnect();
      logger.info('Producer disconnected');
    }
  }
}