import { Producer } from 'kafkajs';
import { KafkaClient } from '../common/kafkaClient';
import { kafkaConfig } from '../common/config';
import { Logger } from '../common/logger';

export class MessageProducer {
  private producer: Producer;
  private kafkaClient: KafkaClient;

  constructor() {
    this.kafkaClient = new KafkaClient();
    this.producer = this.kafkaClient.createProducer();
  }

  async connect(): Promise<void> {
    await this.producer.connect();
    Logger.info('Producer connected');
  }

  async sendMessage(message: string): Promise<void> {
    await this.producer.send({
      topic: kafkaConfig.topic,
      messages: [{
        value: message,
        timestamp: Date.now().toString()
      }]
    });
    Logger.info('Message sent:', message);
  }

  async disconnect(): Promise<void> {
    await this.producer.disconnect();
    Logger.info('Producer disconnected');
  }
}