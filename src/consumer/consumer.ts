import { Consumer } from 'kafkajs';
import { KafkaClient } from '../common/kafkaClient';
import { kafkaConfig } from '../common/config';
import { Logger } from '../common/logger';

export class MessageConsumer {
  private consumer: Consumer;
  private kafkaClient: KafkaClient;

  constructor() {
    this.kafkaClient = new KafkaClient();
    this.consumer = this.kafkaClient.createConsumer();
  }

  async connect(): Promise<void> {
    await this.consumer.connect();
    Logger.info('Consumer connected');
  }

  async subscribe(): Promise<void> {
    await this.consumer.subscribe({ topic: kafkaConfig.topic, fromBeginning: true });
    Logger.info(`Subscribed to topic: ${kafkaConfig.topic}`);
  }

  async startConsuming(): Promise<void> {
    await this.consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        Logger.info('Received message:', {
          topic,
          partition,
          value: message.value?.toString(),
          timestamp: message.timestamp
        });
      }
    });
  }

  async disconnect(): Promise<void> {
    await this.consumer.disconnect();
    Logger.info('Consumer disconnected');
  }
}