import { Kafka, Consumer, Producer } from 'kafkajs';
import { kafkaConfig } from './config';
import { Logger } from './logger';

export class KafkaClient {
  private kafka: Kafka;

  constructor() {
    this.kafka = new Kafka({
      clientId: kafkaConfig.clientId,
      brokers: kafkaConfig.brokers
    });
    Logger.info('Kafka client initialized');
  }

  createProducer(): Producer {
    return this.kafka.producer();
  }

  createConsumer(): Consumer {
    return this.kafka.consumer({ groupId: kafkaConfig.groupId });
  }
}