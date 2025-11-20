import { Kafka, Consumer, Producer } from 'kafkajs';
import { config } from './config';
import { logger } from './logger';

export function createKafka(): Kafka {
  const kafka = new Kafka({
    clientId: config.kafkaClientId,
    brokers: config.kafkaBrokers
  });

  logger.info('Kafka instance created', {
    clientId: config.kafkaClientId,
    brokers: config.kafkaBrokers
  });
  return kafka;
}

export async function createProducer(): Promise<Producer> {
  try {
    const kafka = createKafka();
    const producer = kafka.producer();

    await producer.connect();
    logger.info('Producer connected successfully');

    return producer;
  } catch (error) {
    logger.error('Failed to create and connect producer', {
      error: error instanceof Error ? error.message : error
    });
    throw error;
  }
}

export async function createConsumer(groupIdOverride?: string): Promise<Consumer> {
  try {
    const kafka = createKafka();
    const groupId = groupIdOverride || config.kafkaConsumerGroupId;
    const consumer = kafka.consumer({ groupId });

    await consumer.connect();
    logger.info('Consumer connected successfully', { groupId });

    return consumer;
  } catch (error) {
    logger.error('Failed to create and connect consumer', {
      error: error instanceof Error ? error.message : error
    });
    throw error;
  }
}
