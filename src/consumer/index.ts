import { MessageConsumer } from './consumer';
import { logger } from '../common/logger';

async function main(): Promise<void> {
  logger.info('Starting Kafka Consumer');
  logger.info(`Environment: ${process.env.NODE_ENV || 'undefined'}`);
  logger.info(`Kafka Brokers: ${process.env.KAFKA_BROKERS || 'localhost:9092'}`);
  logger.info(`Topic: ${process.env.KAFKA_TOPIC || 'test-topic'}`);
  logger.info(`Client ID: ${process.env.KAFKA_CLIENT_ID || 'nodejs-kafka-client'}`);
  logger.info(`Group ID: ${process.env.KAFKA_GROUP_ID || 'nodejs-kafka-group'}`);

  const consumer = new MessageConsumer();

  try {
    await consumer.initialize();
    logger.info('Starting to consume messages...');
    await consumer.startConsuming();
  } catch (error) {
    logger.error('Consumer error:', error);
    process.exit(1);
  }

  process.on('SIGINT', async () => {
    logger.info('Shutting down consumer...');
    await consumer.stop();
    process.exit(0);
  });
}

if (require.main === module) {
  main().catch(console.error);
}
