import { MessageConsumer } from './consumer';
import { logger } from '../common/logger';

async function runConsumer(): Promise<void> {
  logger.info('Starting Kafka Consumer....');
  logger.info(`Environment: ${process.env.NODE_ENV || 'undefined'}`);
  logger.info(`Kafka Brokers: ${process.env.KAFKA_BROKERS || 'localhost:9092'}`);
  logger.info(`Topic: ${process.env.KAFKA_TOPIC || 'test-topic'}`);
  logger.info(`Client ID: ${process.env.KAFKA_CLIENT_ID || 'nodejs-kafka-client'}`);
  logger.info(`Group ID: ${process.env.KAFKA_GROUP_ID || 'nodejs-kafka-group'}`);

  const consumer = new MessageConsumer();

  try {
    await consumer.initialize();
    logger.info('Consumer initialized.....');
    await consumer.startConsuming();
  } catch (error) {
    logger.error('Consumer startup failed', {
      error: error instanceof Error ? error.message : error,
      stack: error instanceof Error ? error.stack : undefined
    });

    if (!consumer.isShuttingDownStatus()) {
      process.exit(1);
    }
  }
}

if (require.main === module) {
  runConsumer().catch(error => {
    logger.error('Unhandled consumer error', {
      error: error instanceof Error ? error.message : error
    });
    process.exit(1);
  });
}

export { runConsumer };
