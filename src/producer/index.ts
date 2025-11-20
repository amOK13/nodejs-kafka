import { MessageProducer } from './producer';
import { logger } from '../common/logger';
import { JsonMessageSchema } from '../common/messageValidator';
import { CompressionTypes } from 'kafkajs';
import * as readline from 'readline';

async function main(): Promise<void> {
  logger.info('Starting Enhanced Kafka Producer');
  logger.info(`Environment: ${process.env.NODE_ENV || 'undefined'}`);
  logger.info(`Kafka Brokers: ${process.env.KAFKA_BROKERS || 'localhost:9092'}`);
  logger.info(`Topic: ${process.env.KAFKA_TOPIC || 'test-topic'}`);
  logger.info(`Client ID: ${process.env.KAFKA_CLIENT_ID || 'nodejs-kafka-client'}`);

  const producer = new MessageProducer({
    serializationFormat: 'json',
    enableValidation: true,
    schema: new JsonMessageSchema(),
    preset: (process.env.KAFKA_PRODUCER_PRESET as any) || 'balanced',
    config: {
      compression: {
        type: CompressionTypes.Snappy
      },
      batching: {
        maxBatchSize: parseInt(process.env.KAFKA_BATCH_SIZE || '8192'),
        lingerMs: parseInt(process.env.KAFKA_LINGER_MS || '50'),
        maxInFlightRequests: parseInt(process.env.KAFKA_MAX_IN_FLIGHT_REQUESTS || '5')
      },
      retry: {
        retries: parseInt(process.env.KAFKA_RETRIES || '5'),
        initialRetryTime: parseInt(process.env.KAFKA_RETRY_INITIAL_TIME || '300'),
        maxRetryTime: parseInt(process.env.KAFKA_RETRY_MAX_TIME || '30000')
      },
      timeout: {
        requestTimeoutMs: parseInt(process.env.KAFKA_REQUEST_TIMEOUT || '30000'),
        acks: 1
      },
      performance: {
        idempotent: true,
        transactionTimeout: parseInt(process.env.KAFKA_TRANSACTION_TIMEOUT || '60000')
      }
    }
  });

  try {
    await producer.initialize();

    const args = process.argv.slice(2);

    if (args.length === 0) {
      if (process.env.NODE_ENV === 'development') {
        await interactiveMode(producer);
      } else {
        logger.info('Producer finished successfully');
        return;
      }
    } else {
      const messageText = args.join(' ');
      await producer.sendMessage({
        text: messageText,
        timestamp: new Date().toISOString(),
        source: 'cli-producer'
      });
    }
  } catch (error) {
    logger.error('Producer error:', error);
    process.exit(1);
  } finally {
    await producer.disconnect();
  }
}

async function interactiveMode(producer: MessageProducer): Promise<void> {
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
  });

  logger.info('Interactive mode started. Type messages to send (Ctrl+C to exit):');

  const askForMessage = (): Promise<void> => {
    return new Promise(resolve => {
      rl.question(
        process.env.KAFKA_INTERACTIVE_PROMPT || 'Enter message (or press Enter for default): ',
        async input => {
          const messageText =
            input.trim() ||
            `${process.env.KAFKA_DEFAULT_MESSAGE || 'Hello Enhanced Kafka!'} - ${new Date().toISOString()}`;

          const messageObject = {
            text: messageText,
            timestamp: new Date().toISOString(),
            source: process.env.KAFKA_PRODUCER_SOURCE || 'interactive-producer',
            messageId: Math.random().toString(36).substr(2, 9)
          };

          try {
            await producer.sendMessage(messageObject, `msg-${messageObject.messageId}`, {
              'content-type': process.env.KAFKA_CONTENT_TYPE || 'application/json',
              'producer-mode': process.env.KAFKA_PRODUCER_MODE || 'interactive'
            });
            logger.info(
              process.env.KAFKA_SUCCESS_MESSAGE || 'Enhanced message sent successfully!',
              {
                messageId: messageObject.messageId
              }
            );
          } catch (error) {
            logger.error('Failed to send message:', error);
          }

          askForMessage().then(resolve);
        }
      );
    });
  };

  rl.on('SIGINT', () => {
    logger.info(`\n${process.env.KAFKA_EXIT_MESSAGE || 'Exiting interactive mode...'}`);
    rl.close();
    process.exit(0);
  });

  await askForMessage();
}

if (require.main === module) {
  main().catch(console.error);
}
