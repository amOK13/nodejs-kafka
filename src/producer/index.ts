import { MessageProducer } from './producer';
import { logger } from '../common/logger';
import { JsonMessageSchema } from '../common/messageValidator';
import { CompressionTypes } from 'kafkajs';
import * as readline from 'readline';

// Utility function to get required environment variable
function getRequiredEnvVar(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Required environment variable ${name} is not set`);
  }
  return value;
}

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
        maxBatchSize: parseInt(getRequiredEnvVar('KAFKA_BATCH_SIZE')),
        lingerMs: parseInt(getRequiredEnvVar('KAFKA_LINGER_MS')),
        maxInFlightRequests: parseInt(getRequiredEnvVar('KAFKA_MAX_IN_FLIGHT_REQUESTS'))
      },
      retry: {
        retries: parseInt(getRequiredEnvVar('KAFKA_RETRIES')),
        initialRetryTime: parseInt(getRequiredEnvVar('KAFKA_RETRY_INITIAL_TIME')),
        maxRetryTime: parseInt(getRequiredEnvVar('KAFKA_RETRY_MAX_TIME'))
      },
      timeout: {
        requestTimeoutMs: parseInt(getRequiredEnvVar('KAFKA_REQUEST_TIMEOUT')),
        acks: 1
      },
      performance: {
        idempotent: true,
        transactionTimeout: parseInt(getRequiredEnvVar('KAFKA_TRANSACTION_TIMEOUT'))
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
      rl.question(getRequiredEnvVar('KAFKA_INTERACTIVE_PROMPT'), async input => {
        const messageText =
          input.trim() || `${process.env.KAFKA_DEFAULT_MESSAGE} - ${new Date().toISOString()}`;

        const messageObject = {
          text: messageText,
          timestamp: new Date().toISOString(),
          source: getRequiredEnvVar('KAFKA_PRODUCER_SOURCE'),
          messageId: Math.random().toString(36).substr(2, 9)
        };

        try {
          await producer.sendMessage(messageObject, `msg-${messageObject.messageId}`, {
            'content-type': getRequiredEnvVar('KAFKA_CONTENT_TYPE'),
            'producer-mode': getRequiredEnvVar('KAFKA_PRODUCER_MODE')
          });
          logger.info(getRequiredEnvVar('KAFKA_SUCCESS_MESSAGE'), {
            messageId: messageObject.messageId
          });
        } catch (error) {
          logger.error('Failed to send message:', error);
        }

        askForMessage().then(resolve);
      });
    });
  };

  rl.on('SIGINT', () => {
    logger.info(`\n${getRequiredEnvVar('KAFKA_EXIT_MESSAGE')}`);
    rl.close();
    process.exit(0);
  });

  await askForMessage();
}

if (require.main === module) {
  main().catch(console.error);
}
