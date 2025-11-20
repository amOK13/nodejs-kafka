import { MessageProducer } from './producer';
import { logger } from '../common/logger';
import * as readline from 'readline';

async function main(): Promise<void> {
  logger.info(`Starting Kafka Producer`);
  logger.info(`Environment: ${process.env.NODE_ENV || 'undefined'}`);
  logger.info(`Kafka Brokers: ${process.env.KAFKA_BROKERS || 'localhost:9092'}`);
  logger.info(`Topic: ${process.env.KAFKA_TOPIC || 'test-topic'}`);
  logger.info(`Client ID: ${process.env.KAFKA_CLIENT_ID || 'nodejs-kafka-client'}`);

  const producer = new MessageProducer();
  
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
      if (process.env.NODE_ENV === 'development') {
        logger.error('Interactive mode is only available in development environment (NODE_ENV=development)');
        logger.info('Usage: npm run producer "Your message here"');
        process.exit(1);
      }
      
      await producer.sendMessage(args.join(' '));
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
    return new Promise((resolve) => {
      rl.question('Enter message (or press Enter for "Hello Kafka!"): ', async (input) => {
        const message = input.trim() || `Hello Kafka! - ${new Date().toISOString()}`;
        
        try {
          await producer.sendMessage(message);
          logger.info('Message sent successfully!');
        } catch (error) {
          logger.error('Failed to send message:', error);
        }

        askForMessage().then(resolve);
      });
    });
  };

  rl.on('SIGINT', () => {
    logger.info('\nExiting interactive mode...');
    rl.close();
    process.exit(0);
  });

  await askForMessage();
}

if (require.main === module) {
  main().catch(console.error);
}