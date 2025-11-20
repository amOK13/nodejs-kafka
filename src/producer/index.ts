import { MessageProducer } from './producer';
import { logger } from '../common/logger';
import { JsonMessageSchema } from '../common/messageValidator';
import * as readline from 'readline';

async function main(): Promise<void> {
  logger.info(`Starting Enhanced Kafka Producer`);
  logger.info(`Environment: ${process.env.NODE_ENV || 'undefined'}`);
  logger.info(`Kafka Brokers: ${process.env.KAFKA_BROKERS || 'localhost:9092'}`);
  logger.info(`Topic: ${process.env.KAFKA_TOPIC || 'test-topic'}`);
  logger.info(`Client ID: ${process.env.KAFKA_CLIENT_ID || 'nodejs-kafka-client'}`);

  // Configuration du producer avec validation JSON et sérialisation
  const producer = new MessageProducer({
    serializationFormat: 'json',
    enableValidation: true,
    schema: new JsonMessageSchema()
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
      if (process.env.NODE_ENV === 'development') {
        logger.error('Interactive mode is only available in development environment (NODE_ENV=development)');
        logger.info('Usage: npm run producer "Your message here"');
        process.exit(1);
      }
      
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
    return new Promise((resolve) => {
      rl.question('Enter message (or press Enter for default): ', async (input) => {
        const messageText = input.trim() || `Hello Enhanced Kafka! - ${new Date().toISOString()}`;
        
        const messageObject = {
          text: messageText,
          timestamp: new Date().toISOString(),
          source: 'interactive-producer',
          messageId: Math.random().toString(36).substr(2, 9)
        };
        
        try {
          await producer.sendMessage(messageObject, `msg-${messageObject.messageId}`, {
            'content-type': 'application/json',
            'producer-mode': 'interactive'
          });
          logger.info('Enhanced message sent successfully!', { messageId: messageObject.messageId });
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