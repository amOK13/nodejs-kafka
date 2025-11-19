import { MessageProducer } from './producer';
import { Logger } from '../common/logger';
import * as readline from 'readline';

async function main(): Promise<void> {
  const producer = new MessageProducer();
  
  try {
    await producer.connect();

   const customMessage = process.argv[2];
    
    if (customMessage) {
      await producer.sendMessage(customMessage);
      Logger.info('Producer finished successfully');
    } else {
      await interactiveMode(producer);
    }
    
  } catch (error) {
    Logger.error('Producer error:', error);
  } finally {
    await producer.disconnect();
  }
}

async function interactiveMode(producer: MessageProducer): Promise<void> {
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
  });

  Logger.info('Interactive mode started. Type messages to send (Ctrl+C to exit):');
  
  const askForMessage = (): Promise<void> => {
    return new Promise((resolve) => {
      rl.question('Enter message (or press Enter for "Hello Kafka!"): ', async (input) => {
        const message = input.trim() || `Hello Kafka! - ${new Date().toISOString()}`;
        
        try {
          await producer.sendMessage(message);
          Logger.info('Message sent successfully!');
        } catch (error) {
          Logger.error('Failed to send message:', error);
        }

        askForMessage().then(resolve);
      });
    });
  };

  rl.on('SIGINT', () => {
    Logger.info('\nExiting interactive mode...');
    rl.close();
    process.exit(0);
  });

  await askForMessage();
}

if (require.main === module) {
  main().catch(console.error);
}