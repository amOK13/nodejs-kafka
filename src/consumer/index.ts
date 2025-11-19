import { MessageConsumer } from './consumer';
import { Logger } from '../common/logger';

async function main(): Promise<void> {
  const consumer = new MessageConsumer();
  
  try {
    await consumer.connect();
    await consumer.subscribe();
    
    Logger.info('Starting to consume messages...');
    await consumer.startConsuming();
    
  } catch (error) {
    Logger.error('Consumer error:', error);
  }

  process.on('SIGINT', async () => {
    Logger.info('Shutting down consumer...');
    await consumer.disconnect();
    process.exit(0);
  });
}

if (require.main === module) {
  main().catch(console.error);
}