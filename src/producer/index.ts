import { MessageProducer } from './producer';
import { Logger } from '../common/logger';

async function main(): Promise<void> {
  const producer = new MessageProducer();
  
  try {
    await producer.connect();

    await producer.sendMessage(`Hello Kafka! - ${new Date().toISOString()}`);
    
    Logger.info('Producer finished successfully');
  } catch (error) {
    Logger.error('Producer error:', error);
  } finally {
    await producer.disconnect();
  }
}

if (require.main === module) {
  main().catch(console.error);
}