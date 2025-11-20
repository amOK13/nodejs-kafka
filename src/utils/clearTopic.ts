import { createKafka } from '../common/kafkaClient';
import { logger } from '../common/logger';
import { config } from '../common/config';

/**
 * Utility to clear/empty a Kafka topic in development
 * This will consume all messages from the topic without processing them
 */
async function clearTopic(topicName?: string): Promise<void> {
  const targetTopic = topicName || config.kafkaTopic;

  if (process.env.NODE_ENV === 'production') {
    logger.error('Topic clearing is not allowed in production environment');
    process.exit(1);
  }

  logger.info(`Starting to clear topic: ${targetTopic}`);

  const kafka = createKafka();
  const admin = kafka.admin();
  const consumer = kafka.consumer({ groupId: `clear-topic-${Date.now()}` });

  try {
    await admin.connect();
    await consumer.connect();

    const topics = await admin.listTopics();
    if (!topics.includes(targetTopic)) {
      logger.info(`Topic '${targetTopic}' does not exist`);
      return;
    }

    const metadata = await admin.fetchTopicMetadata({ topics: [targetTopic] });
    const partitions = metadata.topics[0]?.partitions || [];

    if (partitions.length === 0) {
      logger.info(`Topic '${targetTopic}' has no partitions`);
      return;
    }

    logger.info(`Topic '${targetTopic}' has ${partitions.length} partitions`);

    await consumer.subscribe({ topic: targetTopic, fromBeginning: true });

    let totalMessagesCleared = 0;
    let lastMessageTime = Date.now();
    const timeoutMs = 5000;

    logger.info('Starting to consume messages...');

    await consumer.run({
      eachMessage: async ({ partition, message }) => {
        totalMessagesCleared++;
        lastMessageTime = Date.now();

        if (totalMessagesCleared % 100 === 0) {
          logger.info(`Cleared ${totalMessagesCleared} messages so far...`);
        }
      }
    });

    while (Date.now() - lastMessageTime < timeoutMs) {
      await new Promise(resolve => setTimeout(resolve, 1000));
    }

    logger.info(`Topic '${targetTopic}' cleared successfully!`);
    logger.info(`Total messages cleared: ${totalMessagesCleared}`);
  } catch (error) {
    logger.error('Error clearing topic:', error);
    throw error;
  } finally {
    await consumer.disconnect();
    await admin.disconnect();
  }
}

/**
 * Alternative method: Reset topic by deleting and recreating it
 * WARNING: This completely removes the topic and recreates it
 */
async function resetTopic(
  topicName?: string,
  partitions = 3,
  replicationFactor = 1
): Promise<void> {
  const targetTopic = topicName || config.kafkaTopic;

  if (process.env.NODE_ENV === 'production') {
    logger.error('Topic reset is not allowed in production environment');
    process.exit(1);
  }

  logger.error(`WARNING: This will completely delete and recreate topic: ${targetTopic}`);

  const kafka = createKafka();
  const admin = kafka.admin();

  try {
    await admin.connect();

    const topics = await admin.listTopics();

    if (topics.includes(targetTopic)) {
      logger.info(`Deleting existing topic: ${targetTopic}`);
      await admin.deleteTopics({ topics: [targetTopic] });

      await new Promise(resolve => setTimeout(resolve, 2000));
    }

    logger.info(`Creating new topic: ${targetTopic} with ${partitions} partitions`);
    await admin.createTopics({
      topics: [
        {
          topic: targetTopic,
          numPartitions: partitions,
          replicationFactor,
          configEntries: [
            { name: 'cleanup.policy', value: 'delete' },
            { name: 'retention.ms', value: '86400000' }
          ]
        }
      ]
    });

    logger.info(`Topic '${targetTopic}' reset successfully!`);
  } catch (error) {
    logger.error('Error resetting topic:', error);
    throw error;
  } finally {
    await admin.disconnect();
  }
}

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const command = args[0];
  const topicName = args[1];

  try {
    switch (command) {
      case 'clear':
        await clearTopic(topicName);
        break;

      case 'reset': {
        const partitions = args[2] ? parseInt(args[2]) : 3;
        await resetTopic(topicName, partitions);
        break;
      }

      default:
        logger.info('Usage:');
        logger.info('  npm run clear-topic clear [topic-name]     - Clear all messages from topic');
        logger.info(
          '  npm run clear-topic reset [topic-name] [partitions] - Delete and recreate topic'
        );
        logger.info('');
        logger.info('Examples:');
        logger.info('  npm run clear-topic clear');
        logger.info('  npm run clear-topic clear my-topic');
        logger.info('  npm run clear-topic reset');
        logger.info('  npm run clear-topic reset my-topic 5');
        process.exit(1);
    }
  } catch (error) {
    logger.error('Command failed:', error);
    process.exit(1);
  }
}

if (require.main === module) {
  main().catch(console.error);
}

export { clearTopic, resetTopic };
