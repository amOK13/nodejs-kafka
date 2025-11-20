import { createKafka } from '../common/kafkaClient';
import { logger } from '../common/logger';
import { config } from '../common/config';

/**
 * Utility to check the number of messages in a Kafka topic
 */
async function checkTopic(topicName?: string): Promise<void> {
  const targetTopic = topicName || config.kafkaTopic;

  logger.info(`Checking topic: ${targetTopic}`);

  const kafka = createKafka();
  const admin = kafka.admin();
  const consumer = kafka.consumer({ groupId: `check-topic-${Date.now()}` });

  try {
    // Connect admin and consumer
    await admin.connect();
    await consumer.connect();

    // Check if topic exists
    const topics = await admin.listTopics();
    if (!topics.includes(targetTopic)) {
      logger.info(`Topic '${targetTopic}' does not exist`);
      return;
    }

    // Get topic metadata
    const metadata = await admin.fetchTopicMetadata({ topics: [targetTopic] });
    const topicMetadata = metadata.topics.find(t => t.name === targetTopic);

    if (!topicMetadata) {
      logger.error(`Could not fetch metadata for topic: ${targetTopic}`);
      return;
    }

    logger.info(`Topic '${targetTopic}' info:`);
    logger.info(`- Partitions: ${topicMetadata.partitions.length}`);

    // Simple method: try to consume a few messages to see if there are any
    await consumer.subscribe({ topic: targetTopic, fromBeginning: true });

    let messageCount = 0;
    const startTime = Date.now();
    const timeoutMs = 3000; // 3 seconds timeout

    logger.info('Counting messages...');

    const runPromise = consumer.run({
      eachMessage: async ({ partition, message }) => {
        messageCount++;
        if (messageCount % 100 === 0) {
          logger.info(`Found ${messageCount} messages so far...`);
        }
      }
    });

    // Wait for either timeout or to finish consuming
    await new Promise(resolve => {
      const checkInterval = setInterval(() => {
        if (Date.now() - startTime > timeoutMs) {
          clearInterval(checkInterval);
          resolve(undefined);
        }
      }, 100);

      // Also resolve if consumer stops naturally (no more messages)
      setTimeout(() => {
        clearInterval(checkInterval);
        resolve(undefined);
      }, timeoutMs);
    });

    logger.info(`Topic '${targetTopic}' contains approximately ${messageCount} messages`);
    if (messageCount === 0) {
      logger.info('Topic is empty!');
    } else {
      logger.info(`📊 Topic contains ${messageCount} messages`);
    }
  } catch (error) {
    logger.error('Error checking topic:', error);
    throw error;
  } finally {
    await consumer.disconnect();
    await admin.disconnect();
  }
}

// CLI interface
async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const topicName = args[0];

  try {
    await checkTopic(topicName);
  } catch (error) {
    logger.error('Command failed:', error);
    process.exit(1);
  }
}

if (require.main === module) {
  main().catch(console.error);
}

export { checkTopic };
