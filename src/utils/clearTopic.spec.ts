import { clearTopic, resetTopic } from './clearTopic';
import { createKafka } from '../common/kafkaClient';
import { logger } from '../common/logger';

// Mock dependencies
jest.mock('../common/kafkaClient');
jest.mock('../common/logger');
jest.mock('../common/config', () => ({
  config: {
    kafkaTopic: 'test-topic'
  }
}));

const mockAdmin = {
  connect: jest.fn(),
  disconnect: jest.fn(),
  listTopics: jest.fn(),
  fetchTopicMetadata: jest.fn(),
  deleteTopics: jest.fn(),
  createTopics: jest.fn()
};

const mockConsumer = {
  connect: jest.fn(),
  disconnect: jest.fn(),
  subscribe: jest.fn(),
  run: jest.fn()
};

const mockKafka = {
  admin: jest.fn(() => mockAdmin),
  consumer: jest.fn(() => mockConsumer)
};

describe('ClearTopic Utility', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    (createKafka as jest.Mock).mockReturnValue(mockKafka);
    mockAdmin.connect.mockResolvedValue(undefined);
    mockAdmin.disconnect.mockResolvedValue(undefined);
    mockConsumer.connect.mockResolvedValue(undefined);
    mockConsumer.disconnect.mockResolvedValue(undefined);
  });

  describe('clearTopic', () => {
    beforeEach(() => {
      mockAdmin.listTopics.mockResolvedValue(['test-topic', 'other-topic']);
      mockAdmin.fetchTopicMetadata.mockResolvedValue({
        topics: [
          {
            name: 'test-topic',
            partitions: [{ partitionId: 0 }]
          }
        ]
      });
      mockConsumer.subscribe.mockResolvedValue(undefined);
    });

    test('should clear messages from default topic successfully', async () => {
      // Mock consumer run to simulate message processing
      let messageCallback: any;
      mockConsumer.run.mockImplementation((config: any) => {
        messageCallback = config.eachMessage;
        // Simulate processing some messages then stopping
        setTimeout(async () => {
          await messageCallback({ partition: 0, message: {} });
          await messageCallback({ partition: 0, message: {} });
          await messageCallback({ partition: 0, message: {} });
        }, 10);
        return Promise.resolve();
      });

      await clearTopic();

      expect(mockAdmin.connect).toHaveBeenCalled();
      expect(mockAdmin.listTopics).toHaveBeenCalled();
      expect(mockAdmin.fetchTopicMetadata).toHaveBeenCalledWith({ topics: ['test-topic'] });
      expect(mockConsumer.subscribe).toHaveBeenCalledWith({
        topic: 'test-topic',
        fromBeginning: true
      });
      expect(mockConsumer.run).toHaveBeenCalled();
      expect(logger.info).toHaveBeenCalledWith('Starting to clear topic: test-topic');
    });

    test('should clear messages from custom topic', async () => {
      mockAdmin.listTopics.mockResolvedValue(['custom-topic', 'other-topic']);
      mockAdmin.fetchTopicMetadata.mockResolvedValue({
        topics: [
          {
            name: 'custom-topic',
            partitions: [{ partitionId: 0 }]
          }
        ]
      });
      mockConsumer.run.mockResolvedValue(undefined);

      await clearTopic('custom-topic');

      expect(mockAdmin.fetchTopicMetadata).toHaveBeenCalledWith({ topics: ['custom-topic'] });
      expect(mockConsumer.subscribe).toHaveBeenCalledWith({
        topic: 'custom-topic',
        fromBeginning: true
      });
      expect(logger.info).toHaveBeenCalledWith('Starting to clear topic: custom-topic');
    });

    test('should handle non-existent topic gracefully', async () => {
      mockAdmin.listTopics.mockResolvedValue(['other-topic']);

      await clearTopic('non-existent-topic');

      expect(logger.info).toHaveBeenCalledWith("Topic 'non-existent-topic' does not exist");
      expect(mockConsumer.subscribe).not.toHaveBeenCalled();
    });

    test('should handle topic with no partitions', async () => {
      mockAdmin.fetchTopicMetadata.mockResolvedValue({
        topics: [
          {
            name: 'test-topic',
            partitions: []
          }
        ]
      });

      await clearTopic();

      expect(logger.info).toHaveBeenCalledWith("Topic 'test-topic' has no partitions");
      expect(mockConsumer.subscribe).not.toHaveBeenCalled();
    });

    test('should exit early in production environment', async () => {
      const originalEnv = process.env.NODE_ENV;
      process.env.NODE_ENV = 'production';
      const mockExit = jest.spyOn(process, 'exit').mockImplementation(() => {
        throw new Error('process.exit called');
      });

      await expect(clearTopic()).rejects.toThrow('process.exit called');

      expect(logger.error).toHaveBeenCalledWith(
        'Topic clearing is not allowed in production environment'
      );
      expect(mockExit).toHaveBeenCalledWith(1);

      process.env.NODE_ENV = originalEnv;
      mockExit.mockRestore();
    });

    test('should handle connection errors gracefully', async () => {
      const connectionError = new Error('Connection failed');
      mockAdmin.connect.mockRejectedValue(connectionError);

      await expect(clearTopic()).rejects.toThrow('Connection failed');

      expect(logger.error).toHaveBeenCalledWith('Error clearing topic:', connectionError);
      expect(mockConsumer.disconnect).toHaveBeenCalled();
      expect(mockAdmin.disconnect).toHaveBeenCalled();
    });

    test('should disconnect resources in finally block even on error', async () => {
      const error = new Error('Fetch metadata failed');
      mockAdmin.fetchTopicMetadata.mockRejectedValue(error);

      await expect(clearTopic()).rejects.toThrow('Fetch metadata failed');

      expect(mockConsumer.disconnect).toHaveBeenCalled();
      expect(mockAdmin.disconnect).toHaveBeenCalled();
    });
  });

  describe('resetTopic', () => {
    beforeEach(() => {
      mockAdmin.listTopics.mockResolvedValue(['test-topic', 'other-topic']);
      mockAdmin.deleteTopics.mockResolvedValue(undefined);
      mockAdmin.createTopics.mockResolvedValue(undefined);
    });

    test('should reset topic with default parameters', async () => {
      await resetTopic();

      expect(logger.error).toHaveBeenCalledWith(
        'WARNING: This will completely delete and recreate topic: test-topic'
      );
      expect(mockAdmin.deleteTopics).toHaveBeenCalledWith({ topics: ['test-topic'] });
      expect(mockAdmin.createTopics).toHaveBeenCalledWith({
        topics: [
          {
            topic: 'test-topic',
            numPartitions: 3,
            replicationFactor: 1,
            configEntries: [
              { name: 'cleanup.policy', value: 'delete' },
              { name: 'retention.ms', value: '86400000' }
            ]
          }
        ]
      });
    });

    test('should reset topic with custom parameters', async () => {
      mockAdmin.listTopics.mockResolvedValue(['custom-topic', 'other-topic']);

      await resetTopic('custom-topic', 5, 2);

      expect(mockAdmin.deleteTopics).toHaveBeenCalledWith({ topics: ['custom-topic'] });
      expect(mockAdmin.createTopics).toHaveBeenCalledWith({
        topics: [
          {
            topic: 'custom-topic',
            numPartitions: 5,
            replicationFactor: 2,
            configEntries: [
              { name: 'cleanup.policy', value: 'delete' },
              { name: 'retention.ms', value: '86400000' }
            ]
          }
        ]
      });
    });

    test('should skip deletion if topic does not exist', async () => {
      mockAdmin.listTopics.mockResolvedValue(['other-topic']);

      await resetTopic('non-existent-topic');

      expect(mockAdmin.deleteTopics).not.toHaveBeenCalled();
      expect(mockAdmin.createTopics).toHaveBeenCalledWith({
        topics: [
          {
            topic: 'non-existent-topic',
            numPartitions: 3,
            replicationFactor: 1,
            configEntries: [
              { name: 'cleanup.policy', value: 'delete' },
              { name: 'retention.ms', value: '86400000' }
            ]
          }
        ]
      });
    });

    test('should exit early in production environment', async () => {
      const originalEnv = process.env.NODE_ENV;
      process.env.NODE_ENV = 'production';
      const mockExit = jest.spyOn(process, 'exit').mockImplementation(() => {
        throw new Error('process.exit called');
      });

      await expect(resetTopic()).rejects.toThrow('process.exit called');

      expect(logger.error).toHaveBeenCalledWith(
        'Topic reset is not allowed in production environment'
      );
      expect(mockExit).toHaveBeenCalledWith(1);

      process.env.NODE_ENV = originalEnv;
      mockExit.mockRestore();
    });

    test('should handle deletion errors gracefully', async () => {
      const deleteError = new Error('Delete failed');
      mockAdmin.deleteTopics.mockRejectedValue(deleteError);

      await expect(resetTopic()).rejects.toThrow('Delete failed');

      expect(logger.error).toHaveBeenCalledWith('Error resetting topic:', deleteError);
      expect(mockAdmin.disconnect).toHaveBeenCalled();
    });

    test('should handle creation errors gracefully', async () => {
      const createError = new Error('Create failed');
      mockAdmin.createTopics.mockRejectedValue(createError);

      await expect(resetTopic()).rejects.toThrow('Create failed');

      expect(logger.error).toHaveBeenCalledWith('Error resetting topic:', createError);
      expect(mockAdmin.disconnect).toHaveBeenCalled();
    });

    test('should wait after deletion before creation', async () => {
      const startTime = Date.now();

      await resetTopic();

      const endTime = Date.now();
      expect(endTime - startTime).toBeGreaterThanOrEqual(2000); // Should wait at least 2 seconds
    });
  });

  describe('Error handling', () => {
    test('should handle consumer run errors', async () => {
      mockAdmin.listTopics.mockResolvedValue(['test-topic']);
      mockAdmin.fetchTopicMetadata.mockResolvedValue({
        topics: [{ name: 'test-topic', partitions: [{ partitionId: 0 }] }]
      });

      const runError = new Error('Consumer run failed');
      mockConsumer.run.mockRejectedValue(runError);

      await expect(clearTopic()).rejects.toThrow('Consumer run failed');

      expect(logger.error).toHaveBeenCalledWith('Error clearing topic:', runError);
    });

    test('should handle subscribe errors', async () => {
      mockAdmin.listTopics.mockResolvedValue(['test-topic']);
      mockAdmin.fetchTopicMetadata.mockResolvedValue({
        topics: [{ name: 'test-topic', partitions: [{ partitionId: 0 }] }]
      });

      const subscribeError = new Error('Subscribe failed');
      mockConsumer.subscribe.mockRejectedValue(subscribeError);

      await expect(clearTopic()).rejects.toThrow('Subscribe failed');

      expect(logger.error).toHaveBeenCalledWith('Error clearing topic:', subscribeError);
    });
  });

  describe('Message counting', () => {
    test('should log progress every 100 messages', async () => {
      mockAdmin.listTopics.mockResolvedValue(['test-topic']);
      mockAdmin.fetchTopicMetadata.mockResolvedValue({
        topics: [{ name: 'test-topic', partitions: [{ partitionId: 0 }] }]
      });
      mockConsumer.subscribe.mockResolvedValue(undefined);

      let messageCallback: any;
      mockConsumer.run.mockImplementation((config: any) => {
        messageCallback = config.eachMessage;
        // Simulate processing 250 messages
        setTimeout(async () => {
          for (let i = 1; i <= 250; i++) {
            await messageCallback({ partition: 0, message: {} });
          }
        }, 10);
        return Promise.resolve();
      });

      await clearTopic();

      expect(logger.info).toHaveBeenCalledWith('Cleared 100 messages so far...');
      expect(logger.info).toHaveBeenCalledWith('Cleared 200 messages so far...');
      expect(logger.info).not.toHaveBeenCalledWith('Cleared 250 messages so far...');
    });
  });
});
