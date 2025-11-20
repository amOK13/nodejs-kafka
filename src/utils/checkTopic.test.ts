import { checkTopic } from './checkTopic';
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
  fetchTopicMetadata: jest.fn()
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

describe('CheckTopic Utility', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    (createKafka as jest.Mock).mockReturnValue(mockKafka);
    mockAdmin.connect.mockResolvedValue(undefined);
    mockAdmin.disconnect.mockResolvedValue(undefined);
    mockAdmin.listTopics.mockResolvedValue(['test-topic', 'other-topic']);
    mockAdmin.fetchTopicMetadata.mockResolvedValue({
      topics: [{ name: 'test-topic', partitions: [{ partitionId: 0 }] }]
    });
    mockConsumer.connect.mockResolvedValue(undefined);
    mockConsumer.disconnect.mockResolvedValue(undefined);
    mockConsumer.subscribe.mockResolvedValue(undefined);
    mockConsumer.run.mockResolvedValue(undefined);
  });

  describe('checkTopic', () => {
    test('should connect to kafka and check default topic', async () => {
      await checkTopic();

      expect(mockAdmin.connect).toHaveBeenCalled();
      expect(mockConsumer.connect).toHaveBeenCalled();
      expect(mockAdmin.listTopics).toHaveBeenCalled();
      expect(mockAdmin.fetchTopicMetadata).toHaveBeenCalledWith({ topics: ['test-topic'] });
      expect(mockConsumer.subscribe).toHaveBeenCalledWith({
        topic: 'test-topic',
        fromBeginning: true
      });
      expect(mockConsumer.run).toHaveBeenCalled();
      expect(logger.info).toHaveBeenCalledWith('Checking topic: test-topic');
    });

    test('should check custom topic', async () => {
      mockAdmin.listTopics.mockResolvedValue(['custom-topic', 'other-topic']);
      mockAdmin.fetchTopicMetadata.mockResolvedValue({
        topics: [{ name: 'custom-topic', partitions: [{ partitionId: 0 }] }]
      });

      await checkTopic('custom-topic');

      expect(mockAdmin.fetchTopicMetadata).toHaveBeenCalledWith({ topics: ['custom-topic'] });
      expect(mockConsumer.subscribe).toHaveBeenCalledWith({
        topic: 'custom-topic',
        fromBeginning: true
      });
      expect(logger.info).toHaveBeenCalledWith('Checking topic: custom-topic');
    });

    test('should handle non-existent topic', async () => {
      mockAdmin.listTopics.mockResolvedValue(['other-topic']);

      await checkTopic('non-existent-topic');

      expect(logger.info).toHaveBeenCalledWith("Topic 'non-existent-topic' does not exist");
      expect(mockConsumer.subscribe).not.toHaveBeenCalled();
      expect(mockConsumer.run).not.toHaveBeenCalled();
    });

    test('should handle topic without metadata', async () => {
      mockAdmin.fetchTopicMetadata.mockResolvedValue({
        topics: []
      });

      await checkTopic();

      expect(logger.error).toHaveBeenCalledWith('Could not fetch metadata for topic: test-topic');
      expect(mockConsumer.subscribe).not.toHaveBeenCalled();
      expect(mockConsumer.run).not.toHaveBeenCalled();
    });

    test('should log topic partition info', async () => {
      mockAdmin.fetchTopicMetadata.mockResolvedValue({
        topics: [
          {
            name: 'test-topic',
            partitions: [{ partitionId: 0 }, { partitionId: 1 }, { partitionId: 2 }]
          }
        ]
      });

      await checkTopic();

      expect(logger.info).toHaveBeenCalledWith("Topic 'test-topic' info:");
      expect(logger.info).toHaveBeenCalledWith('- Partitions: 3');
    });

    test('should handle connection errors', async () => {
      const connectionError = new Error('Connection failed');
      mockAdmin.connect.mockRejectedValue(connectionError);

      await expect(checkTopic()).rejects.toThrow('Connection failed');

      expect(logger.error).toHaveBeenCalledWith('Error checking topic:', connectionError);
      expect(mockConsumer.disconnect).toHaveBeenCalled();
      expect(mockAdmin.disconnect).toHaveBeenCalled();
    });

    test('should handle subscribe errors', async () => {
      const subscribeError = new Error('Subscribe failed');
      mockConsumer.subscribe.mockRejectedValue(subscribeError);

      await expect(checkTopic()).rejects.toThrow('Subscribe failed');

      expect(logger.error).toHaveBeenCalledWith('Error checking topic:', subscribeError);
      expect(mockConsumer.disconnect).toHaveBeenCalled();
      expect(mockAdmin.disconnect).toHaveBeenCalled();
    });

    test('should call consumer run method', async () => {
      await checkTopic();

      expect(mockConsumer.run).toHaveBeenCalled();

      // Verify the eachMessage callback is a function
      const runCall = mockConsumer.run.mock.calls[0][0];
      expect(typeof runCall.eachMessage).toBe('function');
    });

    test('should disconnect resources in finally block', async () => {
      await checkTopic();

      expect(mockConsumer.disconnect).toHaveBeenCalled();
      expect(mockAdmin.disconnect).toHaveBeenCalled();
    });

    test('should disconnect resources even on error', async () => {
      const error = new Error('Some error');
      mockAdmin.fetchTopicMetadata.mockRejectedValue(error);

      await expect(checkTopic()).rejects.toThrow('Some error');

      expect(mockConsumer.disconnect).toHaveBeenCalled();
      expect(mockAdmin.disconnect).toHaveBeenCalled();
    });

    test('should log counting messages', async () => {
      await checkTopic();

      expect(logger.info).toHaveBeenCalledWith('Counting messages...');
    });

    test('should create unique consumer group', async () => {
      const originalNow = Date.now;
      Date.now = jest.fn(() => 123456789);

      await checkTopic();

      expect(mockKafka.consumer).toHaveBeenCalledWith({ groupId: 'check-topic-123456789' });

      Date.now = originalNow;
    });
  });
});
