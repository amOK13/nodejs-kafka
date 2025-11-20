import { createKafka, createProducer, createConsumer } from './kafkaClient';

jest.mock('kafkajs', () => ({
  Kafka: jest.fn(),
  logLevel: {
    ERROR: 'ERROR',
    WARN: 'WARN',
    INFO: 'INFO'
  }
}));

jest.mock('./logger', () => ({
  logger: {
    info: jest.fn(),
    error: jest.fn(),
    debug: jest.fn()
  }
}));

const { Kafka } = require('kafkajs');

const mockKafkaInstance = {
  producer: jest.fn(),
  consumer: jest.fn()
};

const mockProducer = {
  connect: jest.fn(),
  send: jest.fn(),
  disconnect: jest.fn()
};

const mockConsumer = {
  connect: jest.fn(),
  subscribe: jest.fn(),
  run: jest.fn(),
  disconnect: jest.fn()
};

describe('KafkaClient', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    Kafka.mockImplementation(() => mockKafkaInstance);
    mockKafkaInstance.producer.mockReturnValue(mockProducer);
    mockKafkaInstance.consumer.mockReturnValue(mockConsumer);
  });

  describe('createKafka', () => {
    test('should create Kafka instance with correct configuration', () => {
      const kafka = createKafka();

      expect(Kafka).toHaveBeenCalledWith(
        expect.objectContaining({
          clientId: expect.any(String),
          brokers: expect.any(Array)
        })
      );
      expect(kafka).toBe(mockKafkaInstance);
    });

    test('should use configuration from config module', () => {
      createKafka();

      const kafkaCall = Kafka.mock.calls[0][0];
      expect(kafkaCall.brokers).toBeDefined();
      expect(Array.isArray(kafkaCall.brokers)).toBe(true);
      expect(kafkaCall.clientId).toBeDefined();
    });
  });

  describe('createProducer', () => {
    test('should create and connect producer', async () => {
      mockProducer.connect.mockResolvedValue(undefined);

      const producer = await createProducer();

      expect(mockKafkaInstance.producer).toHaveBeenCalledTimes(1);
      expect(mockProducer.connect).toHaveBeenCalledTimes(1);
      expect(producer).toBe(mockProducer);
    });

    test('should handle producer connection errors', async () => {
      const connectionError = new Error('Producer connection failed');
      mockProducer.connect.mockRejectedValue(connectionError);

      await expect(createProducer()).rejects.toThrow('Producer connection failed');

      expect(mockKafkaInstance.producer).toHaveBeenCalledTimes(1);
      expect(mockProducer.connect).toHaveBeenCalledTimes(1);
    });

    test('should create producer with default configuration', async () => {
      mockProducer.connect.mockResolvedValue(undefined);

      await createProducer();

      expect(mockKafkaInstance.producer).toHaveBeenCalledWith();
    });
  });

  describe('createConsumer', () => {
    test('should create and connect consumer with default group ID', async () => {
      mockConsumer.connect.mockResolvedValue(undefined);

      const consumer = await createConsumer();

      expect(mockKafkaInstance.consumer).toHaveBeenCalledWith(
        expect.objectContaining({
          groupId: expect.any(String)
        })
      );
      expect(mockConsumer.connect).toHaveBeenCalledTimes(1);
      expect(consumer).toBe(mockConsumer);
    });

    test('should create consumer with custom group ID', async () => {
      const customGroupId = 'custom-group';
      mockConsumer.connect.mockResolvedValue(undefined);

      await createConsumer(customGroupId);

      expect(mockKafkaInstance.consumer).toHaveBeenCalledWith(
        expect.objectContaining({
          groupId: customGroupId
        })
      );
    });

    test('should handle consumer connection errors', async () => {
      const connectionError = new Error('Consumer connection failed');
      mockConsumer.connect.mockRejectedValue(connectionError);

      await expect(createConsumer()).rejects.toThrow('Consumer connection failed');

      expect(mockKafkaInstance.consumer).toHaveBeenCalledTimes(1);
      expect(mockConsumer.connect).toHaveBeenCalledTimes(1);
    });

    test('should use only groupId in consumer configuration', async () => {
      const customGroupId = 'test-group';
      mockConsumer.connect.mockResolvedValue(undefined);

      await createConsumer(customGroupId);

      expect(mockKafkaInstance.consumer).toHaveBeenCalledWith({
        groupId: customGroupId
      });
    });
  });

  describe('Error handling and logging', () => {
    test('should log producer creation', async () => {
      mockProducer.connect.mockResolvedValue(undefined);

      await createProducer();

      const { logger } = require('./logger');
      expect(logger.info).toHaveBeenCalledWith('Producer connected successfully');
    });

    test('should log consumer creation', async () => {
      mockConsumer.connect.mockResolvedValue(undefined);

      await createConsumer();

      const { logger } = require('./logger');
      expect(logger.info).toHaveBeenCalledWith('Consumer connected successfully', expect.any(Object));
    });

    test('should log producer connection errors', async () => {
      const error = new Error('Connection failed');
      mockProducer.connect.mockRejectedValue(error);

      await expect(createProducer()).rejects.toThrow();

      const { logger } = require('./logger');
      expect(logger.error).toHaveBeenCalledWith('Failed to create and connect producer', expect.any(Object));
    });

    test('should log consumer connection errors', async () => {
      const error = new Error('Connection failed');
      mockConsumer.connect.mockRejectedValue(error);

      await expect(createConsumer()).rejects.toThrow();

      const { logger } = require('./logger');
      expect(logger.error).toHaveBeenCalledWith('Failed to create and connect consumer', expect.any(Object));
    });
  });
});
