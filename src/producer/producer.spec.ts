import { MessageProducer, ProducerOptions } from './producer';
import { JsonMessageSchema, TextMessageSchema } from '../common/messageValidator';
import { CompressionTypes } from 'kafkajs';

const mockKafka = {
  producer: jest.fn()
};

const mockProducer = {
  connect: jest.fn(),
  disconnect: jest.fn(),
  send: jest.fn()
};

jest.mock('../common/kafkaClient', () => ({
  createKafka: jest.fn(() => mockKafka)
}));

jest.mock('../common/logger', () => ({
  logger: {
    info: jest.fn(),
    error: jest.fn(),
    debug: jest.fn()
  }
}));

const { logger } = require('../common/logger');
const mockLogger = logger as jest.Mocked<typeof logger>;

describe('MessageProducer', () => {
  let producer: MessageProducer;

  beforeEach(() => {
    jest.clearAllMocks();
    mockKafka.producer.mockReturnValue(mockProducer);
    mockProducer.connect.mockResolvedValue(undefined);
    mockProducer.disconnect.mockResolvedValue(undefined);
    mockProducer.send.mockResolvedValue(undefined);
  });

  afterEach(async () => {
    if (producer) {
      await producer.disconnect();
    }
  });

  describe('Constructor', () => {
    test('should create producer with default options', () => {
      producer = new MessageProducer();

      expect(producer).toBeInstanceOf(MessageProducer);
    });

    test('should create producer with custom options', () => {
      const options: ProducerOptions = {
        serializationFormat: 'string',
        enableValidation: false,
        schema: new JsonMessageSchema(),
        preset: 'highThroughput'
      };

      producer = new MessageProducer(options);

      expect(producer).toBeInstanceOf(MessageProducer);
    });

    test('should create producer with custom configuration', () => {
      const options: ProducerOptions = {
        config: {
          compression: { type: CompressionTypes.GZIP },
          batching: { maxBatchSize: 8192, lingerMs: 50 },
          timeout: { requestTimeoutMs: 15000 }
        }
      };

      producer = new MessageProducer(options);

      expect(producer).toBeInstanceOf(MessageProducer);
    });
  });

  describe('Initialization', () => {
    test('should initialize producer successfully', async () => {
      producer = new MessageProducer();

      await producer.initialize();

      expect(mockKafka.producer).toHaveBeenCalledTimes(1);
      expect(mockProducer.connect).toHaveBeenCalledTimes(1);
      expect(mockLogger.info).toHaveBeenCalledWith(
        'Enhanced producer initialized',
        expect.any(Object)
      );
    });

    test('should initialize producer with preset configuration', async () => {
      producer = new MessageProducer({ preset: 'lowLatency' });

      await producer.initialize();

      expect(mockKafka.producer).toHaveBeenCalledTimes(1);
      expect(mockProducer.connect).toHaveBeenCalledTimes(1);
    });

    test('should handle initialization errors', async () => {
      producer = new MessageProducer();
      mockProducer.connect.mockRejectedValue(new Error('Connection failed'));

      await expect(producer.initialize()).rejects.toThrow('Connection failed');
    });
  });

  describe('Message Sending', () => {
    beforeEach(async () => {
      producer = new MessageProducer({
        enableValidation: true,
        schema: new JsonMessageSchema()
      });
      await producer.initialize();
    });

    test('should send valid message successfully', async () => {
      const message = { text: 'Hello World', id: 1 };

      await producer.sendMessage(message);

      expect(mockProducer.send).toHaveBeenCalledTimes(1);
      expect(mockLogger.info).toHaveBeenCalledWith('Message sent successfully', expect.any(Object));
    });

    test('should send message with key and headers', async () => {
      const message = { text: 'Hello World', id: 1 };
      const key = 'message-key';
      const headers = { 'content-type': 'application/json' };

      await producer.sendMessage(message, key, headers);

      expect(mockProducer.send).toHaveBeenCalledWith(
        expect.objectContaining({
          messages: expect.arrayContaining([
            expect.objectContaining({
              key: expect.any(Buffer),
              headers: expect.any(Object)
            })
          ])
        })
      );
    });

    test('should reject invalid message when validation is enabled', async () => {
      producer = new MessageProducer({
        enableValidation: true,
        schema: new TextMessageSchema()
      });
      await producer.initialize();

      const invalidMessage = { not: 'a string' };

      await expect(producer.sendMessage(invalidMessage)).rejects.toThrow(
        'Message validation failed'
      );
      expect(mockProducer.send).not.toHaveBeenCalled();
      const { logger } = require('../common/logger');
      expect(logger.error).toHaveBeenCalledWith('Message validation failed', expect.any(Object));
    });

    test('should send invalid message when validation is disabled', async () => {
      producer = new MessageProducer({
        enableValidation: false,
        schema: new TextMessageSchema()
      });
      await producer.initialize();

      const invalidMessage = { not: 'a string' };

      await producer.sendMessage(invalidMessage);

      expect(mockProducer.send).toHaveBeenCalledTimes(1);
    });

    test('should handle send errors', async () => {
      const message = { text: 'Hello World' };
      mockProducer.send.mockRejectedValue(new Error('Send failed'));

      await expect(producer.sendMessage(message)).rejects.toThrow('Send failed');
      expect(mockLogger.error).toHaveBeenCalledWith('Failed to send message', expect.any(Object));
    });
  });

  describe('Batch Sending', () => {
    beforeEach(async () => {
      producer = new MessageProducer({
        enableValidation: true,
        schema: new JsonMessageSchema(),
        config: {
          batching: { maxBatchSize: 2, lingerMs: 10 }
        }
      });
      await producer.initialize();
    });

    test('should send batch of messages successfully', async () => {
      const messages = [
        { message: { text: 'Message 1' }, key: 'key1' },
        { message: { text: 'Message 2' }, key: 'key2' },
        { message: { text: 'Message 3' }, key: 'key3' }
      ];

      await producer.sendBatch(messages);

      expect(mockProducer.send).toHaveBeenCalledTimes(2);
      expect(mockLogger.info).toHaveBeenCalledWith(
        'Batch messages sent successfully',
        expect.objectContaining({ messageCount: 3 })
      );
    });

    test('should validate all messages in batch', async () => {
      producer = new MessageProducer({
        enableValidation: true,
        schema: new TextMessageSchema(),
        serializationFormat: 'string'
      });
      await producer.initialize();

      const messages = [
        { message: 'Valid message 1' },
        { message: 123 },
        { message: 'Valid message 3' }
      ];

      await expect(producer.sendBatch(messages)).rejects.toThrow('Batch message validation failed');
      expect(mockProducer.send).not.toHaveBeenCalled();
    });

    test('should handle batch send errors', async () => {
      const messages = [{ message: { text: 'Message 1' } }, { message: { text: 'Message 2' } }];

      mockProducer.send.mockRejectedValue(new Error('Batch send failed'));

      await expect(producer.sendBatch(messages)).rejects.toThrow('Batch send failed');
      expect(mockLogger.error).toHaveBeenCalledWith(
        'Failed to send batch messages',
        expect.any(Object)
      );
    });
  });

  describe('Configuration Management', () => {
    beforeEach(async () => {
      producer = new MessageProducer();
      await producer.initialize();
    });

    test('should update schema', () => {
      const newSchema = new JsonMessageSchema();

      producer.updateSchema(newSchema);

      const { logger } = require('../common/logger');
      expect(logger.info).toHaveBeenCalledWith('Producer schema updated');
    });

    test('should enable/disable validation', () => {
      producer.setValidationEnabled(false);
      expect(mockLogger.info).toHaveBeenCalledWith('Producer validation status changed', {
        enabled: false
      });

      producer.setValidationEnabled(true);
      expect(mockLogger.info).toHaveBeenCalledWith('Producer validation status changed', {
        enabled: true
      });
    });

    test('should update configuration', () => {
      const newConfig = {
        compression: { type: CompressionTypes.LZ4 },
        batching: { maxBatchSize: 1024, lingerMs: 5 }
      };

      producer.updateConfiguration(newConfig);

      expect(mockLogger.info).toHaveBeenCalledWith('Producer configuration updated', {
        config: newConfig
      });
    });

    test('should get current configuration', () => {
      const config = producer.getConfiguration();

      expect(config).toHaveProperty('compression');
      expect(config).toHaveProperty('batching');
      expect(config).toHaveProperty('timeout');
      expect(typeof config.batching.maxBatchSize).toBe('number');
      expect(typeof config.batching.lingerMs).toBe('number');
      expect(typeof config.timeout).toBe('number');
    });
  });

  describe('Cleanup', () => {
    test('should disconnect producer', async () => {
      producer = new MessageProducer();
      await producer.initialize();

      await producer.disconnect();

      expect(mockProducer.disconnect).toHaveBeenCalledTimes(1);
      expect(mockLogger.info).toHaveBeenCalledWith('Producer disconnected');
    });

    test('should handle disconnect when producer not initialized', async () => {
      producer = new MessageProducer();

      await producer.disconnect();

      expect(mockProducer.disconnect).not.toHaveBeenCalled();
    });
  });

  describe('Message Chunking', () => {
    test('should chunk messages correctly', () => {
      producer = new MessageProducer();

      const chunkMessages = (producer as any).chunkMessages;
      const messages = [1, 2, 3, 4, 5, 6, 7];
      const chunks = chunkMessages(messages, 3);

      expect(chunks).toEqual([[1, 2, 3], [4, 5, 6], [7]]);
    });

    test('should handle empty array', () => {
      producer = new MessageProducer();

      const chunkMessages = (producer as any).chunkMessages;
      const chunks = chunkMessages([], 3);

      expect(chunks).toEqual([]);
    });

    test('should handle chunk size larger than array', () => {
      producer = new MessageProducer();

      const chunkMessages = (producer as any).chunkMessages;
      const messages = [1, 2];
      const chunks = chunkMessages(messages, 5);

      expect(chunks).toEqual([[1, 2]]);
    });
  });
});
