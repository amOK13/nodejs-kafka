import { MessageProducer } from '../producer/producer';
import { MessageRouter, createContentBasedRule } from '../common/messageRouter';
import { PartitionerFactory } from '../common/partitioners';
import { createMetadataManager } from '../common/metadataManager';
import { JsonMessageSchema } from '../common/messageValidator';

const mockProducer = {
  connect: jest.fn().mockResolvedValue(undefined),
  send: jest.fn().mockResolvedValue(undefined),
  disconnect: jest.fn().mockResolvedValue(undefined),
  transaction: jest.fn().mockResolvedValue({
    commit: jest.fn().mockResolvedValue(undefined),
    abort: jest.fn().mockResolvedValue(undefined)
  })
};

jest.mock('kafkajs', () => ({
  Kafka: jest.fn().mockImplementation(() => ({
    producer: jest.fn().mockImplementation(() => mockProducer)
  })),
  CompressionTypes: {
    GZIP: 'gzip',
    SNAPPY: 'snappy'
  }
}));

jest.mock('../common/logger', () => ({
  logger: {
    info: jest.fn(),
    error: jest.fn(),
    debug: jest.fn(),
    warn: jest.fn()
  }
}));

describe('Enhanced Producer Integration Tests (Step 4)', () => {
  let producer: MessageProducer;

  beforeEach(() => {
    jest.clearAllMocks();
  });

  afterEach(async () => {
    if (producer) {
      await producer.disconnect();
    }
  });

  describe('Message Routing Integration', () => {
    test('should route messages based on content', async () => {
      producer = new MessageProducer({
        enableRouting: true,
        defaultRoutingTopic: 'default-topic',
        serializationFormat: 'json',
        schema: new JsonMessageSchema()
      });

      await producer.initialize();

      // Add routing rule
      const rule = createContentBasedRule(
        'user-type-rule',
        'Route by user type',
        'user.type',
        'premium',
        'premium-topic'
      );
      producer.addRoutingRule(rule);

      // Send premium user message
      await producer.sendMessage(
        { user: { type: 'premium', id: 'user123' }, action: 'purchase' },
        'user123'
      );

      expect(mockProducer.send).toHaveBeenCalledWith({
        topic: 'premium-topic',
        compression: undefined,
        timeout: 30000,
        messages: [
          expect.objectContaining({
            key: Buffer.from('user123'),
            value: expect.any(String),
            headers: expect.any(Object)
          })
        ]
      });
    });

    test('should use default topic when no rules match', async () => {
      producer = new MessageProducer({
        enableRouting: true,
        defaultRoutingTopic: 'default-topic',
        serializationFormat: 'json',
        schema: new JsonMessageSchema()
      });

      await producer.initialize();

      const rule = createContentBasedRule(
        'admin-rule',
        'Route admin messages',
        'user.role',
        'admin',
        'admin-topic'
      );
      producer.addRoutingRule(rule);

      await producer.sendMessage(
        { user: { role: 'user' }, action: 'view' },
        'user456'
      );

      expect(mockProducer.send).toHaveBeenCalledWith({
        topic: 'default-topic',
        compression: undefined,
        timeout: 30000,
        messages: [
          expect.objectContaining({
            key: Buffer.from('user456')
          })
        ]
      });
    });

    test('should manage routing rules', async () => {
      producer = new MessageProducer({
        enableRouting: true,
        defaultRoutingTopic: 'default-topic'
      });

      await producer.initialize();

      const rule = createContentBasedRule(
        'test-rule',
        'Test Rule',
        'type',
        'test',
        'test-topic'
      );

      producer.addRoutingRule(rule);
      expect(producer.getRoutingRules()).toHaveLength(1);

      const removed = producer.removeRoutingRule('test-rule');
      expect(removed).toBe(true);
      expect(producer.getRoutingRules()).toHaveLength(0);
    });
  });

  describe('Custom Partitioner Integration', () => {
    test('should use hash partitioner', async () => {
      const hashPartitioner = PartitionerFactory.createHash();

      producer = new MessageProducer({
        partitioner: hashPartitioner,
        serializationFormat: 'string'
      });

      await producer.initialize();

      await producer.sendMessage('test message', 'test-key');

      expect(mockProducer.send).toHaveBeenCalledWith(
        expect.objectContaining({
          messages: [
            expect.objectContaining({
              partition: expect.any(Number),
              key: Buffer.from('test-key')
            })
          ]
        })
      );
    });

    test('should use content-based partitioner', async () => {
      const contentPartitioner = PartitionerFactory.createContentBased('userId');

      producer = new MessageProducer({
        partitioner: contentPartitioner,
        serializationFormat: 'json',
        schema: new JsonMessageSchema()
      });

      await producer.initialize();

      await producer.sendMessage({ userId: 'user123', data: 'test' });

      expect(mockProducer.send).toHaveBeenCalledWith(
        expect.objectContaining({
          messages: [
            expect.objectContaining({
              partition: expect.any(Number)
            })
          ]
        })
      );
    });

    test('should update partitioner', async () => {
      producer = new MessageProducer({});
      await producer.initialize();

      const roundRobinPartitioner = PartitionerFactory.createRoundRobin();
      producer.setPartitioner(roundRobinPartitioner);

      expect(producer.getPartitioner()).toBe(roundRobinPartitioner);
    });
  });

  describe('Enhanced Metadata Integration', () => {
    test('should include metadata in message headers', async () => {
      const defaultMetadata = {
        source: 'test-service',
        version: '1.0.0',
        messageType: 'event'
      };

      producer = new MessageProducer({
        defaultMetadata,
        serializationFormat: 'string'
      });

      await producer.initialize();

      await producer.sendMessage('test message', 'test-key');

      expect(mockProducer.send).toHaveBeenCalledWith(
        expect.objectContaining({
          messages: [
            expect.objectContaining({
              headers: expect.objectContaining({
                'x-msg-source': Buffer.from('test-service'),
                'x-msg-version': Buffer.from('1.0.0'),
                'x-msg-messageType': Buffer.from('event'),
                'x-msg-messageId': expect.any(Buffer),
                'x-msg-correlationId': expect.any(Buffer),
                'x-msg-timestamp': expect.any(Buffer)
              })
            })
          ]
        })
      );
    });

    test('should override default metadata', async () => {
      producer = new MessageProducer({
        defaultMetadata: { source: 'default-service', priority: 5 },
        serializationFormat: 'string'
      });

      await producer.initialize();

      const customMetadata = {
        source: 'custom-service',
        userId: 'user123'
      };

      await producer.sendMessage('test', 'key', {}, customMetadata);

      expect(mockProducer.send).toHaveBeenCalledWith(
        expect.objectContaining({
          messages: [
            expect.objectContaining({
              headers: expect.objectContaining({
                'x-msg-source': Buffer.from('custom-service'),
                'x-msg-userId': Buffer.from('user123'),
                'x-msg-priority': Buffer.from('5')
              })
            })
          ]
        })
      );
    });

    test('should update default metadata', async () => {
      producer = new MessageProducer({
        serializationFormat: 'string'
      });
      await producer.initialize();

      producer.setDefaultMetadata({
        source: 'updated-service',
        version: '2.0.0'
      });

      await producer.sendMessage('test');

      expect(mockProducer.send).toHaveBeenCalledWith(
        expect.objectContaining({
          messages: [
            expect.objectContaining({
              headers: expect.objectContaining({
                'x-msg-source': Buffer.from('updated-service'),
                'x-msg-version': Buffer.from('2.0.0')
              })
            })
          ]
        })
      );
    });

    test('should enrich metadata', async () => {
      producer = new MessageProducer({});
      await producer.initialize();

      const baseMetadata = { source: 'base-service', priority: 3 };
      const enrichments = { userId: 'user123', priority: 8 };

      const enriched = producer.enrichMetadata(baseMetadata, enrichments);

      expect(enriched.source).toBe('base-service');
      expect(enriched.priority).toBe(8);
      expect(enriched.userId).toBe('user123');
    });
  });

  describe('Transaction Integration', () => {
    test('should execute transaction successfully', async () => {
      producer = new MessageProducer({
        enableTransactions: true,
        serializationFormat: 'string'
      });

      await producer.initialize();

      const result = await producer.sendInTransaction(async (sender) => {
        await sender([
          {
            topic: 'topic1',
            value: Buffer.from('message1'),
            key: 'key1'
          },
          {
            topic: 'topic2',
            value: Buffer.from('message2')
          }
        ]);
        return 'transaction-result';
      });

      expect(result).toBe('transaction-result');
      expect(mockProducer.send).toHaveBeenCalledTimes(2);
    });

    test('should throw error when transactions not enabled', async () => {
      producer = new MessageProducer({
        enableTransactions: false
      });

      await producer.initialize();

      await expect(producer.sendInTransaction(async () => 'test')).rejects.toThrow(
        'Transactions are not enabled'
      );
    });

    test('should get active transaction id', async () => {
      producer = new MessageProducer({
        enableTransactions: true
      });

      await producer.initialize();

      expect(producer.getActiveTransactionId()).toBeNull();
    });
  });

  describe('Combined Step 4 Features', () => {
    test('should use routing, partitioning, metadata and validation together', async () => {
      const hashPartitioner = PartitionerFactory.createHash();

      producer = new MessageProducer({
        enableRouting: true,
        defaultRoutingTopic: 'default-topic',
        partitioner: hashPartitioner,
        defaultMetadata: {
          source: 'integration-test',
          version: '1.0.0'
        },
        enableValidation: false,
        serializationFormat: 'json',
        schema: new JsonMessageSchema()
      });

      await producer.initialize();

      // Add routing rule
      const rule = createContentBasedRule(
        'priority-rule',
        'High Priority Messages',
        'priority',
        'high',
        'priority-topic'
      );
      producer.addRoutingRule(rule);

      // Send message that matches routing rule
      await producer.sendMessage(
        { priority: 'high', message: 'High priority message' },
        'msg-key',
        { 'custom-header': 'custom-value' },
        { userId: 'user123', priority: 9 }
      );

      expect(mockProducer.send).toHaveBeenCalledWith({
        topic: 'priority-topic',
        compression: undefined,
        timeout: 30000,
        messages: [
          {
            partition: expect.any(Number),
            key: Buffer.from('msg-key'),
            value: expect.any(String),
            headers: expect.objectContaining({
              'custom-header': Buffer.from('custom-value'),
              'x-msg-source': Buffer.from('integration-test'),
              'x-msg-version': Buffer.from('1.0.0'),
              'x-msg-userId': Buffer.from('user123'),
              'x-msg-priority': Buffer.from('9')
            }),
            timestamp: expect.any(String)
          }
        ]
      });
    });
  });

  describe('Error Handling', () => {
    test('should throw error when routing not enabled but trying to add rules', async () => {
      producer = new MessageProducer({ enableRouting: false });
      await producer.initialize();

      expect(() => {
        producer.addRoutingRule({
          id: 'test',
          name: 'Test',
          condition: () => true,
          targetTopic: 'test',
          priority: 100
        });
      }).toThrow('Routing is not enabled');
    });
  });
});
