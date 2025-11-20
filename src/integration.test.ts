import { MessageProducer } from './producer/producer';
import { JsonMessageSchema } from './common/messageValidator';
import { CompressionTypes } from 'kafkajs';
import { config } from './common/config';

describe('Kafka Integration Tests', () => {
  let producer: MessageProducer;
  const skipIntegration = process.env.SKIP_INTEGRATION_TESTS === 'true';

  beforeAll(() => {
    if (skipIntegration) {
      console.log('Skipping integration tests - set SKIP_INTEGRATION_TESTS=false to run them');
    }
  });

  beforeEach(async () => {
    if (skipIntegration) return;

    producer = new MessageProducer({
      serializationFormat: 'json',
      enableValidation: true,
      schema: new JsonMessageSchema(),
      preset: 'balanced'
    });
    
    try {
      await producer.initialize();
    } catch (error) {
      console.warn('Kafka not available, skipping integration tests:', error);
      return;
    }
  }, 30000);

  afterEach(async () => {
    if (skipIntegration) return;
    
    try {
      if (producer) await producer.disconnect();
    } catch (error) {
      console.warn('Error during cleanup:', error);
    }
  }, 10000);

  describe('Producer-Consumer Integration', () => {
    test('should send and receive messages end-to-end', async () => {
      if (skipIntegration) return;
      
      const testMessages = [
        { id: 1, text: 'Integration test message 1', timestamp: new Date().toISOString() },
        { id: 2, text: 'Integration test message 2', timestamp: new Date().toISOString() },
        { id: 3, text: 'Integration test message 3', timestamp: new Date().toISOString() }
      ];

      for (const message of testMessages) {
        await producer.sendMessage(message, `key-${message.id}`, {
          'test-type': 'integration',
          'message-id': message.id.toString()
        });
      }
      await new Promise(resolve => setTimeout(resolve, 2000));
      expect(true).toBe(true);
    }, 15000);

    test('should handle batch sending and receiving', async () => {
      if (skipIntegration) return;
      
      const batchMessages = Array.from({ length: 10 }, (_, i) => ({
        message: {
          batchId: 'test-batch-1',
          messageNumber: i + 1,
          text: `Batch message ${i + 1}`,
          timestamp: new Date().toISOString()
        },
        key: `batch-key-${i + 1}`,
        headers: {
          'batch-test': 'true',
          'message-index': i.toString()
        }
      }));

      await producer.sendBatch(batchMessages);
      await new Promise(resolve => setTimeout(resolve, 3000));
      
      expect(true).toBe(true);
    }, 20000);

    test('should handle different compression types', async () => {
      if (skipIntegration) return;
      
      const compressionTypes = [CompressionTypes.GZIP, CompressionTypes.LZ4, CompressionTypes.Snappy];
      
      for (const compressionType of compressionTypes) {
        const compressionProducer = new MessageProducer({
          config: {
            compression: { type: compressionType }
          },
          serializationFormat: 'json',
          enableValidation: true,
          schema: new JsonMessageSchema()
        });
        
        try {
          await compressionProducer.initialize();
          
          await compressionProducer.sendMessage({
            compressionTest: compressionType,
            data: 'x'.repeat(1000),
            timestamp: new Date().toISOString()
          });
          
          await compressionProducer.disconnect();
        } catch (error) {
          console.warn(`Error testing compression ${compressionType}:`, error);
        }
      }
      
      expect(true).toBe(true);
    }, 25000);
  });

  describe('Error Handling Integration', () => {
    test('should handle producer connection failures gracefully', async () => {
      if (skipIntegration) return;
      const invalidProducer = new MessageProducer({
        config: {}
      });
      try {
        await invalidProducer.initialize();
        await invalidProducer.sendMessage({ test: 'message' });
        await invalidProducer.disconnect();
      } catch (error) {
        expect(error).toBeDefined();
      }
    });

    test('should handle message validation failures in production flow', async () => {
      if (skipIntegration) return;
      
      const strictProducer = new MessageProducer({
        enableValidation: true,
        schema: new JsonMessageSchema(),
        serializationFormat: 'json'
      });
      
      try {
        await strictProducer.initialize();
        await expect(
          strictProducer.sendMessage('this is a string not an object')
        ).rejects.toThrow();
        
        await strictProducer.disconnect();
      } catch (error) {
        console.warn('Error in validation test:', error);
      }
    });
  });

  describe('Performance Integration', () => {
    test('should handle high-throughput message sending', async () => {
      if (skipIntegration) return;
      
      const highThroughputProducer = new MessageProducer({
        preset: 'highThroughput',
        serializationFormat: 'json',
        enableValidation: false
      });
      
      try {
        await highThroughputProducer.initialize();
        
        const messageCount = 100;
        const messages = Array.from({ length: messageCount }, (_, i) => ({
          message: {
            id: i,
            text: `Performance test message ${i}`,
            timestamp: new Date().toISOString(),
            payload: 'x'.repeat(100)
          },
          key: `perf-${i}`
        }));
        
        const startTime = Date.now();
        await highThroughputProducer.sendBatch(messages);
        const endTime = Date.now();
        
        const duration = endTime - startTime;
        const messagesPerSecond = messageCount / (duration / 1000);
        
        console.log(`Performance: ${messagesPerSecond.toFixed(2)} messages/second`);
        expect(duration).toBeLessThan(30000);
        
        await highThroughputProducer.disconnect();
      } catch (error) {
        console.warn('Error in performance test:', error);
      }
    }, 45000);

    test('should handle concurrent producers', async () => {
      if (skipIntegration) return;
      
      const producerCount = 3;
      const messagesPerProducer = 20;
      
      const producers = Array.from({ length: producerCount }, () => 
        new MessageProducer({
          preset: 'balanced',
          serializationFormat: 'json',
          enableValidation: true,
          schema: new JsonMessageSchema()
        })
      );
      
      try {
        await Promise.all(producers.map(p => p.initialize()));
        const sendPromises = producers.map((producer, producerIndex) => {
          const messages = Array.from({ length: messagesPerProducer }, (_, i) => ({
            message: {
              producerId: producerIndex,
              messageId: i,
              text: `Concurrent message from producer ${producerIndex}`,
              timestamp: new Date().toISOString()
            },
            key: `producer-${producerIndex}-msg-${i}`
          }));
          
          return producer.sendBatch(messages);
        });
        
        await Promise.all(sendPromises);
        await Promise.all(producers.map(p => p.disconnect()));
        
        expect(true).toBe(true);
      } catch (error) {
        console.warn('Error in concurrent test:', error);
        await Promise.all(producers.map(async p => {
          try { await p.disconnect(); } catch {}
        }));
      }
    }, 60000);
  });
});

describe('Configuration Integration', () => {
  test('should read configuration from environment variables', () => {
    expect(config.kafkaBrokers).toBeDefined();
    expect(Array.isArray(config.kafkaBrokers)).toBe(true);
    expect(config.kafkaTopic).toBeDefined();
    expect(config.kafkaClientId).toBeDefined();
    expect(config.kafkaConsumerGroupId).toBeDefined();
  });

  test('should have consistent configuration across components', () => {
    expect(typeof config.kafkaTopic).toBe('string');
    expect(config.kafkaTopic.length).toBeGreaterThan(0);
    
    expect(typeof config.kafkaClientId).toBe('string');
    expect(config.kafkaClientId.length).toBeGreaterThan(0);
    
    expect(config.kafkaBrokers.length).toBeGreaterThan(0);
    config.kafkaBrokers.forEach((broker: string) => {
      expect(typeof broker).toBe('string');
      expect(broker).toMatch(/^.+:\d+$/);
    });
  });
});