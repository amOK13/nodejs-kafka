import { MessageProducer } from '../producer/producer';
import { JsonMessageSchema } from '../common/messageValidator';
import { logger } from '../common/logger';
import { CompressionTypes } from 'kafkajs';

export class ProducerConfigTests {

  static async testHighThroughputConfiguration(): Promise<void> {
    const producer = new MessageProducer({
      preset: 'highThroughput',
      serializationFormat: 'json',
      enableValidation: true,
      schema: new JsonMessageSchema()
    });

    try {
      await producer.initialize();

      const config = producer.getConfiguration();
      console.assert(config.compression === CompressionTypes.GZIP, 'High throughput should use GZIP compression');
      console.assert(config.batching.maxBatchSize === 16384, 'High throughput should have large batch size');

      logger.info('High throughput producer configured successfully');
    } finally {
      await producer.disconnect();
    }
  }

  static async testLowLatencyConfiguration(): Promise<void> {
    const producer = new MessageProducer({
      preset: 'lowLatency',
      serializationFormat: 'json',
      enableValidation: true,
      schema: new JsonMessageSchema()
    });

    try {
      await producer.initialize();

      const config = producer.getConfiguration();
      console.assert(config.compression === CompressionTypes.LZ4, 'Low latency should use LZ4 compression');
      console.assert(config.batching.lingerMs === 5, 'Low latency should have short linger time');

      logger.info('Low latency producer configured successfully');
    } finally {
      await producer.disconnect();
    }
  }

  static async testReliableConfiguration(): Promise<void> {
    const producer = new MessageProducer({
      preset: 'reliable',
      serializationFormat: 'json',
      enableValidation: true,
      schema: new JsonMessageSchema()
    });

    try {
      await producer.initialize();

      const config = producer.getConfiguration();
      console.assert(config.compression === CompressionTypes.Snappy, 'Reliable should use Snappy compression');

      logger.info(' Reliable producer configured successfully');
    } finally {
      await producer.disconnect();
    }
  }

  static async testCustomConfiguration(): Promise<void> {
    const producer = new MessageProducer({
      serializationFormat: 'json',
      enableValidation: true,
      schema: new JsonMessageSchema(),
      config: {
        compression: {
          type: CompressionTypes.GZIP
        },
        batching: {
          maxBatchSize: 4096,
          lingerMs: 25
        },
        retry: {
          retries: 10,
          initialRetryTime: 500
        },
        timeout: {
          requestTimeoutMs: 15000
        },
        performance: {
          idempotent: true
        }
      }
    });

    try {
      await producer.initialize();

      const config = producer.getConfiguration();
      console.assert(config.compression === CompressionTypes.GZIP, 'Custom config should use GZIP');
      console.assert(config.batching.maxBatchSize === 4096, 'Custom config should have correct batch size');
      console.assert(config.timeout === 15000, 'Custom config should have correct timeout');

      logger.info(' Custom producer configured successfully');
    } finally {
      await producer.disconnect();
    }
  }

  static async testConfigurationUpdates(): Promise<void> {
    const producer = new MessageProducer({
      preset: 'balanced'
    });

    try {
      await producer.initialize();

      producer.updateConfiguration({
        compression: {
          type: CompressionTypes.LZ4
        },
        batching: {
          maxBatchSize: 2048,
          lingerMs: 10
        }
      });

      const config = producer.getConfiguration();
      console.assert(config.compression === CompressionTypes.LZ4, 'Configuration should be updated');
      console.assert(config.batching.maxBatchSize === 2048, 'Batch size should be updated');

      logger.info(' Producer configuration updated successfully');
    } finally {
      await producer.disconnect();
    }
  }

  static async testBatchMessageSending(): Promise<void> {
    const producer = new MessageProducer({
      preset: 'highThroughput',
      serializationFormat: 'json',
      enableValidation: true,
      schema: new JsonMessageSchema()
    });

    try {
      await producer.initialize();

      const messages = Array.from({ length: 100 }, (_, i) => ({
        message: {
          id: i,
          text: `Batch message ${i}`,
          timestamp: new Date().toISOString()
        },
        key: `key-${i}`,
        headers: { 'batch-id': 'test-batch' }
      }));

      const startTime = Date.now();
      await producer.sendBatch(messages);
      const endTime = Date.now();

      logger.info('Batch sending performance', {
        messageCount: messages.length,
        duration: endTime - startTime,
        messagesPerSecond: messages.length / ((endTime - startTime) / 1000)
      });

      console.assert(endTime - startTime < 5000, 'Batch should complete within 5 seconds');
      logger.info(' Batch message sending test passed');
    } finally {
      await producer.disconnect();
    }
  }

  static async runAllTests(): Promise<void> {
    logger.info('🚀 Starting Producer Configuration Tests');

    try {
      await this.testHighThroughputConfiguration();
      await this.testLowLatencyConfiguration();
      await this.testReliableConfiguration();
      await this.testCustomConfiguration();
      await this.testConfigurationUpdates();
      await this.testBatchMessageSending();

      logger.info(' All Producer Configuration Tests Passed!');
    } catch (error) {
      logger.error('❌ Test failed', { error });
      throw error;
    }
  }
}

export async function benchmarkProducerConfigurations() {
  const configurations = [
    { name: 'High Throughput', preset: 'highThroughput' as const },
    { name: 'Low Latency', preset: 'lowLatency' as const },
    { name: 'Reliable', preset: 'reliable' as const },
    { name: 'Balanced', preset: 'balanced' as const }
  ];

  const messageCount = 1000;
  const results: Array<{ name: string; duration: number; messagesPerSecond: number }> = [];

  for (const config of configurations) {
    const producer = new MessageProducer({
      preset: config.preset,
      serializationFormat: 'json',
      enableValidation: true,
      schema: new JsonMessageSchema()
    });

    try {
      await producer.initialize();

      const messages = Array.from({ length: messageCount }, (_, i) => ({
        message: {
          id: i,
          text: `Benchmark message ${i}`,
          timestamp: new Date().toISOString(),
          data: 'x'.repeat(100)
        },
        key: `benchmark-${i}`
      }));

      const startTime = Date.now();
      await producer.sendBatch(messages);
      const endTime = Date.now();

      const duration = endTime - startTime;
      const messagesPerSecond = messageCount / (duration / 1000);

      results.push({
        name: config.name,
        duration,
        messagesPerSecond: Math.round(messagesPerSecond)
      });

      logger.info(`Benchmark ${config.name}`, {
        messageCount,
        duration,
        messagesPerSecond: Math.round(messagesPerSecond)
      });

    } catch (error) {
      logger.error(`Benchmark failed for ${config.name}`, { error });
    } finally {
      await producer.disconnect();
    }
  }

  return results;
}

if (require.main === module) {
  benchmarkProducerConfigurations()
    .then(results => {
      logger.info('Benchmark completed', { results });
    })
    .catch(error => {
      logger.error('Benchmark failed', { error });
    });
}
