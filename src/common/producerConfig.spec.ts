import { ProducerConfigBuilder, ProducerPresets, EnhancedProducerConfig } from './producerConfig';
import { CompressionTypes } from 'kafkajs';

describe('ProducerConfig', () => {
  describe('ProducerConfigBuilder', () => {
    test('should create default configuration', () => {
      const builder = new ProducerConfigBuilder();
      const config = builder.build();

      expect(config).toBeDefined();
      expect(typeof config).toBe('object');
    });

    test('should build configuration with compression', () => {
      const builder = new ProducerConfigBuilder();
      builder.withCompression(CompressionTypes.GZIP);
      const config = builder.build();

      expect(config).toBeDefined();
    });

    test('should build configuration with batching', () => {
      const builder = new ProducerConfigBuilder();
      builder.withBatching(8192, 50, 5);
      const config = builder.build();

      expect(config.maxInFlightRequests).toBe(5);
    });

    test('should build configuration with retries', () => {
      const builder = new ProducerConfigBuilder();
      builder.withRetries(10, 500, 30000);
      const config = builder.build();

      expect(config.retry?.retries).toBe(10);
      expect(config.retry?.initialRetryTime).toBe(500);
      expect(config.retry?.maxRetryTime).toBe(30000);
    });

    test('should build configuration with timeout', () => {
      const builder = new ProducerConfigBuilder();
      builder.withTimeout(25000, 1);
      const config = builder.build();

      expect(config).toBeDefined();
    });

    test('should build configuration with performance settings', () => {
      const builder = new ProducerConfigBuilder();
      builder.withPerformance(true, 120000);
      const config = builder.build();

      expect(config.idempotent).toBe(true);
      expect(config.transactionTimeout).toBe(120000);
    });

    test('should chain configuration methods', () => {
      const config = new ProducerConfigBuilder()
        .withCompression(CompressionTypes.LZ4)
        .withBatching(4096, 25, 3)
        .withRetries(5, 200, 15000)
        .withPerformance(true, 90000)
        .build();

      expect(config.retry?.retries).toBe(5);
      expect(config.idempotent).toBe(true);
      expect(config.maxInFlightRequests).toBe(5);
    });

    test('should accept enhanced config in constructor', () => {
      const enhancedConfig: EnhancedProducerConfig = {
        compression: { type: CompressionTypes.Snappy },
        batching: { maxBatchSize: 2048, lingerMs: 10 },
        retry: { retries: 3, initialRetryTime: 100 }
      };

      const builder = new ProducerConfigBuilder(enhancedConfig);
      const config = builder.build();

      expect(config.retry?.retries).toBe(3);
    });
  });

  describe('ProducerPresets', () => {
    test('should create high throughput preset', () => {
      const builder = ProducerPresets.highThroughput();
      const config = builder.build();

      expect(config.retry?.retries).toBe(10);
      expect(config.idempotent).toBe(true);
      expect(config.maxInFlightRequests).toBe(5);
    });

    test('should create low latency preset', () => {
      const builder = ProducerPresets.lowLatency();
      const config = builder.build();

      expect(config.retry?.retries).toBe(3);
      expect(config.maxInFlightRequests).toBe(1);
    });

    test('should create reliable preset', () => {
      const builder = ProducerPresets.reliable();
      const config = builder.build();

      expect(config.retry?.retries).toBe(15);
      expect(config.idempotent).toBe(true);
    });

    test('should create balanced preset', () => {
      const builder = ProducerPresets.balanced();
      const config = builder.build();

      expect(config.retry?.retries).toBe(5);
      expect(config.idempotent).toBe(true);
      expect(config.maxInFlightRequests).toBe(5);
    });

    test('all presets should return valid ProducerConfigBuilder instances', () => {
      const presets = ['highThroughput', 'lowLatency', 'reliable', 'balanced'] as const;

      presets.forEach(preset => {
        const builder = ProducerPresets[preset]();
        expect(builder).toBeInstanceOf(ProducerConfigBuilder);

        const config = builder.build();
        expect(config).toBeDefined();
        expect(typeof config).toBe('object');
      });
    });
  });

  describe('Configuration validation', () => {
    test('should handle invalid compression type gracefully', () => {
      const builder = new ProducerConfigBuilder();
      expect(() => {
        builder.withCompression('invalid' as any);
      }).not.toThrow();
    });

    test('should handle negative values in batching configuration', () => {
      const builder = new ProducerConfigBuilder();
      builder.withBatching(-1, -5, -10);
      const config = builder.build();
      expect(config).toBeDefined();
    });

    test('should handle zero retries', () => {
      const builder = new ProducerConfigBuilder();
      builder.withRetries(0, 0, 0);
      const config = builder.build();

      expect(config.retry?.retries).toBe(5);
      expect(config.retry?.initialRetryTime).toBe(300);
      expect(config.retry?.maxRetryTime).toBe(30000);
    });
  });
});
