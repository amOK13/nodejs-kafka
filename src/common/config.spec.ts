import { config } from './config';

describe('Config', () => {
  const originalEnv = process.env;

  beforeEach(() => {
    jest.resetModules();
    process.env = { ...originalEnv };
  });

  afterEach(() => {
    process.env = originalEnv;
  });

  test('should have default values when no environment variables are set', () => {
    delete process.env.KAFKA_BROKERS;
    delete process.env.KAFKA_TOPIC;
    delete process.env.KAFKA_CLIENT_ID;
    delete process.env.KAFKA_CONSUMER_GROUP_ID;

    jest.isolateModules(() => {
      const { config: freshConfig } = require('./config');

      expect(freshConfig.kafkaBrokers).toEqual(['localhost:9092']);
      expect(freshConfig.kafkaTopic).toBe('test-topic');
      expect(freshConfig.kafkaClientId).toBe('nodejs-kafka-client');
      expect(freshConfig.kafkaConsumerGroupId).toBe('example-consumer-group');
    });
  });

  test('should use environment variables when provided', () => {
    process.env.KAFKA_BROKERS = 'broker1:9092,broker2:9092,broker3:9092';
    process.env.KAFKA_TOPIC = 'production-topic';
    process.env.KAFKA_CLIENT_ID = 'production-client';
    process.env.KAFKA_CONSUMER_GROUP_ID = 'production-group';

    jest.isolateModules(() => {
      const { config: freshConfig } = require('./config');

      expect(freshConfig.kafkaBrokers).toEqual(['broker1:9092', 'broker2:9092', 'broker3:9092']);
      expect(freshConfig.kafkaTopic).toBe('production-topic');
      expect(freshConfig.kafkaClientId).toBe('production-client');
      expect(freshConfig.kafkaConsumerGroupId).toBe('production-group');
    });
  });

  test('should handle single broker in KAFKA_BROKERS', () => {
    process.env.KAFKA_BROKERS = 'single-broker:9092';

    jest.isolateModules(() => {
      const { config: freshConfig } = require('./config');
      expect(freshConfig.kafkaBrokers).toEqual(['single-broker:9092']);
    });
  });

  test('should handle empty KAFKA_BROKERS environment variable', () => {
    process.env.KAFKA_BROKERS = '';

    jest.isolateModules(() => {
      const { config: freshConfig } = require('./config');
      expect(freshConfig.kafkaBrokers).toEqual(['localhost:9092']);
    });
  });

  test('should trim whitespace from broker addresses', () => {
    process.env.KAFKA_BROKERS = ' broker1:9092 , broker2:9092 , broker3:9092 ';

    jest.isolateModules(() => {
      const { config: freshConfig } = require('./config');
      expect(freshConfig.kafkaBrokers).toEqual(['broker1:9092', 'broker2:9092', 'broker3:9092']);
    });
  });

  test('should export AppConfig interface properties', () => {
    expect(typeof config.kafkaBrokers).toBe('object');
    expect(Array.isArray(config.kafkaBrokers)).toBe(true);
    expect(typeof config.kafkaTopic).toBe('string');
    expect(typeof config.kafkaClientId).toBe('string');
    expect(typeof config.kafkaConsumerGroupId).toBe('string');
  });

  test('should handle various broker port formats', () => {
    process.env.KAFKA_BROKERS = 'localhost:9092,192.168.1.100:9093,kafka.example.com:9094';

    jest.isolateModules(() => {
      const { config: freshConfig } = require('./config');
      expect(freshConfig.kafkaBrokers).toEqual([
        'localhost:9092',
        '192.168.1.100:9093',
        'kafka.example.com:9094'
      ]);
    });
  });

  test('should validate config structure', () => {
    const requiredProperties = [
      'kafkaBrokers',
      'kafkaTopic',
      'kafkaClientId',
      'kafkaConsumerGroupId'
    ];

    requiredProperties.forEach(prop => {
      expect(config).toHaveProperty(prop);
      expect(config[prop as keyof typeof config]).toBeDefined();
    });
  });
});
