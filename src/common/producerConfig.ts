import { CompressionTypes, ProducerConfig } from 'kafkajs';

export interface EnhancedProducerConfig {
  compression?: {
    type: CompressionTypes;
    level?: number;
  };

  batching?: {
    maxBatchSize?: number;
    lingerMs?: number;
    maxInFlightRequests?: number;
  };

  retry?: {
    retries?: number;
    initialRetryTime?: number;
    maxRetryTime?: number;
    factor?: number;
    multiplier?: number;
    retryDelayOnClusterActionFailure?: number;
    retryDelayOnFailover?: number;
  };

  timeout?: {
    requestTimeoutMs?: number;
    acks?: number;
  };

  performance?: {
    idempotent?: boolean;
    transactionTimeout?: number;
    maxInFlightRequests?: number;
  };
}

export class ProducerConfigBuilder {
  private config: ProducerConfig = {};

  constructor(private enhancedConfig: EnhancedProducerConfig = {}) {
    this.buildBaseConfig();
  }

  private buildBaseConfig(): void {
    this.config = {
      ...this.buildCompressionConfig(),
      ...this.buildBatchingConfig(),
      ...this.buildRetryConfig(),
      ...this.buildTimeoutConfig(),
      ...this.buildPerformanceConfig()
    };
  }

  private buildCompressionConfig(): Partial<ProducerConfig> {
    return {};
  }

  private buildBatchingConfig(): Partial<ProducerConfig> {
    const batching = this.enhancedConfig.batching;
    if (!batching) {
      return {};
    }

    return {
      maxInFlightRequests: batching.maxInFlightRequests || 5
    };
  }

  private buildRetryConfig(): Partial<ProducerConfig> {
    const retry = this.enhancedConfig.retry;
    if (!retry) {
      return {};
    }

    return {
      retry: {
        retries: retry.retries || 5,
        initialRetryTime: retry.initialRetryTime || 300,
        maxRetryTime: retry.maxRetryTime || 30000,
        factor: retry.factor || 2,
        multiplier: retry.multiplier || 2
      }
    };
  }

  private buildTimeoutConfig(): Partial<ProducerConfig> {
    return {};
  }

  private buildPerformanceConfig(): Partial<ProducerConfig> {
    const performance = this.enhancedConfig.performance;
    if (!performance) {
      return {};
    }

    return {
      idempotent: performance.idempotent || false,
      transactionTimeout: performance.transactionTimeout || 60000,
      maxInFlightRequests: performance.maxInFlightRequests || 5
    };
  }

  build(): ProducerConfig {
    return { ...this.config };
  }

  withCompression(type: CompressionTypes, level?: number): ProducerConfigBuilder {
    this.enhancedConfig.compression = { type, level };
    this.buildBaseConfig();
    return this;
  }

  withBatching(maxBatchSize?: number, lingerMs?: number, maxInFlightRequests?: number): ProducerConfigBuilder {
    this.enhancedConfig.batching = { maxBatchSize, lingerMs, maxInFlightRequests };
    this.buildBaseConfig();
    return this;
  }

  withRetries(retries?: number, initialRetryTime?: number, maxRetryTime?: number): ProducerConfigBuilder {
    this.enhancedConfig.retry = {
      ...this.enhancedConfig.retry,
      retries,
      initialRetryTime,
      maxRetryTime
    };
    this.buildBaseConfig();
    return this;
  }

  withTimeout(requestTimeoutMs?: number, acks?: number): ProducerConfigBuilder {
    this.enhancedConfig.timeout = { requestTimeoutMs, acks };
    this.buildBaseConfig();
    return this;
  }

  withPerformance(idempotent?: boolean, transactionTimeout?: number): ProducerConfigBuilder {
    this.enhancedConfig.performance = {
      ...this.enhancedConfig.performance,
      idempotent,
      transactionTimeout
    };
    this.buildBaseConfig();
    return this;
  }
}

export const ProducerPresets = {
  highThroughput: (): ProducerConfigBuilder => {
    return new ProducerConfigBuilder()
      .withCompression(CompressionTypes.GZIP)
      .withBatching(16384, 100, 10)
      .withPerformance(true, 60000)
      .withRetries(10, 100, 32000);
  },

  lowLatency: (): ProducerConfigBuilder => {
    return new ProducerConfigBuilder()
      .withCompression(CompressionTypes.LZ4)
      .withBatching(1024, 5, 1)
      .withTimeout(5000, 1)
      .withRetries(3, 50, 5000);
  },

  reliable: (): ProducerConfigBuilder => {
    return new ProducerConfigBuilder()
      .withCompression(CompressionTypes.Snappy)
      .withPerformance(true, 120000)
      .withRetries(15, 500, 60000)
      .withTimeout(60000, -1);
  },

  balanced: (): ProducerConfigBuilder => {
    return new ProducerConfigBuilder()
      .withCompression(CompressionTypes.Snappy)
      .withBatching(8192, 50, 5)
      .withPerformance(true, 60000)
      .withRetries(5, 300, 30000);
  }
};
