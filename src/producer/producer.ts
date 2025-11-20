import { Producer, CompressionTypes } from 'kafkajs';
import { createKafka } from '../common/kafkaClient';
import { config } from '../common/config';
import { logger } from '../common/logger';
import { MessageValidator, MessageSchema } from '../common/messageValidator';
import { MessageSerializer, MessageSerializerFactory, SerializationFormat } from '../common/messageSerializer';
import { EnhancedProducerConfig, ProducerConfigBuilder, ProducerPresets } from '../common/producerConfig';

export interface ProducerOptions {
  serializationFormat?: SerializationFormat;
  enableValidation?: boolean;
  schema?: MessageSchema;
  config?: EnhancedProducerConfig;
  preset?: 'highThroughput' | 'lowLatency' | 'reliable' | 'balanced';
}

export class MessageProducer {
  private producer!: Producer;
  private validator: MessageValidator;
  private serializer: MessageSerializer;
  private enableValidation: boolean;
  private compressionType?: CompressionTypes;
  private batchingConfig: { maxBatchSize: number; lingerMs: number };
  private timeoutMs: number;

  constructor(private options: ProducerOptions = {}) {
    this.enableValidation = options.enableValidation ?? true;
    this.validator = new MessageValidator(options.schema);
    this.serializer = MessageSerializerFactory.create(options.serializationFormat ?? 'json');
    this.compressionType = options.config?.compression?.type;
    this.batchingConfig = {
      maxBatchSize: options.config?.batching?.maxBatchSize ?? 16384,
      lingerMs: options.config?.batching?.lingerMs ?? 100
    };
    this.timeoutMs = options.config?.timeout?.requestTimeoutMs ?? 30000;
  }

  async initialize(): Promise<void> {
    const kafka = createKafka();

    let configBuilder: ProducerConfigBuilder;
    if (this.options.preset) {
      configBuilder = ProducerPresets[this.options.preset]();
    } else {
      configBuilder = new ProducerConfigBuilder(this.options.config);
    }

    const producerConfig = configBuilder.build();
    this.producer = kafka.producer(producerConfig);
    await this.producer.connect();

    logger.info('Enhanced producer initialized', {
      preset: this.options.preset,
      compression: this.compressionType,
      batching: this.batchingConfig,
      timeout: this.timeoutMs,
      validation: this.enableValidation,
      serialization: this.options.serializationFormat
    });
  }

  async sendMessage(message: any, key?: string, headers?: Record<string, string>): Promise<void> {
    try {
      if (this.enableValidation) {
        const validationResult = this.validator.validate(message);
        if (!validationResult.isValid) {
          const error = new Error(`Message validation failed: ${validationResult.error || 'Unknown validation error'}`);
          logger.error('Message validation failed', {
            topic: config.kafkaTopic,
            message,
            validationError: validationResult.error
          });
          throw error;
        }
      }

      const serializedMessage = this.serializer.serialize(message);
      const kafkaHeaders: Record<string, Buffer> = {};

      if (headers) {
        Object.keys(headers).forEach(headerKey => {
          kafkaHeaders[headerKey] = Buffer.from(headers[headerKey]);
        });
      }

      await this.producer.send({
        topic: config.kafkaTopic,
        compression: this.compressionType,
        timeout: this.timeoutMs,
        messages: [
          {
            key: key ? Buffer.from(key) : undefined,
            value: serializedMessage,
            headers: Object.keys(kafkaHeaders).length > 0 ? kafkaHeaders : undefined,
            timestamp: Date.now().toString()
          }
        ]
      });

      logger.info('Message sent successfully', {
        topic: config.kafkaTopic,
        messageLength: serializedMessage.length,
        hasKey: !!key,
        hasHeaders: !!headers
      });
    } catch (error) {
      logger.error('Failed to send message', { topic: config.kafkaTopic, message, error });
      throw error;
    }
  }

  async sendBatch(messages: Array<{ message: any; key?: string; headers?: Record<string, string> }>): Promise<void> {
    try {
      const kafkaMessages = [];
      for (const { message, key, headers } of messages) {
        if (this.enableValidation) {
          const validationResult = this.validator.validate(message);
          if (!validationResult.isValid) {
            const error = new Error(
              `Batch message validation failed: ${validationResult.error || 'Unknown validation error'}`
            );
            logger.error('Batch message validation failed', {
              topic: config.kafkaTopic,
              message,
              validationError: validationResult.error
            });
            throw error;
          }
        }

        const serializedMessage = this.serializer.serialize(message);
        const kafkaHeaders: Record<string, Buffer> = {};

        if (headers) {
          Object.keys(headers).forEach(headerKey => {
            kafkaHeaders[headerKey] = Buffer.from(headers[headerKey]);
          });
        }

        kafkaMessages.push({
          key: key ? Buffer.from(key) : undefined,
          value: serializedMessage,
          headers: Object.keys(kafkaHeaders).length > 0 ? kafkaHeaders : undefined,
          timestamp: Date.now().toString()
        });
      }

      const chunks = this.chunkMessages(kafkaMessages, this.batchingConfig.maxBatchSize);
      for (const chunk of chunks) {
        await this.producer.send({
          topic: config.kafkaTopic,
          compression: this.compressionType,
          timeout: this.timeoutMs,
          messages: chunk
        });

        if (chunks.length > 1 && this.batchingConfig.lingerMs > 0) {
          await new Promise(resolve => setTimeout(resolve, this.batchingConfig.lingerMs));
        }
      }

      logger.info('Batch messages sent successfully', {
        topic: config.kafkaTopic,
        messageCount: messages.length
      });
    } catch (error) {
      logger.error('Failed to send batch messages', { topic: config.kafkaTopic, messageCount: messages.length, error });
      throw error;
    }
  }

  updateSchema(schema: MessageSchema): void {
    this.validator = new MessageValidator(schema);
    logger.info('Producer schema updated');
  }

  setValidationEnabled(enabled: boolean): void {
    this.enableValidation = enabled;
    logger.info('Producer validation status changed', { enabled });
  }

  updateConfiguration(config: Partial<EnhancedProducerConfig>): void {
    if (config.compression) {
      this.compressionType = config.compression.type;
    }
    if (config.batching) {
      this.batchingConfig = {
        maxBatchSize: config.batching.maxBatchSize ?? this.batchingConfig.maxBatchSize,
        lingerMs: config.batching.lingerMs ?? this.batchingConfig.lingerMs
      };
    }
    if (config.timeout) {
      this.timeoutMs = config.timeout.requestTimeoutMs ?? this.timeoutMs;
    }
    logger.info('Producer configuration updated', { config });
  }

  getConfiguration(): { compression?: CompressionTypes; batching: any; timeout: number } {
    return {
      compression: this.compressionType,
      batching: this.batchingConfig,
      timeout: this.timeoutMs
    };
  }

  private chunkMessages<T>(messages: T[], chunkSize: number): T[][] {
    const chunks: T[][] = [];
    for (let i = 0; i < messages.length; i += chunkSize) {
      chunks.push(messages.slice(i, i + chunkSize));
    }
    return chunks;
  }

  async disconnect(): Promise<void> {
    if (this.producer) {
      await this.producer.disconnect();
      logger.info('Producer disconnected');
    }
  }
}
