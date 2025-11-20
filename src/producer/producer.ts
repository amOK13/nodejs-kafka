import { Producer, CompressionTypes } from 'kafkajs';
import { createKafka } from '../common/kafkaClient';
import { config } from '../common/config';
import { logger } from '../common/logger';
import { MessageValidator, MessageSchema } from '../common/messageValidator';
import {
  MessageSerializer,
  MessageSerializerFactory,
  SerializationFormat
} from '../common/messageSerializer';
import {
  EnhancedProducerConfig,
  ProducerConfigBuilder,
  ProducerPresets
} from '../common/producerConfig';
import { MessageRouter, RoutingRule, MessageMetadata } from '../common/messageRouter';
import { Partitioner, PartitionerFactory } from '../common/partitioners';
import {
  TransactionManager,
  TransactionMessage,
  TransactionOptions
} from '../common/transactionManager';
import { MetadataManager, EnhancedMessageMetadata } from '../common/metadataManager';

export interface ProducerOptions {
  serializationFormat?: SerializationFormat;
  enableValidation?: boolean;
  schema?: MessageSchema;
  config?: EnhancedProducerConfig;
  preset?: 'highThroughput' | 'lowLatency' | 'reliable' | 'balanced';
  enableRouting?: boolean;
  defaultRoutingTopic?: string;
  partitioner?: Partitioner;
  enableTransactions?: boolean;
  transactionOptions?: TransactionOptions;
  defaultMetadata?: Partial<EnhancedMessageMetadata>;
}

export class MessageProducer {
  private producer!: Producer;
  private validator: MessageValidator;
  private serializer: MessageSerializer;
  private enableValidation: boolean;
  private compressionType?: CompressionTypes;
  private batchingConfig: { maxBatchSize: number; lingerMs: number };
  private timeoutMs: number;
  private messageRouter?: MessageRouter;
  private partitioner?: Partitioner;
  private transactionManager?: TransactionManager;
  private metadataManager: MetadataManager;

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
    if (options.enableRouting) {
      this.messageRouter = new MessageRouter(options.defaultRoutingTopic || config.kafkaTopic);
    }
    this.partitioner = options.partitioner;
    this.metadataManager = new MetadataManager(options.defaultMetadata);
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

    if (this.options.enableTransactions) {
      this.transactionManager = new TransactionManager(
        this.producer,
        this.options.transactionOptions
      );
    }

    logger.info('Enhanced producer initialized', {
      preset: this.options.preset,
      compression: this.compressionType,
      batching: this.batchingConfig,
      timeout: this.timeoutMs,
      validation: this.enableValidation,
      serialization: this.options.serializationFormat || 'json',
      routing: !!this.messageRouter,
      partitioner: this.partitioner?.getName(),
      transactions: !!this.transactionManager
    });
  }

  async sendMessage(
    message: any,
    key?: string,
    headers?: Record<string, string>,
    metadata?: Partial<EnhancedMessageMetadata>
  ): Promise<void> {
    try {
      const enhancedMetadata = this.metadataManager.createMetadata(metadata);
      if (this.enableValidation) {
        const validationResult = this.validator.validate(message);
        if (!validationResult.isValid) {
          const error = new Error(
            `Message validation failed: ${validationResult.error || 'Unknown validation error'}`
          );
          logger.error('Message validation failed', {
            topic: config.kafkaTopic,
            message,
            validationError: validationResult.error
          });
          throw error;
        }
      }
      let targetTopic = config.kafkaTopic;
      if (this.messageRouter) {
        const routingResult = this.messageRouter.route(message, enhancedMetadata);
        targetTopic = routingResult.topic;
        if (routingResult.matchedRule) {
          logger.debug('Message routed', {
            originalTopic: config.kafkaTopic,
            targetTopic,
            rule: routingResult.matchedRule.name
          });
        }
      }

      const serializedMessage = this.serializer.serialize(message);
      const combinedHeaders = { ...headers };
      const metadataHeaders = this.metadataManager.metadataToHeaders(enhancedMetadata);
      const kafkaHeaders: Record<string, Buffer> = {};
      Object.entries(metadataHeaders).forEach(([key, value]) => {
        kafkaHeaders[key] = value;
      });

      if (combinedHeaders) {
        Object.keys(combinedHeaders).forEach(headerKey => {
          kafkaHeaders[headerKey] = Buffer.from(combinedHeaders[headerKey]);
        });
      }

      let partition: number | undefined;
      if (this.partitioner) {
        const partitionCount = (await this.getTopicPartitionCount(targetTopic)) || 3;
        partition = this.partitioner.partition({
          topic: targetTopic,
          partitionCount,
          message,
          key,
          metadata: enhancedMetadata
        });
      }

      await this.producer.send({
        topic: targetTopic,
        messages: [
          {
            partition,
            key: key ? Buffer.from(key) : undefined,
            value: Buffer.from(serializedMessage),
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

  async sendBatch(
    messages: Array<{ message: any; key?: string; headers?: Record<string, string> }>
  ): Promise<void> {
    try {
      const kafkaMessages = [];
      const partitionCount = this.partitioner
        ? (await this.getTopicPartitionCount(config.kafkaTopic)) || 3
        : undefined;

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

        let partition: number | undefined;
        if (this.partitioner && partitionCount) {
          const enhancedMetadata = this.metadataManager.createMetadata(
            this.options.defaultMetadata || {}
          );
          partition = this.partitioner.partition({
            topic: config.kafkaTopic,
            partitionCount,
            message,
            key,
            metadata: enhancedMetadata
          });
        }

        kafkaMessages.push({
          partition,
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
      logger.error('Failed to send batch messages', {
        topic: config.kafkaTopic,
        messageCount: messages.length,
        error
      });
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

  addRoutingRule(rule: RoutingRule): void {
    if (!this.messageRouter) {
      throw new Error('Routing is not enabled. Set enableRouting: true in options.');
    }
    this.messageRouter.addRule(rule);
    logger.info('Routing rule added', { ruleId: rule.id, name: rule.name });
  }

  removeRoutingRule(ruleId: string): boolean {
    if (!this.messageRouter) {
      return false;
    }
    const removed = this.messageRouter.removeRule(ruleId);
    if (removed) {
      logger.info('Routing rule removed', { ruleId });
    }
    return removed;
  }

  getRoutingRules(): RoutingRule[] {
    return this.messageRouter?.getRules() || [];
  }

  async sendInTransaction<T>(
    operation: (sender: (messages: TransactionMessage[]) => Promise<void>) => Promise<T>
  ): Promise<T> {
    if (!this.transactionManager) {
      throw new Error('Transactions are not enabled. Set enableTransactions: true in options.');
    }
    const result = await this.transactionManager.executeTransaction(operation);
    return result.result;
  }

  getActiveTransactionId(): string | null {
    return this.transactionManager?.getActiveTransactionId() || null;
  }

  setDefaultMetadata(metadata: Partial<EnhancedMessageMetadata>): void {
    this.metadataManager.setDefaultMetadata(metadata);
    logger.info('Default metadata updated', { metadata });
  }

  enrichMetadata(
    baseMetadata: Partial<EnhancedMessageMetadata>,
    enrichments: Partial<EnhancedMessageMetadata>
  ): EnhancedMessageMetadata {
    return this.metadataManager.enrichMetadata(baseMetadata, enrichments);
  }

  setPartitioner(partitioner: Partitioner): void {
    this.partitioner = partitioner;
    logger.info('Partitioner updated', { partitioner: partitioner.getName() });
  }

  getPartitioner(): Partitioner | undefined {
    return this.partitioner;
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

  private async getTopicPartitionCount(topic: string): Promise<number | undefined> {
    try {
      const kafka = createKafka();
      const admin = kafka.admin();
      await admin.connect();

      const metadata = await admin.fetchTopicMetadata({ topics: [topic] });
      const topicMetadata = metadata.topics.find(t => t.name === topic);

      await admin.disconnect();

      return topicMetadata?.partitions.length;
    } catch (error) {
      logger.error('Failed to fetch topic partition count', { topic, error });
      return undefined;
    }
  }

  async disconnect(): Promise<void> {
    if (this.producer) {
      await this.producer.disconnect();
      logger.info('Producer disconnected');
    }
  }
}
