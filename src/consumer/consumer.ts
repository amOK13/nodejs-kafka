import { Consumer } from 'kafkajs';
import { createConsumer } from '../common/kafkaClient';
import { config } from '../common/config';
import { logger } from '../common/logger';

interface OrderCreatedEvent {
  id: string;
  orderId: string;
  customerId: string;
  amount: number;
  createdAt: string;
  status: string;
}

interface ConsumerMetrics {
  totalMessages: number;
  processedMessages: number;
  skippedMessages: number;
  errorMessages: number;
  dlqMessages: number;
}

type ProcessingResult = 'SUCCESS' | 'SKIP' | 'DLQ' | 'ERROR';

interface MessageContext {
  topic: string;
  partition: number;
  offset: string;
  rawMessage: string;
}

export class MessageConsumer {
  private consumer!: Consumer;
  private isShuttingDown = false;
  private consumerRunPromise: Promise<void> | null = null;

  private readonly processedEventIds = new Set<string>();

  private readonly metrics: ConsumerMetrics = {
    totalMessages: 0,
    processedMessages: 0,
    skippedMessages: 0,
    errorMessages: 0,
    dlqMessages: 0
  };

  async initialize(): Promise<void> {
    this.consumer = await createConsumer();
    await this.consumer.subscribe({ topic: config.kafkaTopic, fromBeginning: true });
    this.setupGracefulShutdown();
    logger.info(`Subscribed to topic: ${config.kafkaTopic}`);
  }

  async startConsuming(): Promise<void> {
    this.consumerRunPromise = this.consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        await this.handleMessage({
          topic,
          partition,
          offset: message.offset?.toString() || '0',
          rawMessage: message.value?.toString() || ''
        });
      }
    });

    try {
      await this.consumerRunPromise;
    } catch (error) {
      if (!this.isShuttingDown) {
        logger.error('Consumer run failed', {
          error: error instanceof Error ? error.message : error
        });
        throw error;
      }
    }
  }

  private async handleMessage(context: MessageContext): Promise<void> {
    if (this.isShuttingDown) {
      logger.info('Message ignored because shutting down', {
        topic: context.topic,
        partition: context.partition,
        offset: context.offset
      });
      return;
    }

    const startTime = Date.now();
    this.metrics.totalMessages++;

    try {
      if (!context.rawMessage) {
        this.logDlqCandidate('Empty message received', context);
        this.metrics.dlqMessages++;
        return;
      }

      const parseResult = this.parseMessage(context.rawMessage);
      if (parseResult.result === 'DLQ') {
        this.logDlqCandidate('JSON parsing failed', context);
        this.metrics.dlqMessages++;
        return;
      }

      const event = parseResult.event!;
      const validationResult = this.validateEventStructure(event);
      if (validationResult === 'DLQ') {
        this.logDlqCandidate('Event structure validation failed', context);
        this.metrics.dlqMessages++;
        return;
      }

      if (this.processedEventIds.has(event.id)) {
        logger.info('Skipping already processed event', {
          eventId: event.id,
          orderId: event.orderId,
          ...context
        });
        this.metrics.skippedMessages++;
        return;
      }

      const eventCreatedAt = new Date(event.createdAt).getTime();
      const latencyMs = startTime - eventCreatedAt;

      const processingResult = await this.processOrderEvent(event);

      switch (processingResult) {
        case 'SUCCESS':
          this.processedEventIds.add(event.id);
          this.metrics.processedMessages++;
          logger.info('Successfully processed OrderCreatedEvent', {
            eventId: event.id,
            orderId: event.orderId,
            customerId: event.customerId,
            amount: event.amount,
            latencyMs,
            ...context
          });
          break;

        case 'ERROR':
          logger.error('Business/technical error during processing (retryable)', {
            eventId: event.id,
            orderId: event.orderId,
            ...context
          });
          this.metrics.errorMessages++;
          throw new Error('Retryable processing error');

        case 'DLQ':
          this.logDlqCandidate('Business logic rejected event', context);
          this.metrics.dlqMessages++;
          break;
      }
    } catch (error) {
      logger.error('Unexpected error in message handling', {
        error: error instanceof Error ? error.message : error,
        stack: error instanceof Error ? error.stack : undefined,
        ...context
      });
      this.metrics.errorMessages++;
      throw error;
    } finally {
      this.logConsumerStats();
    }
  }

  private parseMessage(rawMessage: string): {
    result: ProcessingResult;
    event?: OrderCreatedEvent;
  } {
    try {
      const parsed = JSON.parse(rawMessage);
      return { result: 'SUCCESS', event: parsed };
    } catch {
      return { result: 'DLQ' };
    }
  }

  private validateEventStructure(event: any): ProcessingResult {
    const requiredFields = ['id', 'orderId', 'customerId', 'amount', 'createdAt', 'status'];

    for (const field of requiredFields) {
      if (!event || typeof event[field] === 'undefined') {
        return 'DLQ';
      }
    }

    if (typeof event.amount !== 'number' || event.amount < 0) {
      return 'DLQ';
    }

    if (isNaN(new Date(event.createdAt).getTime())) {
      return 'DLQ';
    }

    return 'SUCCESS';
  }

  private async processOrderEvent(event: OrderCreatedEvent): Promise<ProcessingResult> {
    try {
      logger.info('Processing order creation', {
        eventId: event.id,
        orderId: event.orderId,
        customerId: event.customerId,
        amount: event.amount,
        status: event.status
      });

      await new Promise(resolve => setTimeout(resolve, 10));

      return 'SUCCESS';
    } catch (error) {
      logger.error('Error in business processing', {
        eventId: event.id,
        error: error instanceof Error ? error.message : error
      });
      return 'ERROR';
    }
  }

  private logDlqCandidate(reason: string, context: MessageContext): void {
    logger.error('Dead-letter candidate', {
      reason,
      meta: {
        topic: context.topic,
        partition: context.partition,
        offset: context.offset,
        rawMessage: context.rawMessage
      }
    });
  }

  private logConsumerStats(): void {
    logger.info('consumer.stats', {
      meta: {
        totalMessages: this.metrics.totalMessages,
        processedMessages: this.metrics.processedMessages,
        skippedMessages: this.metrics.skippedMessages,
        errorMessages: this.metrics.errorMessages,
        dlqMessages: this.metrics.dlqMessages,
        processedEventIdsCount: this.processedEventIds.size
      }
    });
  }

  getMetrics(): ConsumerMetrics & { processedEventIdsCount: number } {
    return {
      ...this.metrics,
      processedEventIdsCount: this.processedEventIds.size
    };
  }

  clearProcessedEventIds(): void {
    this.processedEventIds.clear();
    logger.info('Cleared processed event IDs cache');
  }

  private setupGracefulShutdown(): void {
    const shutdown = async (signal: string) => {
      if (this.isShuttingDown) {
        logger.info('Shutdown already in progress, forcing exit...', { signal });
        process.exit(1);
      }

      logger.info('Shutdown signal received, stopping consumer gracefully...', { signal });
      await this.gracefulShutdown();
    };

    process.on('SIGINT', () => shutdown('SIGINT'));
    process.on('SIGTERM', () => shutdown('SIGTERM'));
  }

  private async gracefulShutdown(): Promise<void> {
    this.isShuttingDown = true;

    try {
      logger.info('Stopping consumer and waiting for current processing to complete...');

      if (this.consumer) {
        await this.consumer.disconnect();
        logger.info('Consumer disconnected successfully');
      }

      logger.info('Graceful shutdown completed', {
        finalMetrics: this.getMetrics()
      });

      process.exit(0);
    } catch (error) {
      logger.error('Error during graceful shutdown', {
        error: error instanceof Error ? error.message : error
      });
      process.exit(1);
    }
  }

  async stop(): Promise<void> {
    if (!this.isShuttingDown) {
      await this.gracefulShutdown();
    }
  }

  isShuttingDownStatus(): boolean {
    return this.isShuttingDown;
  }
}
