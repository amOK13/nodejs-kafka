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
}

export class MessageConsumer {
  private consumer!: Consumer;

  private readonly processedEventIds = new Set<string>();

  private readonly metrics: ConsumerMetrics = {
    totalMessages: 0,
    processedMessages: 0,
    skippedMessages: 0,
    errorMessages: 0
  };

  async initialize(): Promise<void> {
    this.consumer = await createConsumer();
    await this.consumer.subscribe({ topic: config.kafkaTopic, fromBeginning: true });
    logger.info(`Subscribed to topic: ${config.kafkaTopic}`);
  }

  async startConsuming(): Promise<void> {
    await this.consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const startTime = Date.now();
        this.metrics.totalMessages++;
        
        try {
          const messageValue = message.value?.toString();
          if (!messageValue) {
            logger.error('Received empty message', {
              topic,
              partition,
              offset: message.offset
            });
            this.metrics.errorMessages++;
            this.logConsumerStats();
            return;
          }

          let event: OrderCreatedEvent;
          try {
            event = JSON.parse(messageValue);
          } catch (parseError) {
            logger.error('Failed to parse message JSON', {
              topic,
              partition,
              offset: message.offset,
              error: parseError instanceof Error ? parseError.message : parseError,
              rawMessage: messageValue
            });
            this.metrics.errorMessages++;
            this.logConsumerStats();
            return;
          }

          if (this.processedEventIds.has(event.id)) {
            logger.info('Skipping already processed event', {
              eventId: event.id,
              orderId: event.orderId,
              topic,
              partition,
              offset: message.offset
            });
            this.metrics.skippedMessages++;
            this.logConsumerStats();
            return;
          }

          const eventCreatedAt = new Date(event.createdAt).getTime();
          const latencyMs = startTime - eventCreatedAt;

          await this.processOrderCreatedEvent(event, {
            topic,
            partition,
            offset: message.offset,
            latencyMs
          });

          this.processedEventIds.add(event.id);
          this.metrics.processedMessages++;

          logger.info('Successfully processed OrderCreatedEvent', {
            eventId: event.id,
            orderId: event.orderId,
            customerId: event.customerId,
            amount: event.amount,
            latencyMs,
            topic,
            partition,
            offset: message.offset
          });
        } catch (error) {
          logger.error('Error processing message', {
            topic,
            partition,
            offset: message.offset,
            error: error instanceof Error ? error.message : error,
            stack: error instanceof Error ? error.stack : undefined
          });
          this.metrics.errorMessages++;
        } finally {
          this.logConsumerStats();
        }
      }
    });
  }

  private async processOrderCreatedEvent(
    event: OrderCreatedEvent, 
    metadata: { topic: string; partition: number; offset: string; latencyMs: number }
  ): Promise<void> {
    logger.info('Processing order creation', {
      eventId: event.id,
      orderId: event.orderId,
      customerId: event.customerId,
      amount: event.amount,
      status: event.status,
      businessLatencyMs: metadata.latencyMs
    });

    await new Promise(resolve => setTimeout(resolve, 10));
  }

  private logConsumerStats(): void {
    logger.info('consumer.stats', {
      meta: {
        totalMessages: this.metrics.totalMessages,
        processedMessages: this.metrics.processedMessages,
        skippedMessages: this.metrics.skippedMessages,
        errorMessages: this.metrics.errorMessages,
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

  async stop(): Promise<void> {
    if (this.consumer) {
      await this.consumer.disconnect();
      logger.info('Consumer disconnected', {
        finalMetrics: this.getMetrics()
      });
    }
  }
}
