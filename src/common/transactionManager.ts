import { Producer } from 'kafkajs';
import { logger } from './logger';

export interface TransactionMessage {
  topic: string;
  partition?: number;
  key?: string;
  value: Buffer;
  headers?: Record<string, Buffer>;
  timestamp?: string;
}

export interface TransactionResult {
  transactionId: string;
  success: boolean;
  messageCount: number;
  error?: Error;
  duration: number;
}

export interface TransactionOptions {
  transactionTimeout?: number;
  maxRetries?: number;
  retryDelay?: number;
}

export class TransactionManager {
  private producer: Producer;
  private activeTransaction: string | null = null;
  private transactionTimeout: number;
  private maxRetries: number;
  private retryDelay: number;

  constructor(producer: Producer, options: TransactionOptions = {}) {
    this.producer = producer;
    this.transactionTimeout = options.transactionTimeout || 60000;
    this.maxRetries = options.maxRetries || 3;
    this.retryDelay = options.retryDelay || 1000;
  }

  async beginTransaction(transactionId?: string): Promise<string> {
    if (this.activeTransaction) {
      throw new Error(`Transaction ${this.activeTransaction} is already active`);
    }

    const txnId = transactionId || this.generateTransactionId();

    try {
      await this.producer.transaction();
      this.activeTransaction = txnId;

      logger.info('Transaction started', { transactionId: txnId });
      return txnId;
    } catch (error) {
      logger.error('Failed to start transaction', { transactionId: txnId, error });
      throw new Error(
        `Failed to start transaction: ${error instanceof Error ? error.message : 'Unknown error'}`
      );
    }
  }

  async sendInTransaction(messages: TransactionMessage[]): Promise<void> {
    if (!this.activeTransaction) {
      throw new Error('No active transaction');
    }

    if (messages.length === 0) {
      return;
    }

    try {
      const groupedMessages = this.groupMessagesByTopic(messages);

      for (const [topic, topicMessages] of groupedMessages.entries()) {
        await this.producer.send({
          topic,
          messages: topicMessages.map(msg => ({
            partition: msg.partition,
            key: msg.key ? Buffer.from(msg.key) : undefined,
            value: msg.value,
            headers: msg.headers,
            timestamp: msg.timestamp
          }))
        });
      }

      logger.debug('Messages sent in transaction', {
        transactionId: this.activeTransaction,
        messageCount: messages.length
      });
    } catch (error) {
      logger.error('Failed to send messages in transaction', {
        transactionId: this.activeTransaction,
        error
      });
      throw error;
    }
  }

  async commitTransaction(): Promise<TransactionResult> {
    if (!this.activeTransaction) {
      throw new Error('No active transaction to commit');
    }

    const transactionId = this.activeTransaction;
    const startTime = Date.now();

    try {
      const transaction = await this.producer.transaction();
      await transaction.commit();

      const result: TransactionResult = {
        transactionId,
        success: true,
        messageCount: 0,
        duration: Date.now() - startTime
      };

      logger.info('Transaction committed successfully', result);
      this.activeTransaction = null;
      return result;
    } catch (error) {
      const result: TransactionResult = {
        transactionId,
        success: false,
        messageCount: 0,
        error: error instanceof Error ? error : new Error('Unknown error'),
        duration: Date.now() - startTime
      };

      logger.error('Failed to commit transaction', result);
      this.activeTransaction = null;
      throw error;
    }
  }

  async rollbackTransaction(): Promise<void> {
    if (!this.activeTransaction) {
      throw new Error('No active transaction to rollback');
    }

    const transactionId = this.activeTransaction;

    try {
      const transaction = await this.producer.transaction();
      await transaction.abort();
      logger.info('Transaction rolled back', { transactionId });
      this.activeTransaction = null;
    } catch (error) {
      logger.error('Failed to rollback transaction', { transactionId, error });
      this.activeTransaction = null;
      throw error;
    }
  }

  async executeTransaction<T>(
    operation: (sender: (messages: TransactionMessage[]) => Promise<void>) => Promise<T>,
    options?: TransactionOptions
  ): Promise<{ result: T; transactionResult: TransactionResult }> {
    const transactionId = this.generateTransactionId();
    const startTime = Date.now();
    let messageCount = 0;

    try {
      await this.beginTransaction(transactionId);

      const sender = async (messages: TransactionMessage[]) => {
        await this.sendInTransaction(messages);
        messageCount += messages.length;
      };

      const result = await operation(sender);
      const transactionResult = await this.commitTransaction();

      return {
        result,
        transactionResult: {
          ...transactionResult,
          messageCount
        }
      };
    } catch (error) {
      try {
        await this.rollbackTransaction();
      } catch (rollbackError) {
        logger.error('Failed to rollback after error', { transactionId, rollbackError });
      }

      const transactionResult: TransactionResult = {
        transactionId,
        success: false,
        messageCount,
        error: error instanceof Error ? error : new Error('Unknown error'),
        duration: Date.now() - startTime
      };

      throw transactionResult;
    }
  }

  getActiveTransactionId(): string | null {
    return this.activeTransaction;
  }

  isTransactionActive(): boolean {
    return this.activeTransaction !== null;
  }

  private generateTransactionId(): string {
    return `txn_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  }

  private groupMessagesByTopic(messages: TransactionMessage[]): Map<string, TransactionMessage[]> {
    const grouped = new Map<string, TransactionMessage[]>();

    for (const message of messages) {
      const existing = grouped.get(message.topic) || [];
      existing.push(message);
      grouped.set(message.topic, existing);
    }

    return grouped;
  }
}

export const createTransactionManager = (
  producer: Producer,
  options?: TransactionOptions
): TransactionManager => {
  return new TransactionManager(producer, options);
};
