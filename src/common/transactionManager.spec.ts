import { TransactionManager, TransactionMessage, createTransactionManager } from './transactionManager';
import { Producer, Transaction } from 'kafkajs';

// Mock KafkaJS Producer
const mockTransaction = {
  commit: jest.fn(),
  abort: jest.fn(),
  send: jest.fn()
};

const mockProducer = {
  transaction: jest.fn().mockResolvedValue(mockTransaction),
  send: jest.fn().mockResolvedValue([])
} as unknown as Producer;

describe('TransactionManager', () => {
  let transactionManager: TransactionManager;

  beforeEach(() => {
    jest.clearAllMocks();
    transactionManager = new TransactionManager(mockProducer);
  });

  describe('Constructor', () => {
    test('should create transaction manager with default options', () => {
      expect(transactionManager).toBeInstanceOf(TransactionManager);
    });

    test('should create transaction manager with custom options', () => {
      const options = {
        transactionTimeout: 120000,
        maxRetries: 5,
        retryDelay: 2000
      };

      const manager = new TransactionManager(mockProducer, options);
      expect(manager).toBeInstanceOf(TransactionManager);
    });
  });

  describe('Transaction Lifecycle', () => {
    test('should begin transaction successfully', async () => {
      const transactionId = await transactionManager.beginTransaction();

      expect(mockProducer.transaction).toHaveBeenCalledTimes(1);
      expect(transactionId).toMatch(/^txn_\d+_[a-z0-9]+$/);
      expect(transactionManager.getActiveTransactionId()).toBe(transactionId);
      expect(transactionManager.isTransactionActive()).toBe(true);
    });

    test('should begin transaction with custom id', async () => {
      const customId = 'custom-transaction-id';
      const transactionId = await transactionManager.beginTransaction(customId);

      expect(transactionId).toBe(customId);
      expect(transactionManager.getActiveTransactionId()).toBe(customId);
    });

    test('should throw error when starting transaction while one is active', async () => {
      await transactionManager.beginTransaction();

      await expect(transactionManager.beginTransaction()).rejects.toThrow(
        'Transaction txn_'
      );
    });

    test('should commit transaction successfully', async () => {
      const transactionId = await transactionManager.beginTransaction();

      const result = await transactionManager.commitTransaction();

      expect(mockTransaction.commit).toHaveBeenCalledTimes(1);
      expect(result.transactionId).toBe(transactionId);
      expect(result.success).toBe(true);
      expect(result.duration).toBeGreaterThanOrEqual(0);
      expect(transactionManager.getActiveTransactionId()).toBeNull();
    });

    test('should rollback transaction successfully', async () => {
      await transactionManager.beginTransaction();

      await transactionManager.rollbackTransaction();

      expect(mockTransaction.abort).toHaveBeenCalledTimes(1);
      expect(transactionManager.getActiveTransactionId()).toBeNull();
    });

    test('should throw error when committing without active transaction', async () => {
      await expect(transactionManager.commitTransaction()).rejects.toThrow(
        'No active transaction to commit'
      );
    });

    test('should throw error when rolling back without active transaction', async () => {
      await expect(transactionManager.rollbackTransaction()).rejects.toThrow(
        'No active transaction to rollback'
      );
    });
  });

  describe('Sending Messages in Transaction', () => {
    test('should send messages in transaction', async () => {
      await transactionManager.beginTransaction();

      const messages: TransactionMessage[] = [
        {
          topic: 'test-topic-1',
          key: 'key1',
          value: Buffer.from('message1'),
          headers: { 'header1': Buffer.from('value1') }
        },
        {
          topic: 'test-topic-1',
          key: 'key2',
          value: Buffer.from('message2')
        }
      ];

      await transactionManager.sendInTransaction(messages);

      expect(mockProducer.send).toHaveBeenCalledWith({
        topic: 'test-topic-1',
        messages: [
          {
            partition: undefined,
            key: Buffer.from('key1'),
            value: Buffer.from('message1'),
            headers: { 'header1': Buffer.from('value1') },
            timestamp: undefined
          },
          {
            partition: undefined,
            key: Buffer.from('key2'),
            value: Buffer.from('message2'),
            headers: undefined,
            timestamp: undefined
          }
        ]
      });
    });

    test('should group messages by topic', async () => {
      await transactionManager.beginTransaction();

      const messages: TransactionMessage[] = [
        {
          topic: 'topic-1',
          value: Buffer.from('message1')
        },
        {
          topic: 'topic-2',
          value: Buffer.from('message2')
        },
        {
          topic: 'topic-1',
          value: Buffer.from('message3')
        }
      ];

      await transactionManager.sendInTransaction(messages);

      expect(mockProducer.send).toHaveBeenCalledTimes(2);
      expect(mockProducer.send).toHaveBeenCalledWith({
        topic: 'topic-1',
        messages: expect.arrayContaining([
          expect.objectContaining({ value: Buffer.from('message1') }),
          expect.objectContaining({ value: Buffer.from('message3') })
        ])
      });
      expect(mockProducer.send).toHaveBeenCalledWith({
        topic: 'topic-2',
        messages: [
          expect.objectContaining({ value: Buffer.from('message2') })
        ]
      });
    });

    test('should handle empty messages array', async () => {
      await transactionManager.beginTransaction();

      await transactionManager.sendInTransaction([]);

      expect(mockProducer.send).not.toHaveBeenCalled();
    });

    test('should throw error when sending without active transaction', async () => {
      const messages: TransactionMessage[] = [
        { topic: 'test-topic', value: Buffer.from('message') }
      ];

      await expect(transactionManager.sendInTransaction(messages)).rejects.toThrow(
        'No active transaction'
      );
    });
  });

  describe('Execute Transaction', () => {
    test('should execute transaction operation successfully', async () => {
      const operation = jest.fn().mockResolvedValue('operation-result');

      const result = await transactionManager.executeTransaction(operation);

      expect(operation).toHaveBeenCalledWith(expect.any(Function));
      expect(result.result).toBe('operation-result');
      expect(result.transactionResult.success).toBe(true);
      expect(mockTransaction.commit).toHaveBeenCalledTimes(1);
      expect(transactionManager.getActiveTransactionId()).toBeNull();
    });

    test('should send messages through operation', async () => {
      const messages: TransactionMessage[] = [
        { topic: 'test-topic', value: Buffer.from('test-message') }
      ];

      const operation = async (sender: (msgs: TransactionMessage[]) => Promise<void>) => {
        await sender(messages);
        return 'success';
      };

      const result = await transactionManager.executeTransaction(operation);

      expect(mockProducer.send).toHaveBeenCalledTimes(1);
      expect(result.transactionResult.messageCount).toBe(1);
    });

    test('should rollback transaction on operation error', async () => {
      const operation = jest.fn().mockRejectedValue(new Error('Operation failed'));

      await expect(transactionManager.executeTransaction(operation)).rejects.toMatchObject({
        success: false,
        error: expect.objectContaining({ message: 'Operation failed' })
      });

      expect(mockTransaction.abort).toHaveBeenCalledTimes(1);
      expect(transactionManager.getActiveTransactionId()).toBeNull();
    });

    test('should handle rollback failure', async () => {
      const operation = jest.fn().mockRejectedValue(new Error('Operation failed'));
      mockTransaction.abort.mockRejectedValueOnce(new Error('Rollback failed'));

      await expect(transactionManager.executeTransaction(operation)).rejects.toMatchObject({
        success: false,
        error: expect.objectContaining({ message: 'Operation failed' })
      });
    });
  });

  describe('Error Handling', () => {
    test('should handle producer transaction creation failure', async () => {
      mockProducer.transaction = jest.fn().mockRejectedValue(new Error('Transaction creation failed'));

      await expect(transactionManager.beginTransaction()).rejects.toThrow(
        'Failed to start transaction: Transaction creation failed'
      );
    });

    test('should handle commit failure', async () => {
      // Reset the mock to allow transaction creation
      mockProducer.transaction = jest.fn().mockResolvedValue(mockTransaction);
      await transactionManager.beginTransaction();
      mockTransaction.commit.mockRejectedValueOnce(new Error('Commit failed'));

      await expect(transactionManager.commitTransaction()).rejects.toThrow('Commit failed');
      expect(transactionManager.getActiveTransactionId()).toBeNull();
    });

    test('should handle send failure in transaction', async () => {
      // Reset the mock to allow transaction creation
      mockProducer.transaction = jest.fn().mockResolvedValue(mockTransaction);
      await transactionManager.beginTransaction();
      (mockProducer.send as jest.Mock).mockRejectedValueOnce(new Error('Send failed'));

      const messages: TransactionMessage[] = [
        { topic: 'test-topic', value: Buffer.from('message') }
      ];

      await expect(transactionManager.sendInTransaction(messages)).rejects.toThrow('Send failed');
    });
  });

  describe('Factory Function', () => {
    test('should create transaction manager using factory', () => {
      const options = { transactionTimeout: 120000 };
      const manager = createTransactionManager(mockProducer, options);

      expect(manager).toBeInstanceOf(TransactionManager);
    });
  });
});
