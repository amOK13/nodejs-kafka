import {
  HashPartitioner,
  RoundRobinPartitioner,
  ContentBasedPartitioner,
  CustomPartitioner,
  WeightedPartitioner,
  PartitionerFactory,
  PartitionerContext
} from './partitioners';

describe('Partitioners', () => {
  const createContext = (overrides: Partial<PartitionerContext> = {}): PartitionerContext => ({
    topic: 'test-topic',
    partitionCount: 3,
    message: { id: 1, data: 'test' },
    key: 'test-key',
    ...overrides
  });

  describe('HashPartitioner', () => {
    let partitioner: HashPartitioner;

    beforeEach(() => {
      partitioner = new HashPartitioner();
    });

    test('should have correct name', () => {
      expect(partitioner.getName()).toBe('HashPartitioner');
    });

    test('should return consistent partition for same key', () => {
      const context = createContext({ key: 'consistent-key' });

      const partition1 = partitioner.partition(context);
      const partition2 = partitioner.partition(context);

      expect(partition1).toBe(partition2);
      expect(partition1).toBeGreaterThanOrEqual(0);
      expect(partition1).toBeLessThan(context.partitionCount);
    });

    test('should return random partition when no key provided', () => {
      const context = createContext({ key: undefined });

      const partition = partitioner.partition(context);

      expect(partition).toBeGreaterThanOrEqual(0);
      expect(partition).toBeLessThan(context.partitionCount);
    });

    test('should distribute different keys across partitions', () => {
      const partitions = new Set<number>();

      for (let i = 0; i < 100; i++) {
        const context = createContext({ key: `key-${i}` });
        const partition = partitioner.partition(context);
        partitions.add(partition);
      }

      expect(partitions.size).toBeGreaterThan(1);
    });
  });

  describe('RoundRobinPartitioner', () => {
    let partitioner: RoundRobinPartitioner;

    beforeEach(() => {
      partitioner = new RoundRobinPartitioner();
    });

    test('should have correct name', () => {
      expect(partitioner.getName()).toBe('RoundRobinPartitioner');
    });

    test('should cycle through partitions sequentially', () => {
      const context = createContext();

      expect(partitioner.partition(context)).toBe(0);
      expect(partitioner.partition(context)).toBe(1);
      expect(partitioner.partition(context)).toBe(2);
      expect(partitioner.partition(context)).toBe(0);
      expect(partitioner.partition(context)).toBe(1);
    });

    test('should reset counter', () => {
      const context = createContext();

      partitioner.partition(context);
      partitioner.partition(context);

      partitioner.reset();

      expect(partitioner.partition(context)).toBe(0);
    });

    test('should handle different partition counts', () => {
      const context = createContext({ partitionCount: 5 });

      const partitions = [];
      for (let i = 0; i < 10; i++) {
        partitions.push(partitioner.partition(context));
      }

      expect(partitions).toEqual([0, 1, 2, 3, 4, 0, 1, 2, 3, 4]);
    });
  });

  describe('ContentBasedPartitioner', () => {
    test('should have correct name with content path', () => {
      const partitioner = new ContentBasedPartitioner('user.id');
      expect(partitioner.getName()).toBe('ContentBasedPartitioner(user.id)');
    });

    test('should partition based on content path', () => {
      const partitioner = new ContentBasedPartitioner('user.id');
      const context = createContext({
        message: { user: { id: 'user123' } }
      });

      const partition1 = partitioner.partition(context);
      const partition2 = partitioner.partition(context);

      expect(partition1).toBe(partition2);
      expect(partition1).toBeGreaterThanOrEqual(0);
      expect(partition1).toBeLessThan(context.partitionCount);
    });

    test('should return 0 when content path not found', () => {
      const partitioner = new ContentBasedPartitioner('nonexistent.path');
      const context = createContext();

      const partition = partitioner.partition(context);

      expect(partition).toBe(0);
    });

    test('should handle nested paths', () => {
      const partitioner = new ContentBasedPartitioner('data.nested.value');
      const context = createContext({
        message: { data: { nested: { value: 'test' } } }
      });

      const partition = partitioner.partition(context);

      expect(partition).toBeGreaterThanOrEqual(0);
      expect(partition).toBeLessThan(context.partitionCount);
    });

    test('should use custom hash function', () => {
      const customHash = jest.fn().mockReturnValue(42);
      const partitioner = new ContentBasedPartitioner('id', customHash);
      const context = createContext({
        message: { id: 'test-id' }
      });

      partitioner.partition(context);

      expect(customHash).toHaveBeenCalledWith('test-id');
    });
  });

  describe('CustomPartitioner', () => {
    test('should use custom partition function', () => {
      const customFunction = jest.fn().mockReturnValue(1);
      const partitioner = new CustomPartitioner('CustomTest', customFunction);

      expect(partitioner.getName()).toBe('CustomTest');

      const context = createContext();
      const partition = partitioner.partition(context);

      expect(customFunction).toHaveBeenCalledWith(context);
      expect(partition).toBe(1);
    });

    test('should clamp partition to valid range', () => {
      const partitioner = new CustomPartitioner('Test', () => 10);
      const context = createContext({ partitionCount: 3 });

      const partition = partitioner.partition(context);

      expect(partition).toBe(2); // Clamped to partitionCount - 1
    });

    test('should handle negative values', () => {
      const partitioner = new CustomPartitioner('Test', () => -5);
      const context = createContext();

      const partition = partitioner.partition(context);

      expect(partition).toBe(0);
    });
  });

  describe('WeightedPartitioner', () => {
    test('should throw error for empty weights', () => {
      expect(() => new WeightedPartitioner([])).toThrow('Weights array cannot be empty');
    });

    test('should have correct name', () => {
      const partitioner = new WeightedPartitioner([1, 2, 3]);
      expect(partitioner.getName()).toBe('WeightedPartitioner');
    });

    test('should return weights', () => {
      const weights = [1, 2, 3];
      const partitioner = new WeightedPartitioner(weights);

      expect(partitioner.getWeights()).toEqual(weights);
    });

    test('should distribute according to weights', () => {
      const partitioner = new WeightedPartitioner([1, 9]); // 10% vs 90%
      const context = createContext({ partitionCount: 2 });

      const partitions = { 0: 0, 1: 0 };
      const iterations = 1000;

      for (let i = 0; i < iterations; i++) {
        const partition = partitioner.partition(context);
        partitions[partition as keyof typeof partitions]++;
      }

      // Partition 1 should have roughly 9x more assignments than partition 0
      const ratio = partitions[1] / partitions[0];
      expect(ratio).toBeGreaterThan(5); // Allow some variance
      expect(ratio).toBeLessThan(15);
    });

    test('should handle partition count smaller than weights array', () => {
      const partitioner = new WeightedPartitioner([1, 2, 3, 4, 5]);
      const context = createContext({ partitionCount: 3 });

      // Should only return partitions 0, 1, or 2
      for (let i = 0; i < 100; i++) {
        const partition = partitioner.partition(context);
        expect(partition).toBeGreaterThanOrEqual(0);
        expect(partition).toBeLessThan(3);
      }
    });
  });

  describe('PartitionerFactory', () => {
    test('should create HashPartitioner', () => {
      const partitioner = PartitionerFactory.createHash();
      expect(partitioner).toBeInstanceOf(HashPartitioner);
    });

    test('should create RoundRobinPartitioner', () => {
      const partitioner = PartitionerFactory.createRoundRobin();
      expect(partitioner).toBeInstanceOf(RoundRobinPartitioner);
    });

    test('should create ContentBasedPartitioner', () => {
      const partitioner = PartitionerFactory.createContentBased('user.id');
      expect(partitioner).toBeInstanceOf(ContentBasedPartitioner);
    });

    test('should create CustomPartitioner', () => {
      const customFunction = () => 0;
      const partitioner = PartitionerFactory.createCustom('Test', customFunction);
      expect(partitioner).toBeInstanceOf(CustomPartitioner);
    });

    test('should create WeightedPartitioner', () => {
      const partitioner = PartitionerFactory.createWeighted([1, 2, 3]);
      expect(partitioner).toBeInstanceOf(WeightedPartitioner);
    });
  });
});
