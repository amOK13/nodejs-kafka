export interface PartitionerContext {
  topic: string;
  partitionCount: number;
  message: any;
  key?: string;
  metadata?: any;
}

export interface Partitioner {
  partition(context: PartitionerContext): number;
  getName(): string;
}

export class HashPartitioner implements Partitioner {
  getName(): string {
    return 'HashPartitioner';
  }

  partition(context: PartitionerContext): number {
    if (!context.key) {
      return Math.floor(Math.random() * context.partitionCount);
    }

    const hash = this.simpleHash(context.key);
    return Math.abs(hash) % context.partitionCount;
  }

  private simpleHash(str: string): number {
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
      const char = str.charCodeAt(i);
      hash = ((hash << 5) - hash) + char;
      hash = hash & hash;
    }
    return hash;
  }
}

export class RoundRobinPartitioner implements Partitioner {
  private counter: number = 0;

  getName(): string {
    return 'RoundRobinPartitioner';
  }

  partition(context: PartitionerContext): number {
    const partition = this.counter % context.partitionCount;
    this.counter = (this.counter + 1) % context.partitionCount;
    return partition;
  }

  reset(): void {
    this.counter = 0;
  }
}

export class ContentBasedPartitioner implements Partitioner {
  private contentPath: string;
  private hashFunction: (value: any) => number;

  constructor(contentPath: string, hashFunction?: (value: any) => number) {
    this.contentPath = contentPath;
    this.hashFunction = hashFunction || this.defaultHashFunction;
  }

  getName(): string {
    return `ContentBasedPartitioner(${this.contentPath})`;
  }

  partition(context: PartitionerContext): number {
    const pathParts = this.contentPath.split('.');
    let current = context.message;

    for (const part of pathParts) {
      if (current === null || current === undefined) {
        return 0;
      }
      current = current[part];
    }

    if (current === null || current === undefined) {
      return 0;
    }

    const hash = this.hashFunction(current);
    return Math.abs(hash) % context.partitionCount;
  }

  private defaultHashFunction(value: any): number {
    const str = String(value);
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
      const char = str.charCodeAt(i);
      hash = ((hash << 5) - hash) + char;
      hash = hash & hash;
    }
    return hash;
  }
}

export class CustomPartitioner implements Partitioner {
  private name: string;
  private partitionFunction: (context: PartitionerContext) => number;

  constructor(name: string, partitionFunction: (context: PartitionerContext) => number) {
    this.name = name;
    this.partitionFunction = partitionFunction;
  }

  getName(): string {
    return this.name;
  }

  partition(context: PartitionerContext): number {
    const result = this.partitionFunction(context);
    return Math.max(0, Math.min(result, context.partitionCount - 1));
  }
}

export class WeightedPartitioner implements Partitioner {
  private weights: number[];
  private cumulativeWeights: number[];

  constructor(weights: number[]) {
    if (weights.length === 0) {
      throw new Error('Weights array cannot be empty');
    }

    this.weights = [...weights];
    this.cumulativeWeights = [];

    let sum = 0;
    for (const weight of this.weights) {
      sum += weight;
      this.cumulativeWeights.push(sum);
    }
  }

  getName(): string {
    return 'WeightedPartitioner';
  }

  partition(context: PartitionerContext): number {
    const totalWeight = this.cumulativeWeights[this.cumulativeWeights.length - 1];
    const random = Math.random() * totalWeight;

    for (let i = 0; i < this.cumulativeWeights.length && i < context.partitionCount; i++) {
      if (random <= this.cumulativeWeights[i]) {
        return i;
      }
    }

    return 0;
  }

  getWeights(): number[] {
    return [...this.weights];
  }
}

export const PartitionerFactory = {
  createHash(): HashPartitioner {
    return new HashPartitioner();
  },

  createRoundRobin(): RoundRobinPartitioner {
    return new RoundRobinPartitioner();
  },

  createContentBased(contentPath: string, hashFunction?: (value: any) => number): ContentBasedPartitioner {
    return new ContentBasedPartitioner(contentPath, hashFunction);
  },

  createCustom(name: string, partitionFunction: (context: PartitionerContext) => number): CustomPartitioner {
    return new CustomPartitioner(name, partitionFunction);
  },

  createWeighted(weights: number[]): WeightedPartitioner {
    return new WeightedPartitioner(weights);
  }
};
