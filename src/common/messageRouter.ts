export interface RoutingRule {
  id: string;
  name: string;
  condition: (message: any, metadata?: MessageMetadata) => boolean;
  targetTopic: string;
  priority: number;
}

export interface MessageMetadata {
  correlationId?: string;
  messageType?: string;
  source?: string;
  timestamp?: Date;
  userId?: string;
  traceId?: string;
  headers?: Record<string, string>;
}

export interface RoutingResult {
  topic: string;
  matchedRule?: RoutingRule;
  defaultRoute: boolean;
}

export class MessageRouter {
  private rules: RoutingRule[] = [];
  private defaultTopic: string;

  constructor(defaultTopic: string) {
    this.defaultTopic = defaultTopic;
  }

  addRule(rule: RoutingRule): void {
    this.rules.push(rule);
    this.rules.sort((a, b) => b.priority - a.priority);
  }

  removeRule(ruleId: string): boolean {
    const initialLength = this.rules.length;
    this.rules = this.rules.filter(rule => rule.id !== ruleId);
    return this.rules.length < initialLength;
  }

  updateRule(ruleId: string, updatedRule: Partial<RoutingRule>): boolean {
    const ruleIndex = this.rules.findIndex(rule => rule.id === ruleId);
    if (ruleIndex === -1) {
      return false;
    }

    this.rules[ruleIndex] = { ...this.rules[ruleIndex], ...updatedRule };
    this.rules.sort((a, b) => b.priority - a.priority);
    return true;
  }

  route(message: any, metadata?: MessageMetadata): RoutingResult {
    for (const rule of this.rules) {
      try {
        if (rule.condition(message, metadata)) {
          return {
            topic: rule.targetTopic,
            matchedRule: rule,
            defaultRoute: false
          };
        }
      } catch (error) {
        console.warn(`Error evaluating routing rule ${rule.id}:`, error);
      }
    }

    return {
      topic: this.defaultTopic,
      defaultRoute: true
    };
  }

  getRules(): RoutingRule[] {
    return [...this.rules];
  }

  clearRules(): void {
    this.rules = [];
  }

  setDefaultTopic(topic: string): void {
    this.defaultTopic = topic;
  }

  getDefaultTopic(): string {
    return this.defaultTopic;
  }
}

export const createContentBasedRule = (
  id: string,
  name: string,
  contentPath: string,
  expectedValue: any,
  targetTopic: string,
  priority: number = 100
): RoutingRule => {
  return {
    id,
    name,
    condition: message => {
      const pathParts = contentPath.split('.');
      let current = message;

      for (const part of pathParts) {
        if (current === null || current === undefined) {
          return false;
        }
        current = current[part];
      }

      return current === expectedValue;
    },
    targetTopic,
    priority
  };
};

export const createMetadataBasedRule = (
  id: string,
  name: string,
  metadataKey: keyof MessageMetadata,
  expectedValue: any,
  targetTopic: string,
  priority: number = 100
): RoutingRule => {
  return {
    id,
    name,
    condition: (message, metadata) => {
      return Boolean(metadata && metadata[metadataKey] === expectedValue);
    },
    targetTopic,
    priority
  };
};

export const createPatternBasedRule = (
  id: string,
  name: string,
  contentPath: string,
  pattern: RegExp,
  targetTopic: string,
  priority: number = 100
): RoutingRule => {
  return {
    id,
    name,
    condition: message => {
      const pathParts = contentPath.split('.');
      let current = message;

      for (const part of pathParts) {
        if (current === null || current === undefined) {
          return false;
        }
        current = current[part];
      }

      return typeof current === 'string' && pattern.test(current);
    },
    targetTopic,
    priority
  };
};
