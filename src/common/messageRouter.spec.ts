import {
  MessageRouter,
  RoutingRule,
  MessageMetadata,
  createContentBasedRule,
  createMetadataBasedRule,
  createPatternBasedRule
} from './messageRouter';

describe('MessageRouter', () => {
  let router: MessageRouter;
  const defaultTopic = 'default-topic';

  beforeEach(() => {
    router = new MessageRouter(defaultTopic);
  });

  describe('Constructor', () => {
    test('should create router with default topic', () => {
      expect(router.getDefaultTopic()).toBe(defaultTopic);
    });
  });

  describe('Rule Management', () => {
    test('should add routing rule', () => {
      const rule: RoutingRule = {
        id: 'test-rule',
        name: 'Test Rule',
        condition: () => true,
        targetTopic: 'test-topic',
        priority: 100
      };

      router.addRule(rule);
      const rules = router.getRules();

      expect(rules).toHaveLength(1);
      expect(rules[0]).toEqual(rule);
    });

    test('should sort rules by priority (highest first)', () => {
      const lowPriorityRule: RoutingRule = {
        id: 'low',
        name: 'Low Priority',
        condition: () => true,
        targetTopic: 'low-topic',
        priority: 50
      };

      const highPriorityRule: RoutingRule = {
        id: 'high',
        name: 'High Priority',
        condition: () => true,
        targetTopic: 'high-topic',
        priority: 200
      };

      router.addRule(lowPriorityRule);
      router.addRule(highPriorityRule);

      const rules = router.getRules();
      expect(rules[0]).toEqual(highPriorityRule);
      expect(rules[1]).toEqual(lowPriorityRule);
    });

    test('should remove routing rule by id', () => {
      const rule: RoutingRule = {
        id: 'test-rule',
        name: 'Test Rule',
        condition: () => true,
        targetTopic: 'test-topic',
        priority: 100
      };

      router.addRule(rule);
      expect(router.getRules()).toHaveLength(1);

      const removed = router.removeRule('test-rule');
      expect(removed).toBe(true);
      expect(router.getRules()).toHaveLength(0);
    });

    test('should return false when removing non-existent rule', () => {
      const removed = router.removeRule('non-existent');
      expect(removed).toBe(false);
    });

    test('should update existing rule', () => {
      const rule: RoutingRule = {
        id: 'test-rule',
        name: 'Test Rule',
        condition: () => true,
        targetTopic: 'test-topic',
        priority: 100
      };

      router.addRule(rule);

      const updated = router.updateRule('test-rule', {
        name: 'Updated Rule',
        targetTopic: 'updated-topic'
      });

      expect(updated).toBe(true);

      const rules = router.getRules();
      expect(rules[0].name).toBe('Updated Rule');
      expect(rules[0].targetTopic).toBe('updated-topic');
    });

    test('should clear all rules', () => {
      const rule1: RoutingRule = {
        id: 'rule1',
        name: 'Rule 1',
        condition: () => true,
        targetTopic: 'topic1',
        priority: 100
      };

      const rule2: RoutingRule = {
        id: 'rule2',
        name: 'Rule 2',
        condition: () => true,
        targetTopic: 'topic2',
        priority: 100
      };

      router.addRule(rule1);
      router.addRule(rule2);
      expect(router.getRules()).toHaveLength(2);

      router.clearRules();
      expect(router.getRules()).toHaveLength(0);
    });
  });

  describe('Message Routing', () => {
    test('should route message using matching rule', () => {
      const rule: RoutingRule = {
        id: 'test-rule',
        name: 'Test Rule',
        condition: message => message.type === 'test',
        targetTopic: 'test-topic',
        priority: 100
      };

      router.addRule(rule);

      const result = router.route({ type: 'test', data: 'some data' });

      expect(result.topic).toBe('test-topic');
      expect(result.matchedRule).toEqual(rule);
      expect(result.defaultRoute).toBe(false);
    });

    test('should use default topic when no rules match', () => {
      const rule: RoutingRule = {
        id: 'test-rule',
        name: 'Test Rule',
        condition: message => message.type === 'test',
        targetTopic: 'test-topic',
        priority: 100
      };

      router.addRule(rule);

      const result = router.route({ type: 'other', data: 'some data' });

      expect(result.topic).toBe(defaultTopic);
      expect(result.matchedRule).toBeUndefined();
      expect(result.defaultRoute).toBe(true);
    });

    test('should handle rule evaluation errors gracefully', () => {
      const rule: RoutingRule = {
        id: 'error-rule',
        name: 'Error Rule',
        condition: () => {
          throw new Error('Rule evaluation error');
        },
        targetTopic: 'error-topic',
        priority: 100
      };

      router.addRule(rule);

      const consoleSpy = jest.spyOn(console, 'warn').mockImplementation();

      const result = router.route({ type: 'test' });

      expect(result.topic).toBe(defaultTopic);
      expect(result.defaultRoute).toBe(true);
      expect(consoleSpy).toHaveBeenCalledWith(
        'Error evaluating routing rule error-rule:',
        expect.any(Error)
      );

      consoleSpy.mockRestore();
    });

    test('should use highest priority rule when multiple rules match', () => {
      const lowPriorityRule: RoutingRule = {
        id: 'low',
        name: 'Low Priority',
        condition: () => true,
        targetTopic: 'low-topic',
        priority: 50
      };

      const highPriorityRule: RoutingRule = {
        id: 'high',
        name: 'High Priority',
        condition: () => true,
        targetTopic: 'high-topic',
        priority: 200
      };

      router.addRule(lowPriorityRule);
      router.addRule(highPriorityRule);

      const result = router.route({ type: 'test' });

      expect(result.topic).toBe('high-topic');
      expect(result.matchedRule).toEqual(highPriorityRule);
    });
  });

  describe('Helper Functions', () => {
    test('createContentBasedRule should create rule based on message content', () => {
      const rule = createContentBasedRule(
        'content-rule',
        'Content Based Rule',
        'user.type',
        'premium',
        'premium-topic'
      );

      expect(rule.id).toBe('content-rule');
      expect(rule.name).toBe('Content Based Rule');
      expect(rule.targetTopic).toBe('premium-topic');
      expect(rule.priority).toBe(100);

      expect(rule.condition({ user: { type: 'premium' } })).toBe(true);
      expect(rule.condition({ user: { type: 'basic' } })).toBe(false);
      expect(rule.condition({ other: 'data' })).toBe(false);
    });

    test('createMetadataBasedRule should create rule based on metadata', () => {
      const rule = createMetadataBasedRule(
        'metadata-rule',
        'Metadata Based Rule',
        'messageType',
        'event',
        'event-topic'
      );

      const metadata: MessageMetadata = {
        messageType: 'event',
        source: 'user-service'
      };

      expect(rule.condition({}, metadata)).toBe(true);
      expect(rule.condition({}, { messageType: 'command' })).toBe(false);
      expect(rule.condition({}, undefined)).toBe(false);
    });

    test('createPatternBasedRule should create rule based on regex pattern', () => {
      const rule = createPatternBasedRule(
        'pattern-rule',
        'Pattern Based Rule',
        'email',
        /.*@company\.com$/,
        'internal-topic'
      );

      expect(rule.condition({ email: 'user@company.com' })).toBe(true);
      expect(rule.condition({ email: 'user@external.com' })).toBe(false);
      expect(rule.condition({ email: 123 })).toBe(false);
      expect(rule.condition({ other: 'data' })).toBe(false);
    });
  });

  describe('Default Topic Management', () => {
    test('should set and get default topic', () => {
      const newDefaultTopic = 'new-default-topic';
      router.setDefaultTopic(newDefaultTopic);

      expect(router.getDefaultTopic()).toBe(newDefaultTopic);
    });
  });
});
