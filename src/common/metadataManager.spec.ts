import {
  MetadataManager,
  EnhancedMessageMetadata,
  TraceContext,
  CorrelationContext,
  createMetadataManager
} from './metadataManager';

describe('MetadataManager', () => {
  let manager: MetadataManager;

  beforeEach(() => {
    manager = new MetadataManager();
  });

  describe('Constructor and Default Metadata', () => {
    test('should create manager without default metadata', () => {
      expect(manager).toBeInstanceOf(MetadataManager);
    });

    test('should create manager with default metadata', () => {
      const defaultMetadata = {
        source: 'test-service',
        messageType: 'event',
        userId: 'user123'
      };

      const managerWithDefaults = new MetadataManager(defaultMetadata);
      const metadata = managerWithDefaults.createMetadata();

      expect(metadata.source).toBe('test-service');
      expect(metadata.messageType).toBe('event');
      expect(metadata.userId).toBe('user123');
    });

    test('should set default metadata', () => {
      const defaultMetadata = {
        source: 'updated-service',
        version: '1.2.0'
      };

      manager.setDefaultMetadata(defaultMetadata);
      const metadata = manager.createMetadata();

      expect(metadata.source).toBe('updated-service');
      expect(metadata.version).toBe('1.2.0');
    });

    test('should update default metadata', () => {
      manager.setDefaultMetadata({ source: 'initial-service', priority: 5 });
      manager.updateDefaultMetadata({ source: 'updated-service', userId: 'user456' });

      const metadata = manager.createMetadata();

      expect(metadata.source).toBe('updated-service');
      expect(metadata.priority).toBe(5);
      expect(metadata.userId).toBe('user456');
    });
  });

  describe('Metadata Creation', () => {
    test('should create metadata with auto-generated fields', () => {
      const metadata = manager.createMetadata();

      expect(metadata.messageId).toBeDefined();
      expect(metadata.correlationId).toBeDefined();
      expect(metadata.timestamp).toBeInstanceOf(Date);
      expect(metadata.messageId).toMatch(/^msg_\d+_[a-z0-9]+$/);
      expect(metadata.correlationId).toMatch(/^corr_\d+_[a-z0-9]+$/);
    });

    test('should override auto-generated fields with provided values', () => {
      const customMetadata = {
        messageId: 'custom-message-id',
        correlationId: 'custom-correlation-id',
        messageType: 'command',
        priority: 8
      };

      const metadata = manager.createMetadata(customMetadata);

      expect(metadata.messageId).toBe('custom-message-id');
      expect(metadata.correlationId).toBe('custom-correlation-id');
      expect(metadata.messageType).toBe('command');
      expect(metadata.priority).toBe(8);
    });

    test('should merge default metadata with overrides', () => {
      manager.setDefaultMetadata({
        source: 'default-service',
        version: '1.0.0',
        priority: 5
      });

      const metadata = manager.createMetadata({
        messageType: 'event',
        priority: 9
      });

      expect(metadata.source).toBe('default-service');
      expect(metadata.version).toBe('1.0.0');
      expect(metadata.messageType).toBe('event');
      expect(metadata.priority).toBe(9);
    });
  });

  describe('Correlation Context', () => {
    test('should create correlation context without parent', () => {
      const context = manager.createCorrelationContext();

      expect(context.correlationId).toMatch(/^corr_\d+_[a-z0-9]+$/);
      expect(context.causationId).toBeUndefined();
      expect(context.conversationId).toMatch(/^conv_\d+_[a-z0-9]+$/);
    });

    test('should create correlation context with parent', () => {
      const parentCorrelationId = 'corr_1234567890_abcdef123';
      const context = manager.createCorrelationContext(parentCorrelationId);

      expect(context.correlationId).toMatch(/^corr_\d+_[a-z0-9]+$/);
      expect(context.causationId).toBe(parentCorrelationId);
      expect(context.conversationId).toMatch(/^conv_1234567890_abcdef123$/);
    });
  });

  describe('Trace Context', () => {
    test('should create trace context without parent', () => {
      const context = manager.createTraceContext();

      expect(context.traceId).toMatch(/^trace_\d+_[a-z0-9]+$/);
      expect(context.spanId).toMatch(/^span_\d+_[a-z0-9]+$/);
      expect(context.parentSpanId).toBeUndefined();
      expect(context.baggage).toEqual({});
    });

    test('should create trace context with parent', () => {
      const parentContext: Partial<TraceContext> = {
        traceId: 'trace_123_parent',
        spanId: 'span_123_parent',
        baggage: { key1: 'value1', key2: 'value2' }
      };

      const context = manager.createTraceContext(parentContext);

      expect(context.traceId).toBe('trace_123_parent');
      expect(context.spanId).toMatch(/^span_\d+_[a-z0-9]+$/);
      expect(context.parentSpanId).toBe('span_123_parent');
      expect(context.baggage).toEqual({ key1: 'value1', key2: 'value2' });
    });
  });

  describe('Header Conversion', () => {
    test('should convert metadata to headers', () => {
      const metadata: EnhancedMessageMetadata = {
        messageId: 'msg123',
        correlationId: 'corr123',
        messageType: 'event',
        priority: 5,
        timestamp: new Date('2023-01-01T00:00:00Z'),
        customHeaders: {
          'x-request-id': 'req123',
          'x-user-agent': 'test-agent'
        }
      };

      const headers = manager.metadataToHeaders(metadata);

      expect(headers['x-msg-messageId']).toEqual(Buffer.from('msg123'));
      expect(headers['x-msg-correlationId']).toEqual(Buffer.from('corr123'));
      expect(headers['x-msg-messageType']).toEqual(Buffer.from('event'));
      expect(headers['x-msg-priority']).toEqual(Buffer.from('5'));
      expect(headers['x-msg-timestamp']).toEqual(Buffer.from('2023-01-01T00:00:00.000Z'));
      expect(headers['x-custom-x-request-id']).toEqual(Buffer.from('req123'));
      expect(headers['x-custom-x-user-agent']).toEqual(Buffer.from('test-agent'));
    });

    test('should convert headers to metadata', () => {
      const headers: Record<string, Buffer> = {
        'x-msg-messageId': Buffer.from('msg123'),
        'x-msg-correlationId': Buffer.from('corr123'),
        'x-msg-messageType': Buffer.from('event'),
        'x-msg-priority': Buffer.from('5'),
        'x-msg-timestamp': Buffer.from('2023-01-01T00:00:00.000Z'),
        'x-custom-request-id': Buffer.from('req123'),
        'other-header': Buffer.from('ignored')
      };

      const metadata = manager.headersToMetadata(headers);

      expect(metadata.messageId).toBe('msg123');
      expect(metadata.correlationId).toBe('corr123');
      expect(metadata.messageType).toBe('event');
      expect(metadata.priority).toBe(5);
      expect(metadata.timestamp).toEqual(new Date('2023-01-01T00:00:00.000Z'));
      expect(metadata.customHeaders).toEqual({ 'request-id': 'req123' });
    });

    test('should handle round-trip conversion', () => {
      const originalMetadata: EnhancedMessageMetadata = {
        messageId: 'msg123',
        correlationId: 'corr123',
        messageType: 'event',
        priority: 8,
        ttl: 3600,
        timestamp: new Date('2023-01-01T12:00:00Z'),
        customHeaders: { 'request-id': 'req123' }
      };

      const headers = manager.metadataToHeaders(originalMetadata);
      const convertedMetadata = manager.headersToMetadata(headers);

      expect(convertedMetadata.messageId).toBe(originalMetadata.messageId);
      expect(convertedMetadata.correlationId).toBe(originalMetadata.correlationId);
      expect(convertedMetadata.messageType).toBe(originalMetadata.messageType);
      expect(convertedMetadata.priority).toBe(originalMetadata.priority);
      expect(convertedMetadata.ttl).toBe(originalMetadata.ttl);
      expect(convertedMetadata.timestamp).toEqual(originalMetadata.timestamp);
    });
  });

  describe('Header Transforms', () => {
    test('should add custom header transform', () => {
      const customTransform = (value: any) => `custom_${value}`;
      manager.addHeaderTransform('customField', customTransform);

      const metadata = { customField: 'test' } as any;
      const headers = manager.metadataToHeaders(metadata);

      expect(headers['x-msg-customField']).toEqual(Buffer.from('custom_test'));
    });
  });

  describe('Metadata Enrichment', () => {
    test('should enrich metadata with additional data', () => {
      const baseMetadata = {
        messageId: 'msg123',
        correlationId: 'corr123',
        customHeaders: { 'base-header': 'base-value' }
      };

      const enrichments = {
        messageType: 'event',
        priority: 5,
        customHeaders: { 'enriched-header': 'enriched-value' }
      };

      const enriched = manager.enrichMetadata(baseMetadata, enrichments);

      expect(enriched.messageId).toBe('msg123');
      expect(enriched.correlationId).toBe('corr123');
      expect(enriched.messageType).toBe('event');
      expect(enriched.priority).toBe(5);
      expect(enriched.customHeaders).toEqual({
        'base-header': 'base-value',
        'enriched-header': 'enriched-value'
      });
    });
  });

  describe('Metadata Validation', () => {
    test('should validate valid metadata', () => {
      const metadata: EnhancedMessageMetadata = {
        correlationId: 'corr123',
        messageType: 'event',
        priority: 5,
        ttl: 3600
      };

      const validation = manager.validateMetadata(metadata);

      expect(validation.isValid).toBe(true);
      expect(validation.errors).toEqual([]);
    });

    test('should identify missing required fields', () => {
      const metadata: EnhancedMessageMetadata = {
        priority: 5
      };

      const validation = manager.validateMetadata(metadata);

      expect(validation.isValid).toBe(false);
      expect(validation.errors).toContain('correlationId is required');
      expect(validation.errors).toContain('messageType is required');
    });

    test('should validate priority range', () => {
      const invalidMetadata1: EnhancedMessageMetadata = {
        correlationId: 'corr123',
        messageType: 'event',
        priority: -1
      };

      const invalidMetadata2: EnhancedMessageMetadata = {
        correlationId: 'corr123',
        messageType: 'event',
        priority: 11
      };

      expect(manager.validateMetadata(invalidMetadata1).errors).toContain('priority must be between 0 and 10');
      expect(manager.validateMetadata(invalidMetadata2).errors).toContain('priority must be between 0 and 10');
    });

    test('should validate TTL', () => {
      const invalidMetadata: EnhancedMessageMetadata = {
        correlationId: 'corr123',
        messageType: 'event',
        ttl: -1
      };

      const validation = manager.validateMetadata(invalidMetadata);

      expect(validation.errors).toContain('ttl must be positive');
    });
  });

  describe('Factory Function', () => {
    test('should create metadata manager using factory', () => {
      const defaultMetadata = { source: 'factory-test' };
      const manager = createMetadataManager(defaultMetadata);

      expect(manager).toBeInstanceOf(MetadataManager);

      const metadata = manager.createMetadata();
      expect(metadata.source).toBe('factory-test');
    });
  });
});
