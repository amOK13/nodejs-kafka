export interface TraceContext {
  traceId: string;
  spanId: string;
  parentSpanId?: string;
  baggage?: Record<string, string>;
}

export interface CorrelationContext {
  correlationId: string;
  causationId?: string;
  conversationId?: string;
}

export interface EnhancedMessageMetadata {
  correlationId?: string;
  causationId?: string;
  conversationId?: string;
  messageId?: string;
  messageType?: string;
  source?: string;
  destination?: string;
  timestamp?: Date;
  userId?: string;
  sessionId?: string;
  tenantId?: string;
  version?: string;
  priority?: number;
  ttl?: number;
  retryCount?: number;
  traceContext?: TraceContext;
  customHeaders?: Record<string, string>;
}

export class MetadataManager {
  private defaultMetadata: Partial<EnhancedMessageMetadata> = {};
  private headerTransforms: Map<string, (value: any) => string> = new Map();

  constructor(defaultMetadata?: Partial<EnhancedMessageMetadata>) {
    if (defaultMetadata) {
      this.defaultMetadata = { ...defaultMetadata };
    }
    this.setupDefaultTransforms();
  }

  setDefaultMetadata(metadata: Partial<EnhancedMessageMetadata>): void {
    this.defaultMetadata = { ...metadata };
  }

  updateDefaultMetadata(updates: Partial<EnhancedMessageMetadata>): void {
    this.defaultMetadata = { ...this.defaultMetadata, ...updates };
  }

  createMetadata(overrides?: Partial<EnhancedMessageMetadata>): EnhancedMessageMetadata {
    const metadata: EnhancedMessageMetadata = {
      ...this.defaultMetadata,
      messageId: this.generateMessageId(),
      timestamp: new Date(),
      ...overrides
    };

    if (!metadata.correlationId) {
      metadata.correlationId = this.generateCorrelationId();
    }

    return metadata;
  }

  createCorrelationContext(parentCorrelationId?: string): CorrelationContext {
    return {
      correlationId: this.generateCorrelationId(),
      causationId: parentCorrelationId,
      conversationId: parentCorrelationId
        ? this.extractConversationId(parentCorrelationId)
        : this.generateConversationId()
    };
  }

  createTraceContext(parentContext?: Partial<TraceContext>): TraceContext {
    return {
      traceId: parentContext?.traceId || this.generateTraceId(),
      spanId: this.generateSpanId(),
      parentSpanId: parentContext?.spanId,
      baggage: parentContext?.baggage ? { ...parentContext.baggage } : {}
    };
  }

  metadataToHeaders(metadata: EnhancedMessageMetadata): Record<string, Buffer> {
    const headers: Record<string, Buffer> = {};

    Object.entries(metadata).forEach(([key, value]) => {
      if (value !== undefined && value !== null) {
        const transform = this.headerTransforms.get(key) || this.defaultTransform;
        headers[`x-msg-${key}`] = Buffer.from(transform(value));
      }
    });

    if (metadata.customHeaders) {
      Object.entries(metadata.customHeaders).forEach(([key, value]) => {
        headers[`x-custom-${key}`] = Buffer.from(value);
      });
    }

    return headers;
  }

  headersToMetadata(headers: Record<string, Buffer>): Partial<EnhancedMessageMetadata> {
    const metadata: Partial<EnhancedMessageMetadata> = {};
    const customHeaders: Record<string, string> = {};

    Object.entries(headers).forEach(([key, value]) => {
      const stringValue = value.toString();

      if (key.startsWith('x-msg-')) {
        const metadataKey = key.substring(6) as keyof EnhancedMessageMetadata;
        this.setMetadataValue(metadata, metadataKey, stringValue);
      } else if (key.startsWith('x-custom-')) {
        const customKey = key.substring(9);
        customHeaders[customKey] = stringValue;
      }
    });

    if (Object.keys(customHeaders).length > 0) {
      metadata.customHeaders = customHeaders;
    }

    return metadata;
  }

  addHeaderTransform(key: string, transform: (value: any) => string): void {
    this.headerTransforms.set(key, transform);
  }

  enrichMetadata(
    baseMetadata: Partial<EnhancedMessageMetadata>,
    enrichments: Partial<EnhancedMessageMetadata>
  ): EnhancedMessageMetadata {
    return {
      ...baseMetadata,
      ...enrichments,
      customHeaders: {
        ...baseMetadata.customHeaders,
        ...enrichments.customHeaders
      }
    };
  }

  validateMetadata(metadata: EnhancedMessageMetadata): { isValid: boolean; errors: string[] } {
    const errors: string[] = [];

    if (!metadata.correlationId) {
      errors.push('correlationId is required');
    }

    if (!metadata.messageType) {
      errors.push('messageType is required');
    }

    if (metadata.priority !== undefined && (metadata.priority < 0 || metadata.priority > 10)) {
      errors.push('priority must be between 0 and 10');
    }

    if (metadata.ttl !== undefined && metadata.ttl <= 0) {
      errors.push('ttl must be positive');
    }

    return {
      isValid: errors.length === 0,
      errors
    };
  }

  private generateMessageId(): string {
    return `msg_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  }

  private generateCorrelationId(): string {
    return `corr_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  }

  private generateConversationId(): string {
    return `conv_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  }

  private generateTraceId(): string {
    return `trace_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  }

  private generateSpanId(): string {
    return `span_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  }

  private extractConversationId(correlationId: string): string {
    const parts = correlationId.split('_');
    return parts.length >= 3 ? `conv_${parts[1]}_${parts[2]}` : this.generateConversationId();
  }

  private setupDefaultTransforms(): void {
    this.headerTransforms.set('timestamp', (value: Date) => value.toISOString());
    this.headerTransforms.set('priority', (value: number) => value.toString());
    this.headerTransforms.set('ttl', (value: number) => value.toString());
    this.headerTransforms.set('retryCount', (value: number) => value.toString());
    this.headerTransforms.set('traceContext', (value: TraceContext) => JSON.stringify(value));
  }

  private defaultTransform(value: any): string {
    if (typeof value === 'string') {
      return value;
    }
    return JSON.stringify(value);
  }

  private setMetadataValue(
    metadata: Partial<EnhancedMessageMetadata>,
    key: keyof EnhancedMessageMetadata,
    value: string
  ): void {
    switch (key) {
    case 'timestamp':
      metadata[key] = new Date(value);
      break;
    case 'priority':
    case 'ttl':
    case 'retryCount':
      metadata[key] = parseInt(value, 10);
      break;
    case 'traceContext':
      metadata[key] = JSON.parse(value);
      break;
    default:
      (metadata as any)[key] = value;
    }
  }
}

export const createMetadataManager = (defaultMetadata?: Partial<EnhancedMessageMetadata>): MetadataManager => {
  return new MetadataManager(defaultMetadata);
};
