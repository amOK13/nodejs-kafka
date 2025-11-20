export interface MessageSchema {
  validate(message: any): { isValid: boolean; error?: string };
}

export class TextMessageSchema implements MessageSchema {
  validate(message: any): { isValid: boolean; error?: string } {
    if (typeof message !== 'string') {
      return { isValid: false, error: 'Message must be a string' };
    }
    
    if (message.trim().length === 0) {
      return { isValid: false, error: 'Message cannot be empty' };
    }
    
    if (message.length > 10000) {
      return { isValid: false, error: 'Message too long (max 10000 characters)' };
    }
    
    return { isValid: true };
  }
}

export class JsonMessageSchema implements MessageSchema {
  validate(message: any): { isValid: boolean; error?: string } {
    try {
      if (typeof message === 'string') {
        JSON.parse(message);
      } else if (typeof message === 'object' && message !== null) {
        JSON.stringify(message);
      } else {
        return { isValid: false, error: 'Message must be a valid JSON object or string' };
      }
      return { isValid: true };
    } catch (error) {
      return { isValid: false, error: `Invalid JSON: ${error instanceof Error ? error.message : 'Unknown error'}` };
    }
  }
}

export class MessageValidator {
  private schema: MessageSchema;

  constructor(schema: MessageSchema = new TextMessageSchema()) {
    this.schema = schema;
  }

  validate(message: any): { isValid: boolean; error?: string } {
    return this.schema.validate(message);
  }

  setSchema(schema: MessageSchema): void {
    this.schema = schema;
  }
}