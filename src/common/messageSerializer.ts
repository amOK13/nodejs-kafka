import { logger } from './logger';

export type SerializationFormat = 'string' | 'json';

export interface MessageSerializer {
  serialize(data: any): string;
  deserialize(data: string): any;
}

export class StringSerializer implements MessageSerializer {
  serialize(data: any): string {
    if (typeof data === 'string') {
      return data;
    }
    return String(data);
  }

  deserialize(data: string): string {
    return data;
  }
}

export class JsonSerializer implements MessageSerializer {
  serialize(data: any): string {
    try {
      if (typeof data === 'string') {
        JSON.parse(data);
        return data;
      }
      return JSON.stringify(data);
    } catch (error) {
      logger.error('Failed to serialize message as JSON', { data, error: error instanceof Error ? error.message : error });
      throw new Error(`Serialization failed: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  }

  deserialize(data: string): any {
    try {
      return JSON.parse(data);
    } catch (error) {
      logger.error('Failed to deserialize JSON message', { data, error: error instanceof Error ? error.message : error });
      throw new Error(`Deserialization failed: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  }
}

export class MessageSerializerFactory {
  static create(format: SerializationFormat): MessageSerializer {
    switch (format) {
      case 'string':
        return new StringSerializer();
      case 'json':
        return new JsonSerializer();
      default:
        throw new Error(`Unsupported serialization format: ${format}`);
    }
  }
}