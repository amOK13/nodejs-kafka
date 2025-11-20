import { Producer } from 'kafkajs';
import { createProducer } from '../common/kafkaClient';
import { config } from '../common/config';
import { logger } from '../common/logger';
import { MessageValidator, MessageSchema } from '../common/messageValidator';
import { MessageSerializer, MessageSerializerFactory, SerializationFormat } from '../common/messageSerializer';

export interface ProducerOptions {
  serializationFormat?: SerializationFormat;
  enableValidation?: boolean;
  schema?: MessageSchema;
}

export class MessageProducer {
  private producer!: Producer;
  private validator: MessageValidator;
  private serializer: MessageSerializer;
  private enableValidation: boolean;

  constructor(private options: ProducerOptions = {}) {
    this.enableValidation = options.enableValidation ?? true;
    this.validator = new MessageValidator(options.schema);
    this.serializer = MessageSerializerFactory.create(options.serializationFormat ?? 'json');
  }

  async initialize(): Promise<void> {
    this.producer = await createProducer();
    logger.info('Producer initialized with validation and serialization support');
  }

  async sendMessage(message: any, key?: string, headers?: Record<string, string>): Promise<void> {
    try {
      if (this.enableValidation) {
        const validationResult = this.validator.validate(message);
        if (!validationResult.isValid) {
          const error = new Error(`Message validation failed: ${validationResult.error || 'Unknown validation error'}`);
          logger.error('Message validation failed', { 
            topic: config.kafkaTopic, 
            message, 
            validationError: validationResult.error 
          });
          throw error;
        }
      }

      const serializedMessage = this.serializer.serialize(message);
      const kafkaHeaders: Record<string, Buffer> = {};

      if (headers) {
        Object.keys(headers).forEach(headerKey => {
          kafkaHeaders[headerKey] = Buffer.from(headers[headerKey]);
        });
      }

      await this.producer.send({
        topic: config.kafkaTopic,
        messages: [
          {
            key: key ? Buffer.from(key) : undefined,
            value: serializedMessage,
            headers: Object.keys(kafkaHeaders).length > 0 ? kafkaHeaders : undefined,
            timestamp: Date.now().toString(),
          },
        ],
      });
      
      logger.info('Message sent successfully', { 
        topic: config.kafkaTopic,
        messageLength: serializedMessage.length,
        hasKey: !!key,
        hasHeaders: !!headers 
      });
    } catch (error) {
      logger.error('Failed to send message', { topic: config.kafkaTopic, message, error });
      throw error;
    }
  }

  async sendBatch(messages: Array<{ message: any; key?: string; headers?: Record<string, string> }>): Promise<void> {
    try {
      const kafkaMessages = [];
      for (const { message, key, headers } of messages) {
        if (this.enableValidation) {
          const validationResult = this.validator.validate(message);
          if (!validationResult.isValid) {
            const error = new Error(`Batch message validation failed: ${validationResult.error || 'Unknown validation error'}`);
            logger.error('Batch message validation failed', { 
              topic: config.kafkaTopic, 
              message, 
              validationError: validationResult.error 
            });
            throw error;
          }
        }

        const serializedMessage = this.serializer.serialize(message);
        const kafkaHeaders: Record<string, Buffer> = {};

        if (headers) {
          Object.keys(headers).forEach(headerKey => {
            kafkaHeaders[headerKey] = Buffer.from(headers[headerKey]);
          });
        }

        kafkaMessages.push({
          key: key ? Buffer.from(key) : undefined,
          value: serializedMessage,
          headers: Object.keys(kafkaHeaders).length > 0 ? kafkaHeaders : undefined,
          timestamp: Date.now().toString(),
        });
      }

      await this.producer.send({
        topic: config.kafkaTopic,
        messages: kafkaMessages,
      });

      logger.info('Batch messages sent successfully', { 
        topic: config.kafkaTopic,
        messageCount: messages.length 
      });
    } catch (error) {
      logger.error('Failed to send batch messages', { topic: config.kafkaTopic, messageCount: messages.length, error });
      throw error;
    }
  }

  updateSchema(schema: MessageSchema): void {
    this.validator = new MessageValidator(schema);
    logger.info('Producer schema updated');
  }

  setValidationEnabled(enabled: boolean): void {
    this.enableValidation = enabled;
    logger.info('Producer validation status changed', { enabled });
  }

  async disconnect(): Promise<void> {
    if (this.producer) {
      await this.producer.disconnect();
      logger.info('Producer disconnected');
    }
  }
}