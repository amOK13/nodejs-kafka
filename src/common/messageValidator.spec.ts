import { MessageValidator, TextMessageSchema, JsonMessageSchema } from './messageValidator';

describe('MessageValidator', () => {
  let validator: MessageValidator;

  describe('TextMessageSchema', () => {
    beforeEach(() => {
      validator = new MessageValidator(new TextMessageSchema());
    });

    test('should validate valid string messages', () => {
      const result = validator.validate('Hello World');
      expect(result.isValid).toBe(true);
      expect(result.error).toBeUndefined();
    });

    test('should reject non-string messages', () => {
      const result = validator.validate({ message: 'test' });
      expect(result.isValid).toBe(false);
      expect(result.error).toBe('Message must be a string');
    });

    test('should reject empty messages', () => {
      const result = validator.validate('   ');
      expect(result.isValid).toBe(false);
      expect(result.error).toBe('Message cannot be empty');
    });

    test('should reject messages that are too long', () => {
      const longMessage = 'x'.repeat(10001);
      const result = validator.validate(longMessage);
      expect(result.isValid).toBe(false);
      expect(result.error).toBe('Message too long (max 10000 characters)');
    });

    test('should accept messages at the length limit', () => {
      const maxMessage = 'x'.repeat(10000);
      const result = validator.validate(maxMessage);
      expect(result.isValid).toBe(true);
    });
  });

  describe('JsonMessageSchema', () => {
    beforeEach(() => {
      validator = new MessageValidator(new JsonMessageSchema());
    });

    test('should validate valid JSON objects', () => {
      const result = validator.validate({ name: 'test', value: 123 });
      expect(result.isValid).toBe(true);
      expect(result.error).toBeUndefined();
    });

    test('should validate valid JSON strings', () => {
      const result = validator.validate('{"name":"test","value":123}');
      expect(result.isValid).toBe(true);
      expect(result.error).toBeUndefined();
    });

    test('should reject invalid JSON strings', () => {
      const result = validator.validate('{"name":invalid}');
      expect(result.isValid).toBe(false);
      expect(result.error).toContain('Invalid JSON');
    });

    test('should reject non-JSON-serializable objects', () => {
      const circularObj: any = {};
      circularObj.self = circularObj;
      const result = validator.validate(circularObj);
      expect(result.isValid).toBe(false);
      expect(result.error).toContain('Invalid JSON');
    });

    test('should reject primitive non-object types', () => {
      const result = validator.validate(123);
      expect(result.isValid).toBe(false);
      expect(result.error).toBe('Message must be a valid JSON object or string');
    });

    test('should reject null values', () => {
      const result = validator.validate(null);
      expect(result.isValid).toBe(false);
      expect(result.error).toBe('Message must be a valid JSON object or string');
    });
  });

  describe('Schema switching', () => {
    beforeEach(() => {
      validator = new MessageValidator();
    });

    test('should allow changing schemas', () => {
      let result = validator.validate({ test: 'data' });
      expect(result.isValid).toBe(false);
      expect(result.error).toBe('Message must be a string');

      validator.setSchema(new JsonMessageSchema());
      result = validator.validate({ test: 'data' });
      expect(result.isValid).toBe(true);
    });

    test('should use TextMessageSchema by default', () => {
      const result = validator.validate('test message');
      expect(result.isValid).toBe(true);
    });
  });
});
