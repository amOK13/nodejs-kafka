import {
  MessageSerializer,
  MessageSerializerFactory,
  StringSerializer,
  JsonSerializer
} from './messageSerializer';

describe('MessageSerializer', () => {
  describe('StringSerializer', () => {
    let serializer: StringSerializer;

    beforeEach(() => {
      serializer = new StringSerializer();
    });

    test('should serialize string values as-is', () => {
      const input = 'Hello World';
      const result = serializer.serialize(input);
      expect(result).toBe('Hello World');
    });

    test('should serialize non-string values to strings', () => {
      expect(serializer.serialize(123)).toBe('123');
      expect(serializer.serialize(true)).toBe('true');
      expect(serializer.serialize(null)).toBe('null');
      expect(serializer.serialize(undefined)).toBe('undefined');
    });

    test('should serialize objects to string representation', () => {
      const obj = { name: 'test' };
      const result = serializer.serialize(obj);
      expect(result).toBe('[object Object]');
    });

    test('should deserialize any string as-is', () => {
      const input = 'test string';
      const result = serializer.deserialize(input);
      expect(result).toBe('test string');
    });
  });

  describe('JsonSerializer', () => {
    let serializer: JsonSerializer;

    beforeEach(() => {
      serializer = new JsonSerializer();
    });

    test('should serialize objects to JSON strings', () => {
      const input = { name: 'test', value: 123 };
      const result = serializer.serialize(input);
      expect(result).toBe('{"name":"test","value":123}');
    });

    test('should serialize arrays to JSON strings', () => {
      const input = [1, 2, 3, 'test'];
      const result = serializer.serialize(input);
      expect(result).toBe('[1,2,3,"test"]');
    });

    test('should return valid JSON strings as-is', () => {
      const input = '{"already":"json"}';
      const result = serializer.serialize(input);
      expect(result).toBe('{"already":"json"}');
    });

    test('should throw error for circular references', () => {
      const circular: any = {};
      circular.self = circular;

      expect(() => serializer.serialize(circular)).toThrow();
    });

    test('should deserialize valid JSON strings to objects', () => {
      const input = '{"name":"test","value":123}';
      const result = serializer.deserialize(input);
      expect(result).toEqual({ name: 'test', value: 123 });
    });

    test('should deserialize JSON arrays', () => {
      const input = '[1,2,3,"test"]';
      const result = serializer.deserialize(input);
      expect(result).toEqual([1, 2, 3, 'test']);
    });

    test('should throw error for invalid JSON', () => {
      expect(() => serializer.deserialize('{invalid:json}')).toThrow('Deserialization failed');
    });

    test('should handle nested objects', () => {
      const input = {
        user: { name: 'John', age: 30 },
        items: ['item1', 'item2'],
        active: true
      };
      const serialized = serializer.serialize(input);
      const deserialized = serializer.deserialize(serialized);
      expect(deserialized).toEqual(input);
    });
  });

  describe('MessageSerializerFactory', () => {
    test('should create StringSerializer for "string" format', () => {
      const serializer = MessageSerializerFactory.create('string');
      expect(serializer).toBeInstanceOf(StringSerializer);
    });

    test('should create JsonSerializer for "json" format', () => {
      const serializer = MessageSerializerFactory.create('json');
      expect(serializer).toBeInstanceOf(JsonSerializer);
    });

    test('should throw error for unsupported format', () => {
      expect(() => MessageSerializerFactory.create('xml' as any)).toThrow(
        'Unsupported serialization format: xml'
      );
    });
  });
});
