import { logger } from './logger';

const mockConsoleLog = jest.fn();
const mockConsoleError = jest.fn();

const originalConsole = {
  log: console.log,
  error: console.error
};

describe('Logger', () => {
  beforeEach(() => {
    mockConsoleLog.mockClear();
    mockConsoleError.mockClear();
    console.log = mockConsoleLog;
    console.error = mockConsoleError;
  });

  afterEach(() => {
    console.log = originalConsole.log;
    console.error = originalConsole.error;
  });

  describe('info', () => {
    test('should log info messages with correct format', () => {
      const message = 'Test info message';
      logger.info(message);

      expect(mockConsoleLog).toHaveBeenCalledTimes(1);

      const logCall = mockConsoleLog.mock.calls[0][0];
      const logData = JSON.parse(logCall);

      expect(logData.level).toBe('INFO');
      expect(logData.message).toBe(message);
      expect(logData.timestamp).toBeDefined();
      expect(new Date(logData.timestamp)).toBeInstanceOf(Date);
    });

    test('should log info messages with metadata', () => {
      const message = 'Test message with meta';
      const meta = { userId: '123', action: 'login' };

      logger.info(message, meta);

      expect(mockConsoleLog).toHaveBeenCalledTimes(1);

      const logCall = mockConsoleLog.mock.calls[0][0];
      const logData = JSON.parse(logCall);

      expect(logData.level).toBe('INFO');
      expect(logData.message).toBe(message);
      expect(logData.meta).toEqual(meta);
      expect(logData.timestamp).toBeDefined();
    });

    test('should handle complex metadata objects', () => {
      const message = 'Complex metadata test';
      const meta = {
        user: { id: 1, name: 'John' },
        settings: { theme: 'dark', notifications: true },
        array: [1, 2, 3],
        nested: { deep: { value: 'test' } }
      };

      logger.info(message, meta);

      const logCall = mockConsoleLog.mock.calls[0][0];
      const logData = JSON.parse(logCall);

      expect(logData.meta).toEqual(meta);
    });
  });

  describe('error', () => {
    test('should log error messages with correct format', () => {
      const message = 'Test error message';
      logger.error(message);

      expect(mockConsoleError).toHaveBeenCalledTimes(1);

      const logCall = mockConsoleError.mock.calls[0][0];
      const logData = JSON.parse(logCall);

      expect(logData.level).toBe('ERROR');
      expect(logData.message).toBe(message);
      expect(logData.timestamp).toBeDefined();
    });

    test('should log error messages with metadata', () => {
      const message = 'Database connection failed';
      const meta = { host: 'localhost', port: 5432, error: 'Connection refused' };

      logger.error(message, meta);

      expect(mockConsoleError).toHaveBeenCalledTimes(1);

      const logCall = mockConsoleError.mock.calls[0][0];
      const logData = JSON.parse(logCall);

      expect(logData.level).toBe('ERROR');
      expect(logData.message).toBe(message);
      expect(logData.meta).toEqual(meta);
    });

    test('should handle Error objects in metadata', () => {
      const message = 'Exception occurred';
      const error = new Error('Something went wrong');
      const meta = { error: error.message, stack: error.stack };

      logger.error(message, meta);

      const logCall = mockConsoleError.mock.calls[0][0];
      const logData = JSON.parse(logCall);

      expect(logData.meta.error).toBe('Something went wrong');
      expect(logData.meta.stack).toBeDefined();
    });
  });

  describe('debug', () => {
    test('should log debug messages with correct format', () => {
      const message = 'Debug information';
      logger.debug(message);

      expect(mockConsoleLog).toHaveBeenCalledTimes(1);

      const logCall = mockConsoleLog.mock.calls[0][0];
      const logData = JSON.parse(logCall);

      expect(logData.level).toBe('DEBUG');
      expect(logData.message).toBe(message);
      expect(logData.timestamp).toBeDefined();
    });

    test('should log debug messages with metadata', () => {
      const message = 'Variable values';
      const meta = { count: 42, items: ['a', 'b', 'c'], flag: true };

      logger.debug(message, meta);

      const logCall = mockConsoleLog.mock.calls[0][0];
      const logData = JSON.parse(logCall);

      expect(logData.level).toBe('DEBUG');
      expect(logData.message).toBe(message);
      expect(logData.meta).toEqual(meta);
    });
  });

  describe('timestamp format', () => {
    test('should use ISO 8601 timestamp format', () => {
      logger.info('Timestamp test');
      const logCall = mockConsoleLog.mock.calls[0][0];
      const logData = JSON.parse(logCall);
      expect(logData.timestamp).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/);
    });

    test('should have recent timestamp', () => {
      const beforeLog = new Date();
      logger.info('Timestamp recency test');
      const afterLog = new Date();

      const logCall = mockConsoleLog.mock.calls[0][0];
      const logData = JSON.parse(logCall);
      const logTimestamp = new Date(logData.timestamp);

      expect(logTimestamp.getTime()).toBeGreaterThanOrEqual(beforeLog.getTime());
      expect(logTimestamp.getTime()).toBeLessThanOrEqual(afterLog.getTime());
    });
  });

  describe('JSON format validation', () => {
    test('should produce valid JSON for all log levels', () => {
      const testMessage = 'JSON validation test';
      const testMeta = { key: 'value', number: 123 };

      logger.info(testMessage, testMeta);
      logger.error(testMessage, testMeta);
      logger.debug(testMessage, testMeta);

      expect(mockConsoleLog).toHaveBeenCalledTimes(2);
      expect(mockConsoleError).toHaveBeenCalledTimes(1);
      expect(() => JSON.parse(mockConsoleLog.mock.calls[0][0])).not.toThrow();
      expect(() => JSON.parse(mockConsoleError.mock.calls[0][0])).not.toThrow();
      expect(() => JSON.parse(mockConsoleLog.mock.calls[1][0])).not.toThrow();
    });

    test('should handle special characters in messages', () => {
      const specialMessage = 'Message with "quotes" and \n newlines \t tabs';
      logger.info(specialMessage);

      const logCall = mockConsoleLog.mock.calls[0][0];
      const logData = JSON.parse(logCall);

      expect(logData.message).toBe(specialMessage);
    });
  });
});
