type LogLevel = 'INFO' | 'ERROR' | 'DEBUG';

interface LogEntry {
  timestamp: string;
  level: LogLevel;
  message: string;
  meta?: unknown;
}

function createLogEntry(level: LogLevel, message: string, meta?: unknown): string {
  const entry: LogEntry = {
    timestamp: new Date().toISOString(),
    level,
    message
  };

  if (meta !== undefined) {
    entry.meta = meta;
  }

  return JSON.stringify(entry);
}

export const logger = {
  info(message: string, meta?: unknown): void {
    console.log(createLogEntry('INFO', message, meta));
  },

  error(message: string, meta?: unknown): void {
    console.error(createLogEntry('ERROR', message, meta));
  },

  debug(message: string, meta?: unknown): void {
    console.log(createLogEntry('DEBUG', message, meta));
  }
};
