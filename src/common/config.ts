import 'dotenv/config';

export interface KafkaConfig {
  brokers: string[];
  clientId: string;
  groupId: string;
  topic: string;
}

export interface ServerConfig {
  port: number;
  nodeEnv: string;
}

export interface LogConfig {
  level: string;
}

// Kafka Configuration
export const kafkaConfig: KafkaConfig = {
  brokers: process.env.KAFKA_BROKERS?.split(',') || ['localhost:9092'],
  clientId: process.env.KAFKA_CLIENT_ID || 'nodejs-kafka-client',
  groupId: process.env.KAFKA_GROUP_ID || 'nodejs-kafka-group',
  topic: process.env.KAFKA_TOPIC || 'test-topic'
};

// Server Configuration
export const serverConfig: ServerConfig = {
  port: parseInt(process.env.PORT || '3000', 10),
  nodeEnv: process.env.NODE_ENV || 'development'
};

// Logging Configuration
export const logConfig: LogConfig = {
  level: process.env.LOG_LEVEL || 'info'
};