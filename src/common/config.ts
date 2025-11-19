export interface KafkaConfig {
  brokers: string[];
  clientId: string;
  groupId: string;
  topic: string;
}

export const kafkaConfig: KafkaConfig = {
  brokers: process.env.KAFKA_BROKERS?.split(',') || ['localhost:9092'],
  clientId: process.env.KAFKA_CLIENT_ID || 'nodejs-kafka-client',
  groupId: process.env.KAFKA_GROUP_ID || 'nodejs-kafka-group',
  topic: process.env.KAFKA_TOPIC || 'test-topic'
};