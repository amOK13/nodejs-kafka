import 'dotenv/config';

export interface AppConfig {
  kafkaBrokers: string[];
  kafkaTopic: string;
  kafkaConsumerGroupId: string;
  kafkaClientId: string;
}

function parseKafkaBrokers(brokersEnv?: string): string[] {
  if (!brokersEnv) {
    return [process.env.KAFKA_DEFAULT_BROKER || "localhost:9092"];
  }
  return brokersEnv.split(',').map(broker => broker.trim());
}

export const config: AppConfig = {
  kafkaBrokers: parseKafkaBrokers(process.env.KAFKA_BROKERS),
  kafkaTopic: process.env.KAFKA_TOPIC || "example-topic",
  kafkaConsumerGroupId: process.env.KAFKA_CONSUMER_GROUP_ID || "example-consumer-group",
  kafkaClientId: process.env.KAFKA_CLIENT_ID || "nodejs-kafka-client"
};