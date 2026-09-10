// Shared configuration for the producer, consumer, and DLQ watcher.
module.exports = {
  KAFKA_BROKERS: ["localhost:9092"],
  SCHEMA_REGISTRY_URL: "http://localhost:8081",

  TOPIC_ORDERS: "orders",
  TOPIC_DLQ: "orders-dlq",

  CLIENT_ID: "order-pipeline",
  CONSUMER_GROUP_ID: "order-consumer-group",

  // Retry logic (temporary-failure handling) before a message is routed to the DLQ.
  MAX_RETRIES: 3,
  RETRY_BASE_DELAY_MS: 500, // exponential backoff: 500ms, 1000ms, 2000ms...
};
