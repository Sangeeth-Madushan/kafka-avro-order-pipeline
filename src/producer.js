// Producer: generates randomized order messages, serializes them with Avro
// (via Confluent Schema Registry), and publishes them to the "orders" topic.
const fs = require("fs");
const path = require("path");
const { Kafka } = require("kafkajs");
const { SchemaRegistry } = require("@kafkajs/confluent-schema-registry");
const config = require("./config");

const kafka = new Kafka({
  clientId: config.CLIENT_ID,
  brokers: config.KAFKA_BROKERS,
});

const producer = kafka.producer();
const registry = new SchemaRegistry({ host: config.SCHEMA_REGISTRY_URL });

const PRODUCTS = ["Item1", "Item2", "Item3", "Item4", "Item5"];

let orderSeq = 1000;

function randomOrder() {
  orderSeq += 1;

  // Occasionally emit an invalid price (~10% of the time) to demonstrate
  // the consumer's "permanent failure" path straight to the DLQ.
  const isBadData = Math.random() < 0.1;
  const price = isBadData
    ? -Math.round(Math.random() * 100) // invalid: negative price
    : Number((Math.random() * 490 + 10).toFixed(2)); // valid: 10.00 - 500.00

  return {
    orderId: String(orderSeq),
    product: PRODUCTS[Math.floor(Math.random() * PRODUCTS.length)],
    price,
  };
}

async function registerSchema() {
  const schemaPath = path.join(__dirname, "..", "schemas", "order.avsc");
  const schemaString = fs.readFileSync(schemaPath, "utf-8");
  const { id } = await registry.register({
    type: "AVRO",
    schema: schemaString,
  });
  console.log(`Registered Avro schema for "orders" topic (schema id: ${id})`);
  return id;
}

async function run() {
  const schemaId = await registerSchema();
  await producer.connect();
  console.log("Producer connected. Publishing order messages every 1s. Ctrl+C to stop.\n");

  setInterval(async () => {
    const order = randomOrder();
    try {
      const encodedValue = await registry.encode(schemaId, order);
      await producer.send({
        topic: config.TOPIC_ORDERS,
        messages: [{ key: order.orderId, value: encodedValue }],
      });
      console.log("Produced:", order);
    } catch (err) {
      console.error("Failed to produce message:", err.message);
    }
  }, 1000);
}

run().catch((err) => {
  console.error("Producer failed to start:", err);
  process.exit(1);
});

process.on("SIGINT", async () => {
  console.log("\nShutting down producer...");
  await producer.disconnect();
  process.exit(0);
});
