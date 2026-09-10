// Consumer: reads Avro-encoded order messages, maintains a real-time running
// average of prices, retries messages that fail for "temporary" reasons, and
// routes permanently-failed messages to a Dead Letter Queue (DLQ) topic.
const { Kafka } = require("kafkajs");
const { SchemaRegistry } = require("@kafkajs/confluent-schema-registry");
const config = require("./config");

const kafka = new Kafka({
  clientId: config.CLIENT_ID,
  brokers: config.KAFKA_BROKERS,
});

const consumer = kafka.consumer({ groupId: config.CONSUMER_GROUP_ID });
const dlqProducer = kafka.producer();
const registry = new SchemaRegistry({ host: config.SCHEMA_REGISTRY_URL });

// --- Error types -----------------------------------------------------------
// TransientError: worth retrying (e.g. a flaky downstream dependency).
// PermanentError: bad/invalid data - retrying will never help, so it goes
// straight to the DLQ.
class TransientError extends Error {}
class PermanentError extends Error {}

// --- Real-time aggregation state -------------------------------------------
const aggregate = {
  count: 0,
  sum: 0,
  average: 0,
};
const perProduct = new Map(); // product -> { count, sum, average }

function updateAggregate(order) {
  aggregate.count += 1;
  aggregate.sum += order.price;
  aggregate.average = aggregate.sum / aggregate.count;

  const stats = perProduct.get(order.product) || { count: 0, sum: 0, average: 0 };
  stats.count += 1;
  stats.sum += order.price;
  stats.average = stats.sum / stats.count;
  perProduct.set(order.product, stats);
}

function printAggregate(order) {
  console.log(
    `[order ${order.orderId}] ${order.product} $${order.price.toFixed(2)} | ` +
      `running average: $${aggregate.average.toFixed(2)} (n=${aggregate.count}) | ` +
      `${order.product} average: $${perProduct.get(order.product).average.toFixed(2)}`
  );
}

// --- Business logic (where "processing" happens) ----------------------------
// Replace this with real work (DB write, downstream API call, etc). Here we
// validate the data and simulate an intermittent downstream failure so the
// retry path can be demonstrated live.
async function processOrder(order) {
  if (!Number.isFinite(order.price) || order.price < 0) {
    throw new PermanentError(`Invalid price for order ${order.orderId}: ${order.price}`);
  }

  // Simulate a transient downstream failure ~25% of the time.
  if (Math.random() < 0.25) {
    throw new TransientError(`Simulated temporary failure processing order ${order.orderId}`);
  }

  updateAggregate(order);
  printAggregate(order);
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// --- Retry wrapper -----------------------------------------------------------
// Retries only TransientError, using exponential backoff. PermanentError and
// an exhausted retry budget both result in the message being sent to the DLQ.
async function processWithRetry(order) {
  let attempt = 0;

  while (attempt <= config.MAX_RETRIES) {
    try {
      await processOrder(order);
      return; // success
    } catch (err) {
      if (err instanceof PermanentError) {
        throw err; // no point retrying bad data
      }

      attempt += 1;
      if (attempt > config.MAX_RETRIES) {
        throw new Error(
          `Exceeded ${config.MAX_RETRIES} retries for order ${order.orderId}: ${err.message}`
        );
      }

      const delay = config.RETRY_BASE_DELAY_MS * 2 ** (attempt - 1);
      console.warn(
        `[retry ${attempt}/${config.MAX_RETRIES}] order ${order.orderId} failed (${err.message}). ` +
          `Retrying in ${delay}ms...`
      );
      await sleep(delay);
    }
  }
}

// --- Dead Letter Queue -------------------------------------------------------
async function sendToDlq({ rawValue, decodedOrder, reason }) {
  const dlqMessage = {
    originalValueBase64: rawValue.toString("base64"),
    decodedOrder: decodedOrder || null,
    errorReason: reason,
    failedAt: new Date().toISOString(),
  };

  await dlqProducer.send({
    topic: config.TOPIC_DLQ,
    messages: [
      {
        key: decodedOrder ? decodedOrder.orderId : undefined,
        value: JSON.stringify(dlqMessage),
      },
    ],
  });

  console.error(`Sent order to DLQ (${config.TOPIC_DLQ}): ${reason}`);
}

async function run() {
  await consumer.connect();
  await dlqProducer.connect();
  await consumer.subscribe({ topic: config.TOPIC_ORDERS, fromBeginning: true });

  console.log(`Consumer listening on "${config.TOPIC_ORDERS}"...\n`);

  await consumer.run({
    eachMessage: async ({ message }) => {
      let decodedOrder;
      try {
        decodedOrder = await registry.decode(message.value);
      } catch (err) {
        // Corrupt / non-Avro payload - cannot be retried, straight to DLQ.
        await sendToDlq({
          rawValue: message.value,
          decodedOrder: null,
          reason: `Avro decode failure: ${err.message}`,
        });
        return;
      }

      try {
        await processWithRetry(decodedOrder);
      } catch (err) {
        await sendToDlq({
          rawValue: message.value,
          decodedOrder,
          reason: err.message,
        });
      }
    },
  });
}

run().catch((err) => {
  console.error("Consumer failed to start:", err);
  process.exit(1);
});

process.on("SIGINT", async () => {
  console.log("\nShutting down consumer...");
  await consumer.disconnect();
  await dlqProducer.disconnect();
  process.exit(0);
});
