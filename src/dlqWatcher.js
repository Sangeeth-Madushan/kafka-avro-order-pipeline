// Small utility consumer for demo purposes: prints every message that lands
// on the DLQ topic so failures are visible live during a demonstration.
const { Kafka } = require("kafkajs");
const config = require("./config");

const kafka = new Kafka({
  clientId: `${config.CLIENT_ID}-dlq-watcher`,
  brokers: config.KAFKA_BROKERS,
});

const consumer = kafka.consumer({ groupId: "dlq-watcher-group" });

async function run() {
  await consumer.connect();
  await consumer.subscribe({ topic: config.TOPIC_DLQ, fromBeginning: true });

  console.log(`Watching DLQ topic "${config.TOPIC_DLQ}"...\n`);

  await consumer.run({
    eachMessage: async ({ message }) => {
      const dlqMessage = JSON.parse(message.value.toString());
      console.log("--- DLQ message ---");
      console.log(JSON.stringify(dlqMessage, null, 2));
      console.log("-------------------\n");
    },
  });
}

run().catch((err) => {
  console.error("DLQ watcher failed to start:", err);
  process.exit(1);
});

process.on("SIGINT", async () => {
  await consumer.disconnect();
  process.exit(0);
});
