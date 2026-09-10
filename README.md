# Kafka Order Pipeline

This project is a simple Kafka-based order processing system. It uses Avro for message serialization and Confluent Schema Registry to manage the Avro schema.

The system has a producer that sends order messages to Kafka and a consumer that reads and processes those orders. The consumer calculates the average price, handles temporary failures using retries, and sends failed messages to a Dead Letter Queue (DLQ).

The project is developed using Node.js, KafkaJS, and Confluent Schema Registry.

## Features

* Avro serialization is used for the order messages.
* The Avro schema is stored in `schemas/order.avsc`.
* Confluent Schema Registry is used to register and manage the schema.
* The consumer calculates the running average price.
* The average is calculated for all orders and also for each product.
* Temporary failures are handled using retry logic.
* Exponential backoff is used between retries.
* Messages that cannot be processed are sent to the `orders-dlq` topic.
* The DLQ keeps the original message and information about why the message failed.

## Architecture

The basic flow of the system is:

```text
producer.js
     |
     | Avro encoded order
     v
[ orders topic ]
     |
     v
consumer.js
     |
     +----> Running average
     |
     v
[ orders-dlq topic ]
     |
     v
dlqWatcher.js
```

The producer sends orders to the `orders` topic. The consumer reads these orders and processes them. If an order has a permanent error or the maximum number of retries is reached, the message is sent to the `orders-dlq` topic. The DLQ watcher can then be used to view those failed messages.

## Project Structure

```text
├── docker-compose.yml
├── package.json
├── schemas/
│   └── order.avsc
└── src/
    ├── config.js
    ├── producer.js
    ├── consumer.js
    └── dlqWatcher.js
```

### Main files

* `docker-compose.yml` - Starts Kafka, Zookeeper and Schema Registry.
* `package.json` - Contains the project dependencies and npm commands.
* `schemas/order.avsc` - Contains the Avro schema for an order.
* `src/config.js` - Contains Kafka brokers, topic names and retry settings.
* `src/producer.js` - Creates random orders and sends them to Kafka.
* `src/consumer.js` - Reads orders, calculates averages, handles retries and sends failed messages to the DLQ.
* `src/dlqWatcher.js` - Reads the DLQ topic and displays failed messages.

## Prerequisites

Before running the project, install:

* Node.js 18 or newer
* Docker Desktop with Docker Compose

## Setup

### 1. Start Kafka and Schema Registry

Run the following commands from the project folder:

```bash
docker compose up -d
docker compose ps
```

The `docker compose ps` command can be used to check whether the required containers are running.

### 2. Install the Node.js dependencies

```bash
npm install
```

## Running the Project

The project uses three terminals when running the demo.

### Terminal 1 - Start the DLQ watcher

It is better to start this first so that any failed messages can be seen while the system is running.

```bash
npm run dlq:watch
```

### Terminal 2 - Start the consumer

The consumer processes the orders, calculates the running averages, and handles retries and DLQ messages.

```bash
npm run consume
```

### Terminal 3 - Start the producer

The producer creates and sends a new random order approximately every second.

```bash
npm run produce
```

## Expected Output

When the project is running, the following things can be observed:

* The producer displays the orders that it sends.
* The consumer displays the running average after successfully processing an order.
* The consumer displays retry messages when a temporary failure occurs.
* Failed messages are sent to the `orders-dlq` topic.
* The DLQ watcher displays messages that were sent to the DLQ.

For example, the consumer may display retry messages similar to:

```text
[retry 1/3]
[retry 2/3]
```

If all retries fail, the message is sent to the DLQ.

## Checking Kafka Topics

The Kafka topics can also be checked directly using the Kafka command-line tools.

To list the available topics:

```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list
```

To view messages from the DLQ:

```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic orders-dlq --from-beginning
```

## Stopping the Project

To stop the Docker containers, run:

```bash
docker compose down
```

## Design Details

There are two types of failures included in this project.

### 1. Permanent failures

The producer intentionally generates some orders with a negative price. These orders are considered invalid and are not retried.

Approximately 10% of the generated orders have a negative price. This is mainly included to demonstrate how the DLQ works.

### 2. Temporary failures

The consumer also simulates a temporary failure for some valid orders.

The failure rate is approximately 25%. When this happens, the consumer tries to process the order again.

The retry delay uses exponential backoff:

```text
Retry 1 -> 500 ms
Retry 2 -> 1000 ms
Retry 3 -> 2000 ms
```

The maximum number of retries is controlled by `MAX_RETRIES` in:

```text
src/config.js
```

If the message still cannot be processed after the allowed retries, it is sent to the DLQ.

## Dead Letter Queue

The `orders-dlq` topic is used for messages that cannot be processed successfully.

A DLQ message contains:

* The original Avro message as Base64 data
* The decoded order, when it can be decoded
* The reason for the failure
* The time when the message was added to the DLQ

Keeping this information makes it possible to inspect failed messages and, if required, use them later for debugging or replay.

## Summary

The project demonstrates a basic Kafka order processing pipeline with:

* Kafka producer and consumer
* Avro serialization
* Confluent Schema Registry
* Running price aggregation
* Retry handling
* Exponential backoff
* Dead Letter Queue
* Failed message monitoring

The main purpose of the project is to show how these Kafka features can work together in an order-processing system.
