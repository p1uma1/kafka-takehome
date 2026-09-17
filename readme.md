# Kafka Avro Order System

A simple Kafka producer/consumer application that sends `Order` records using Apache Avro and Confluent Schema Registry.

## Architecture

```text
Producer
   ↓
Kafka topic: orders
   ↓
Consumer
   ↓
Save order to PostgreSQL
   ↓
Calculate running average

Database failure
   ↓
Retry 3 times
   ↓
orders-dlq
```

The Java producer and consumer run on the host machine and connect to Kafka using `localhost:9092`. Schema Registry is available at `localhost:8081`.

## Requirements

* Java 17
* Maven
* Docker Desktop

Check:

```powershell
java -version
mvn -version
docker --version
```

## 1. Create Docker Network

Run once:

```powershell
docker network create kafka-net
```

Kafka and Schema Registry use this shared Docker network.

## 2. Create Kafka Container

Run once:

```powershell
docker run -d `
  --name kafka-assignment `
  --hostname kafka `
  --network kafka-net `
  -p 9092:9092 `
  -e KAFKA_NODE_ID=1 `
  -e KAFKA_PROCESS_ROLES=broker,controller `
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:29093 `
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT `
  -e KAFKA_LISTENERS=CONTROLLER://:29093,PLAINTEXT_HOST://:9092,PLAINTEXT://:19092 `
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT_HOST://localhost:9092,PLAINTEXT://kafka:19092 `
  -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT `
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER `
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 `
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 `
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 `
  apache/kafka:4.3.1
```

Kafka is exposed to the Java application at:

```text
localhost:9092
```

Other Docker containers can reach Kafka at:

```text
kafka:19092
```

## 3. Create Schema Registry Container

Run once:

```powershell
docker run -d `
  --name schema-registry `
  --network kafka-net `
  -p 8081:8081 `
  -e SCHEMA_REGISTRY_HOST_NAME=schema-registry `
  -e SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081 `
  -e SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=PLAINTEXT://kafka:19092 `
  confluentinc/cp-schema-registry:8.3.1
```

Schema Registry is available at:

```text
http://localhost:8081
```

Test it:

```powershell
Invoke-RestMethod http://localhost:8081/subjects
```

An empty result is normal before any Avro schemas are registered.

## 4. Create PostgreSQL Container

Run once:

```powershell
docker run `
  --name kafka-db `
  -e POSTGRES_PASSWORD=mysecretpassword `
  -p 5432:5432 `
  -d postgres
```

Create the database:

```sql
CREATE DATABASE orderdb;
```

Connect to it:

```sql
\c orderdb
```

Create the orders table:

```sql
CREATE TABLE orders (
    order_id VARCHAR(20) PRIMARY KEY,
    product VARCHAR(100) NOT NULL,
    price REAL NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## 5. Start Existing Containers

After the containers have been created once, you only need:

```powershell
docker start kafka-assignment
docker start schema-registry
docker start kafka-db
```

Check that they are running:

```powershell
docker ps
```

## 6. Java Connection Configuration

Kafka:

```java
props.put("bootstrap.servers", "localhost:9092");
```

Schema Registry:

```java
props.put(
    "schema.registry.url",
    "http://localhost:8081"
);
```

PostgreSQL:

```java
jdbc:postgresql://localhost:5432/orderdb
```

## 7. Compile

```powershell
mvn clean compile
```

## 8. Run Consumer

Open a terminal in the project directory:

```powershell
mvn exec:java "-Dexec.mainClass=org.uor.takehome.Consumer"
```

## 9. Run Producer

Open another terminal:

```powershell
mvn exec:java "-Dexec.mainClass=org.uor.takehome.Producer"
```

Select a product from the menu. The producer sends an Avro order to the `orders` topic.

The consumer:

```text
receives order
    ↓
saves to PostgreSQL
    ↓
updates running average
```

If the database operation fails:

```text
Attempt 1
   ↓
Attempt 2
   ↓
Attempt 3
   ↓
orders-dlq
```
## Ports

| Service         | Address                 | Purpose                        |
| --------------- | ----------------------- | ------------------------------ |
| Kafka           | `localhost:9092`        | Java producer/consumer         |
| Kafka internal  | `kafka:19092`           | Docker container communication |
| Schema Registry | `http://localhost:8081` | Avro schemas                   |
| PostgreSQL      | `localhost:5432`        | Order database                 |
