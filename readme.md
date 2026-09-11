# Kafka Avro Order System

A simple Kafka producer/consumer application that sends `Order` records using Apache Avro and Confluent Schema Registry.

## Architecture

```text
Java Producer / Consumer (Windows host)
        |
        | Kafka: localhost:9092
        |
        v
+----------------------------- Docker -----------------------------+
|                                                                  |
|   Kafka                                                         |
|   - External listener: localhost:9092                            |
|   - Internal listener: kafka:19092                               |
|          ^                                                       |
|          |                                                       |
|          | kafka:19092                                           |
|          |                                                       |
|   Schema Registry                                                |
|   - Exposed to host at localhost:8081                            |
|                                                                  |
+------------------------------------------------------------------+
```

The Java application runs on the host machine, so it connects to Kafka using:

```properties
bootstrap.servers=localhost:9092
```

Schema Registry runs inside Docker, so it connects to Kafka through the Docker network using:

```text
kafka:19092
```

## Prerequisites

- Java 17
- Maven
- Docker Desktop

Check them with:

```powershell
java -version
mvn -version
docker --version
```

## 1. Create a Docker Network

```powershell
docker network create kafka-net
```

The shared network lets Schema Registry reach Kafka by container hostname:

```text
schema-registry -> kafka:19092
```

## 2. Run Kafka with Docker

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

### Kafka Docker arguments and properties

**`docker run -d`**  
Runs Kafka in detached mode so it continues in the background.

**`--name kafka-assignment`**  
Names the container `kafka-assignment`. This is the name used with commands such as:

```powershell
docker logs kafka-assignment
docker stop kafka-assignment
docker start kafka-assignment
```

**`--hostname kafka`**  
Sets the container hostname to `kafka`. Other containers on the same Docker network can therefore reach it as `kafka`.

**`--network kafka-net`**  
Connects Kafka to the shared Docker network.

**`-p 9092:9092`**  
Maps port `9092` inside the Kafka container to port `9092` on Windows. This lets the Java application use `localhost:9092`.

### `KAFKA_NODE_ID=1`

Sets this Kafka node's unique KRaft node ID to `1`.

### `KAFKA_PROCESS_ROLES=broker,controller`

Runs this single Kafka node in both roles:

- `broker` — stores records and serves producers/consumers
- `controller` — manages cluster metadata in KRaft mode

This is a single-node development setup.

### `KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:29093`

Defines the KRaft controller quorum.

```text
1@kafka:29093
```

means:

```text
node ID = 1
hostname = kafka
controller port = 29093
```

### `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP`

```text
CONTROLLER:PLAINTEXT,
PLAINTEXT:PLAINTEXT,
PLAINTEXT_HOST:PLAINTEXT
```

Maps listener names to network protocols.

| Listener | Purpose | Protocol |
|---|---|---|
| `CONTROLLER` | KRaft controller communication | PLAINTEXT |
| `PLAINTEXT` | Container-to-container Kafka traffic | PLAINTEXT |
| `PLAINTEXT_HOST` | Host-to-Kafka traffic | PLAINTEXT |

This local setup does not use TLS or SASL authentication.

### `KAFKA_LISTENERS`

```text
CONTROLLER://:29093,
PLAINTEXT_HOST://:9092,
PLAINTEXT://:19092
```

Defines where Kafka actually listens.

- `CONTROLLER://:29093` — KRaft controller traffic
- `PLAINTEXT_HOST://:9092` — Java applications running on Windows
- `PLAINTEXT://:19092` — other Docker containers

### `KAFKA_ADVERTISED_LISTENERS`

```text
PLAINTEXT_HOST://localhost:9092,
PLAINTEXT://kafka:19092
```

Defines the addresses Kafka gives back to clients.

This is different from `KAFKA_LISTENERS`:

```text
KAFKA_LISTENERS
    = where Kafka listens

KAFKA_ADVERTISED_LISTENERS
    = what addresses Kafka tells clients to use
```

Windows applications receive:

```text
localhost:9092
```

Docker containers receive:

```text
kafka:19092
```

This separation is necessary because `localhost` inside the Schema Registry container refers to Schema Registry itself, not the Kafka container.

### `KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT`

Selects the `PLAINTEXT` listener for broker-to-broker communication.

Even though this setup only has one broker, Kafka still needs the listener name configured.

### `KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER`

Marks `CONTROLLER` as the listener used for KRaft controller traffic.

### `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1`

Kafka stores consumer group offsets in the internal `__consumer_offsets` topic.

The default replication factor expects multiple brokers, so for this one-broker development setup it is set to `1`.

### `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1`

Sets the transaction-state internal topic replication factor to `1`, because there is only one broker.

### `KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1`

ISR means *in-sync replicas*. With a single broker, the minimum ISR must be `1`.

### `apache/kafka:4.3.1`

Uses Apache Kafka Docker image version `4.3.1`.

## 3. Check Kafka

```powershell
docker ps
```

View logs if necessary:

```powershell
docker logs kafka-assignment
```

## 4. Run Schema Registry with Docker

Kafka and Schema Registry should use the same Docker network.

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

### Schema Registry properties

**`--name schema-registry`**  
Names the container `schema-registry`.

**`--network kafka-net`**  
Puts Schema Registry on the same Docker network as Kafka.

**`-p 8081:8081`**  
Maps the Schema Registry HTTP API to `localhost:8081` on Windows.

### `SCHEMA_REGISTRY_HOST_NAME=schema-registry`

Sets Schema Registry's hostname.

### `SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081`

Makes Schema Registry listen for HTTP requests on port `8081` on all interfaces inside its container.

### `SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=PLAINTEXT://kafka:19092`

Tells Schema Registry how to reach Kafka.

Because both services are Docker containers on `kafka-net`, it uses:

```text
kafka:19092
```

Schema Registry uses the Kafka cluster for coordination and schema storage.

### `confluentinc/cp-schema-registry:8.3.1`

Runs Confluent Schema Registry version `8.3.1`.

## 5. Check Schema Registry

Make sure both containers are running:

```powershell
docker ps
```

View Schema Registry logs:

```powershell
docker logs schema-registry
```

Test its REST API:

```powershell
Invoke-RestMethod http://localhost:8081/subjects
```

An empty result is normal when no schemas have been registered yet.

## 6. Java Configuration

The Java application runs on Windows, so Kafka remains:

```java
props.put("bootstrap.servers", "localhost:9092");
```

Schema Registry is exposed to Windows on port `8081`:

```java
props.put("schema.registry.url", "http://localhost:8081");
```

Producer serializers:

```java
props.put(
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
    StringSerializer.class.getName()
);

props.put(
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
    KafkaAvroSerializer.class.getName()
);
```

Runtime flow:

```text
Order object
    |
    v
KafkaAvroSerializer
    |
    +----> Schema Registry :8081
    |        register/find schema
    |
    v
Avro data + schema ID
    |
    v
Kafka :9092
```

## 7. Compile the Java Project

```powershell
mvn clean compile
```

or:

```powershell
mvn compile
```

## 8. Run the Producer

```powershell
mvn exec:java "-Dexec.mainClass=org.uor.takehome.Producer"
```

## 9. Run the Consumer

Open another terminal in the project directory:

```powershell
mvn exec:java "-Dexec.mainClass=org.uor.takehome.Consumer"
```

## Useful Docker Commands

Check running containers:

```powershell
docker ps
```

Check all containers:

```powershell
docker ps -a
```

Kafka logs:

```powershell
docker logs kafka-assignment
```

Schema Registry logs:

```powershell
docker logs schema-registry
```

Stop:

```powershell
docker stop schema-registry
docker stop kafka-assignment
```

Start again:

```powershell
docker start kafka-assignment
docker start schema-registry
```

Remove containers:

```powershell
docker rm -f schema-registry
docker rm -f kafka-assignment
```

Remove the network after the containers are removed:

```powershell
docker network rm kafka-net
```

## Startup Order

```text
1. Create kafka-net
        |
        v
2. Start Kafka
        |
        v
3. Start Schema Registry
        |
        v
4. Test Schema Registry
        |
        v
5. Compile Java project
        |
        v
6. Start Consumer
        |
        v
7. Start Producer
```

The producer and consumer can be started in either order, but starting the consumer first makes it easy to observe newly produced messages.

## Ports

| Service | Address | Purpose |
|---|---|---|
| Kafka host listener | `localhost:9092` | Java producer/consumer |
| Kafka Docker listener | `kafka:19092` | Container-to-container traffic |
| Kafka controller | `kafka:29093` | KRaft controller traffic |
| Schema Registry | `http://localhost:8081` | Schema registration and lookup |

## Troubleshooting

### Schema Registry cannot connect to Kafka

If Schema Registry logs contain errors involving `localhost:9092`, verify that Kafka advertises both:

```text
PLAINTEXT_HOST://localhost:9092
PLAINTEXT://kafka:19092
```

and Schema Registry uses:

```text
PLAINTEXT://kafka:19092
```

Both containers must be on:

```text
kafka-net
```

### Schema Registry is not reachable

```powershell
Invoke-RestMethod http://localhost:8081/subjects
docker ps -a
docker logs schema-registry
```

### Kafka is not reachable from Java

```powershell
docker ps
docker logs kafka-assignment
```

The Java application should continue using:

```java
props.put("bootstrap.servers", "localhost:9092");
```

## Development Note

This configuration uses `PLAINTEXT` networking and a single Kafka broker. It is intended for local development and coursework, not production.
