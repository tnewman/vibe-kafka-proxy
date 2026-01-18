# Kafka gRPC Proxy

> [!CAUTION]
> This project is not suitable for production. It was vibe-coded as an experiment to see if Gemini could 
> pull off a technically complex project. Not a single line of code was written by a human outside of 
> this warning. The code needs a thorough review and clean up.

A high-performance gRPC proxy for Kafka that provides a simplified, message-oriented interface for producers and consumers. It abstracts Kafka-specific complexities like offset management and rebalancing, offering built-in flow control and dynamic client sharding.

## Purpose

The Kafka Proxy acts as an intermediary between gRPC clients and a Kafka cluster. It is designed to:
- **Simplify Integration:** Provide a standard gRPC interface, removing the need for Kafka-specific client libraries in every service.
- **Abstract Complexity:** Handle Kafka offsets and commits internally. Clients only need to acknowledge (ACK) messages once processed.
- **Provide Flow Control:** Automatically manage backpressure by pausing Kafka consumption or blocking producers when internal buffers are full or clients are lagging.
- **Enable Broker Portability:** Built on a generic abstraction layer (`pkg/broker`), allowing Kafka to be replaced with other messaging systems (e.g., Pulsar, RabbitMQ) without changing the gRPC API.

## Features

- **Simplified gRPC API:** Bidirectional streaming for both Produce and Consume operations.
- **Message-Oriented Consumption:**
    - Proxy manages Kafka consumer groups and offset commits.
    - Clients ACK messages by a unique `message_id`.
    - Proxy ensures "at-least-once" delivery by tracking un-ACKed messages.
- **Dynamic Sharding & Rebalancing:**
    - Uses hash rings to distribute Kafka partitions among multiple connected gRPC clients.
    - Gracefully handles clients joining or leaving the proxy.
- **Intelligent Flow Control:**
    - **Consumer Backpressure:** Pauses specific partitions if the proxy buffer for that partition is full or if a gRPC client has too many un-ACKed messages.
    - **Producer Backpressure:** Blocks gRPC produce streams if the internal Kafka producer buffer is exceeded.
- **Resource Efficiency:** Automatically unsubscribes from Kafka if no gRPC clients are connected to a specific subscription.
- **Observability:** Integrated with `zap` for structured logging.

## Prerequisites

- **Go:** 1.25.5 or later.
- **Kafka:** A running Kafka cluster (or accessible seed brokers).

## Building the Application

To build the Kafka Proxy server:

```bash
go build -o kafka-proxy ./cmd/server
```

## Running the Application

Start the proxy server by pointing it to your Kafka seed brokers:

```bash
./kafka-proxy --kafka-seed-brokers localhost:9092
```

## Configuration

The proxy can be configured via CLI flags, environment variables, or a YAML configuration file.

### CLI Flags

| Flag | Default | Description |
| :--- | :--- | :--- |
| `--grpc-port` | `50051` | Port for the gRPC server to listen on. |
| `--kafka-seed-brokers` | `localhost:9092` | Comma-separated list of Kafka seed brokers. |
| `--kafka-consumer-group`| `proxy-consumer-group`| Default Kafka consumer group name. |
| `--max-pending-produces` | `1000` | Max pending produce requests before blocking. |
| `--max-unacked-client-messages` | `1000` | Max un-ACKed messages per client before pausing consumption. |
| `--kafka-commit-interval` | `5s` | Interval for committing Kafka offsets. |
| `--config` | `""` | Path to a YAML config file. |

### Configuration File

By default, the proxy looks for a file named `.kafka-proxy.yaml` in the user's home directory. You can also specify a file using the `--config` flag.

Example `.kafka-proxy.yaml`:
```yaml
GRPCPort: "50051"
KafkaSeedBrokers:
  - "localhost:9092"
MaxPendingProduces: 1000
MaxUnackedClientMessages: 1000
ConsumerGroup: "my-proxy-group"
KafkaCommitInterval: 5s
```

### Environment Variables

All flags can be set via environment variables (prefixed with `KAFKA_PROXY_` and using underscores instead of dashes):
- `KAFKA_PROXY_GRPC_PORT`
- `KAFKA_PROXY_KAFKA_SEED_BROKERS`
- `KAFKA_PROXY_MAX_PENDING_PRODUCES`

## Development

### Proto Definition

The gRPC service is defined in `proto/message_proxy.proto`. If you modify the proto, you can regenerate the Go code (requires `protoc` and `protoc-gen-go-grpc`):

```bash
protoc --go_out=. --go-grpc_out=. proto/message_proxy.proto
```

### Running Tests

The project includes unit tests and integration tests (using Testcontainers).

```bash
# Run unit tests
go test ./...

# Run integration tests (requires Docker)
cd integration && go test ./...
```

## Project Structure

- `cmd/server/`: Main application entry point.
- `pkg/broker/`: Generic broker interfaces for abstraction.
- `pkg/broker/kafka/`: Kafka-specific implementation using `franz-go`.
- `pkg/proxy/`: Core gRPC server logic and flow control.
- `proto/`: Protocol Buffer definitions.
- `integration/`: Integration tests using Testcontainers.
