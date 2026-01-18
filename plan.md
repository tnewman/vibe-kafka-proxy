### Regenerated Implementation Plan

#### Phase 1: Project Setup and Protocol Definition

1.  **Initialize Go Project:** Set up the Go module and create a new directory structure that promotes abstraction:
    *   `/proto`: For the generic gRPC API definition.
    *   `/cmd/server`: Main application entry point.
    *   `/pkg/broker`: Will contain the generic interfaces for interacting with any message broker (`Producer`, `Consumer`, `Message`, etc.).
    *   `/pkg/broker/kafka`: An implementation of the broker interfaces using the `franz-go` library. All Kafka-specific code will be isolated here.
    *   `/pkg/proxy`: The core proxy logic, which will depend only on the generic `/pkg/broker` interfaces.
    *   `/test`: For integration tests.

2.  **Define Generic gRPC API:** Create a `message_proxy.proto` file. Naming will be generic (`topic`, `partition`, `subscription`).
    *   **Service:** `MessageProxy`
    *   **RPCs:**
        *   `Produce(stream ProduceRequest) returns (stream ProduceResponse)`
        *   `Consume(stream ConsumeRequest) returns (stream ConsumeResponse)`
    *   **Messages:** The `SubscribeRequest` message will use `subscription` instead of `consumer_group`.

3.  **Generate Go Code:** Compile the `.proto` file.

#### Phase 2: Broker Abstraction Layer

1.  **Define Interfaces:** In the `/pkg/broker` directory, I will define a set of Go interfaces that abstract the message broker's functionality.
    *   `Producer` Interface: With methods like `ProduceAsync(message Message, callback func(...))`.
    *   `Consumer` Interface: With methods like `PollMessages(ctx)`, `Commit(messages...)`, `PausePartitions(...)`, etc.
    *   `Message` and `MessageReceipt` structs: Generic structures to represent messages and their delivery status, independent of any specific broker library.

2.  **Implement Kafka Broker:** In the `/pkg/broker/kafka` directory, I will implement the broker interfaces. This package will wrap the `franz-go` client and translate its specific types and behaviors to conform to the generic interfaces defined in `/pkg/broker`. This is where the project's only direct dependency on `franz-go` will exist.

#### Phase 3: Producer Implementation (Using Abstractions)

1.  **`ProducerService`:** This component in `/pkg/proxy` will be implemented against the generic `broker.Producer` interface.
2.  **Dependency Injection:** The main server will instantiate the `kafka.Producer` and inject it into the `ProducerService`.
3.  **Core Logic:** The logic for handling gRPC streams, correlating responses via callbacks, and managing flow control (backpressure) will remain as previously planned but will interact with the broker through the abstraction layer.
4.  **Testing:** The Testcontainers integration test will start a Kafka container, initialize the `kafka.Producer`, inject it into the proxy, and verify that messages sent from a gRPC client are correctly produced to the Kafka topic.

#### Phase 4: Consumer Implementation (Using Abstractions)

1.  **`ConsumerService`:** This service in `/pkg/proxy` will manage consumption logic using the generic `broker.Consumer` interface.
2.  **Client & Partition Management:** The hash ring for distributing partitions among connected gRPC clients will remain, as "partition" is a common sharding concept.
3.  **Message Processing Loop:** The service will call the generic `PollMessages` method on the `Consumer` interface. It will manage message ACKs from clients and periodically call the `Commit` method on the interface. All rebalancing and flow control logic will also use the generic interface methods like `PausePartitions`.
4.  **Testing:** Integration tests will be updated to inject the `kafka.Consumer` into the service and validate all scenarios: multi-client consumption, rebalancing on client join/leave, offset commits, and flow control.