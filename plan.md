# Implementation Plan - SOLID & Clean Code Refactor

## Phase 1: Shared Utilities & Sharding
*   **Implement `pkg/sharding/hashring.go`**: Create a consistent hashing ring to map `ShardID` (partitions) to `ClientID`s. This will be used to distribute load fairly among connected gRPC clients.
*   **Implement `pkg/proxy/flowcontrol`**: Create a `FlowController` component to encapsulate backpressure logic (max pending produces/unacked messages). This replaces raw channels/atomics in the main service with a clean interface.

## Phase 2: Broker Layer Decomposition (Internalizing Logic)
*   **Implement `pkg/broker/kafka/committer.go`**: Dedicated component for the manual offset commit loop. Must implement **indefinite retry logic** for commit errors as per requirements.
*   **Implement `pkg/broker/kafka/partition_manager.go`**: Encapsulate logic for tracking partition pause/resume states based on un-ACKed message thresholds.
*   **Refactor `pkg/broker/kafka/consumer.go`**: Orchestrator that delegates specialized tasks to the `Committer` and `PartitionManager`.

## Phase 3: Proxy Layer Modularization (Consumer)
*   **Implement `pkg/proxy/subscription_manager.go`**: Manages the lifecycle of a single Kafka subscription (consumer group).
    *   Orchestrates multiple gRPC `ClientSession`s.
    *   Manages the `broker.Consumer` instance.
    *   Handles rebalancing via `pkg/sharding` when clients join or leave.
*   **Implement `pkg/proxy/client_session.go`**: Manages a single gRPC stream's state.
    *   Tracks assigned partitions and un-ACKed messages.
    *   Implements the **60s idle timeout** for client disconnection.
*   **Implement `pkg/proxy/translator.go`**: Pure functions for converting between `broker.Message` and gRPC `ConsumeResponse`.

## Phase 4: Proxy Layer Modularization (Producer)
*   **Implement `pkg/proxy/producer_handler.go`**: Extracts `Produce` stream logic from the main server.
    *   Uses `FlowController` to block `Recv()` when the producer buffer is full.
    *   Handles asynchronous callback correlation.

## Phase 5: Implementation, Integration & Verification
1.  **Iterative Development**: Build and unit test each component (Sharding, Committer, etc.) independently.
2.  **System Integration**: Wire components together in `pkg/proxy/server.go`.
3.  **Comprehensive Testing**:
    *   **Unit Tests**: Sharding redistribution, Committer retries, FlowControl blocking.
    *   **Integration Tests**: Multi-client consumption, rebalancing (preserving in-flight messages), and 60s idle timeouts.
4.  **Standards Compliance**: Execute `go fmt`, `go vet`, and linting to ensure production-grade quality.
