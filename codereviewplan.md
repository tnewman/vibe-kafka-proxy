### **Consolidated Code Review Plan**

**Overall Strengths of the Project:**
*   **Strong Abstraction:** Excellent use of the `broker` interface for Kafka independence (`pkg/broker/broker.go`).
*   **Idiomatic Go:** Good use of concurrency primitives (`channels`, `sync.Map`, `atomic.Int64`, `context.Context`) across `pkg/proxy/server.go` and `pkg/broker/kafka/consumer.go`.
*   **Robust Logging:** Integration with `go.uber.org/zap` for structured logging across the project.
*   **Comprehensive Integration Tests:** Effective use of `testcontainers-go` for realistic testing of Kafka and gRPC interactions (`integration/proxy/consumer_test.go`, `integration/proxy/server_test.go`).
*   **Effective Backpressure:** The `unackedSlots` mechanism for consume backpressure and `produceResponseChan` for produce backpressure are well-implemented in `pkg/proxy/server.go`.

**Critical Bugs / Major Missing Features:**
1.  **CRITICAL BUG: `main.go` fails to compile.** The call to `proxy.NewMessageProxyServer` in `cmd/server/main.go` is missing the `maxUnackedClientMessages` argument required by the function signature in `pkg/proxy/server.go`. This must be fixed for the server application to compile and run.
2.  **MAJOR MISSING FEATURE: Consume 4 (Multiple Clients / Sharding):** The current `MessageProxyService` (`pkg/proxy/server.go`) does not implement hash rings or rebalancing logic for sharding messages to multiple clients, which is a core requirement for consumer scalability.
3.  **MAJOR MISSING FEATURE: Consume 6 (Client Inactivity Disconnect):** There is no explicit mechanism to disconnect inactive `Consume` clients if they don't consume messages within a configurable duration.
4.  **MAJOR MISSING FEATURE: Consume 8 (Dynamic Consumer Sub/Unsub):** The current design with a single shared `kafkaConsumer` is not set up for dynamic Kafka topic subscription/unsubscription based on the number and activity of active gRPC clients, as required.

---

### **Detailed Action Plan (Ordered by Priority):**

**Phase 1: Critical Fixes & Basic Hardcoding (Immediate Priority)**

*   **1.1 Fix `main.go` Compilation Error:**
    *   **Description:** The `cmd/server/main.go` file attempts to call `proxy.NewMessageProxyServer` with an incorrect number of arguments.
    *   **Action:** In `cmd/server/main.go`, modify the `NewMessageProxyServer` call to include the `maxUnackedClientMessages` argument. For example:
        ```go
        const (
        	// ...
        	maxUnackedClientMessages = 1000 // Add this constant
        )
        // ...
        messageProxyServer := proxy.NewMessageProxyServer(kafkaProducer, kafkaConsumer, logger, maxPendingProduces, maxUnackedClientMessages)
        ```
    *   **Status: Completed (Bug already resolved in current codebase)**
*   **1.2 Remove Unused `StringPtr` Function:**
    *   **Description:** The `StringPtr` helper function in `pkg/broker/kafka/consumer.go` is declared but not used.
    *   **Action:** Remove the `StringPtr` function from `pkg/broker/kafka/consumer.go`.
    *   **Status: Completed (Function not found, implying it was already removed)**

**Phase 2: Configuration & Robustness (High Priority)**

*   **2.1 Centralize Configuration:**
    *   **Description:** Several critical parameters are currently hardcoded, preventing flexible deployment and adherence to "configurable" requirements.
    *   **Action:** Implement a robust configuration mechanism (e.g., using environment variables, command-line flags, or a configuration file) to make the following configurable:
        *   Kafka `seedBrokers` (`cmd/server/main.go`)
        *   gRPC `grpcPort` (`cmd/server/main.go`)
        *   `maxPendingProduces` (`cmd/server/main.go`, used in `proxy.NewMessageProxyServer`)
        *   `maxUnackedClientMessages` (`cmd/server/main.go`, used in `proxy.NewMessageProxyServer`)
        *   Kafka `consumerGroup` name (`cmd/server/main.go`, used in `kafka.NewConsumer`)
        *   Kafka `commitInterval` (`pkg/broker/kafka/consumer.go`)
    *   **Status: Completed**
*   **2.2 Implement Graceful Shutdown Timeout:**
    *   **Description:** The `grpcServer.GracefulStop()` call blocks indefinitely, potentially delaying server shutdown if clients do not disconnect.
    *   **Action:** In `cmd/server/main.go`, introduce a `context.WithTimeout` for the `grpcServer.GracefulStop()` call to ensure a bounded shutdown period.
    *   **Status: Completed**
*   **2.3 Improve `doCommit` Error Handling and Retry Logic:**
    *   **Description:** The `doCommit` function in `pkg/broker/kafka/consumer.go` currently clears `acknowledgedOffsets` regardless of whether the offset commit to Kafka was successful. This could lead to message re-delivery if a commit fails. (Addresses `TODO` in `pkg/broker/kafka/consumer.go`).
    *   **Action:** Implement robust error handling and retry logic for Kafka offset commit failures. Only clear `acknowledgedOffsets` if the commit is successful.
    *   **Status: Completed**
*   **2.4 Enhance `Consume 9` Error Feedback and Retry Logic:**
    *   **Description:** The server currently logs errors from `PollMessages` and `MarkConsumed` (in `pkg/proxy/server.go`) and cancels the client's context. However, it doesn't explicitly return detailed error messages to the client or implement the "re-subscribe in a loop" / "retry until successful" logic.
    *   **Action:**
        *   Modify `proxy.MessageProxyService.Consume` to send more specific gRPC error statuses back to the client when Kafka-related errors occur, detailing the nature of the failure.
        *   Implement the specified retry logic for consume errors (e.g., re-subscribing in a loop for transient issues) and commit errors (retrying until successful).
    *   **Status: Completed**

**Phase 3: Test Improvements (High Priority for Maintainability)**

*   **3.1 Consolidate Test Setup Logic:**
    *   **Description:** There is significant boilerplate code for setting up Kafka containers, gRPC servers/clients, and topics in `integration/proxy/consumer_test.go` and `integration/proxy/server_test.go`.
    *   **Action:** Create common helper functions or a dedicated `test_utils.go` file within the `integration/proxy` package to centralize and reuse this setup logic, improving readability and maintainability of the tests.
*   **3.2 Improve Test Waiting Strategies:**
    *   **Description:** Several `time.Sleep` calls are in integration tests, which can lead to flaky tests or unnecessary delays.
    *   **Action:** Replace `time.Sleep` with more robust polling-with-timeout mechanisms (e.g., `testify/require.Eventually` or custom polling loops) for scenarios like waiting for topic creation or message availability in verification consumers.

**Phase 4: Core Feature Implementation (Major Effort)**

*   **4.1 Implement Multi-Client Sharding (Consume 4):**
    *   **Description:** This is a major missing feature for supporting multiple consumers with balanced load and in-order processing guarantees.
    *   **Action:** Design and implement the hash ring and partition redistribution logic to shard messages among multiple connected `Consume` gRPC clients, handling client joins and leaves gracefully while preserving in-flight messages.
*   **4.2 Implement Client Inactivity Disconnect (Consume 6):**
    *   **Description:** The server currently has no mechanism to detect and disconnect inactive clients.
    *   **Action:** Implement a per-client goroutine or a centralized monitoring mechanism that tracks client activity (e.g., `ConsumeRequest`s or ACKs received). If a client is inactive for a configurable duration (e.g., 60 seconds), disconnect it and re-distribute its in-flight messages.
*   **4.3 Implement Dynamic Consumer Sub/Unsub (Consume 8):**
    *   **Description:** The current single `kafkaConsumer` is not designed to dynamically manage subscriptions based on active gRPC clients.
    *   **Action:** Refactor the `kafka.Consumer` lifecycle and `MessageProxyService` to allow dynamic Kafka topic subscription and unsubscription. This may involve:
        *   A pool of `kafka.Consumer` instances.
        *   Dynamic registration/deregistration of topics with a single `kafka.Consumer` that manages multiple internal `kgo.Client` instances.
        *   Handling the Kafka consumer group rebalance process in response to gRPC client changes.
*   **4.4 Address `Consume` TODO for Subscriptions:**
    *   **Description:** The `TODO` in `pkg/proxy/server.go` indicates that client-initiated subscription requests are not yet handled.
    *   **Action:** Implement the logic within `proxy.MessageProxyService.Consume` to process `proxy_pkg.ConsumeRequest_Subscribe` messages, allowing clients to specify their desired topics and subscription groups dynamically. This ties directly into Phase 4.3.
