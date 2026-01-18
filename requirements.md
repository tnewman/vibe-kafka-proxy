Your job is to build a GRPC Kafka proxy.

Two major use cases are supported:
1. Clients will be able to Consume messages
2. Clients will be able to Produce messages

# Requirements
## Core Requirements
1. Go Lang programming language will be used.
2. The Franz Go Kafka library will be used.
3. GRPC will be used for the client/server protocol.
4. Working unit tests shall be provided.
5. Working integration tests leveraging Test Containers shall be provided.
6. All naming shall be generic. For example, use the word subscription 
   instead of consumer group.
7. Interfaces should be used such that Kafka can be easily replaced by a 
   separate event streaming cluster, such as Pulsar or RabbitMQ.
8. Commit is a Kafka-specific concept, so it should be internalized to 
   the Kafka implementation.

## Consume Messages
Clients will be able to consume messages that are proxied from the Kafka 
cluster.

### Message Oriented Mode
1. The concept of Kafka offsets/commits will be abstracted from the client. 
   When a client receives a message, it is responsible for sending an ACK 
   message to the server with the message id.
2. When a client has accumulated more than 1,000 un-ACKed offsets
   (configurable), the client should be paused.
3. The server will commit every 5 seconds or 1,000 messages (configurable). 
   The server will only commit offsets up to the point where all message 
   ids have been ACKed by the client.
4. Multiple clients must be supported. Messages will be sharded to clients 
   based on partition to ensure in-order processing. Hash rings shall be 
   used to gracefully handle clients leaving and joining.
5. When a new client joins, the partitions should be redistributed between 
   clients to balance load as fairly as possible; however, messages that 
   are in-flight (un-ACKed), need to remain with the existing client, even 
   if the partition was re-assigned.
6. If a client does not consume messages within 60 seconds (configurable), 
   it shall be disconnected after a terminal error message is sent 
   indicating the disconnection. Any in-flight messages shall be distributed 
   to the owner of the partition after redistribution.
7. The Kafka consumer shall be paused for any partition that has more than 
   1,000 undelivered or un-committed messages.
8. The Kafka consumer shall unsubscribe if there are no GRPC consumers 
   connected and clear all buffers. It shall not re-subscribe until at 
   least one active GRPC consumer is connected.
9. The server shall return an error to all clients if it is not able to 
   successfully poll or commit. In the case of consume errors, it will 
   attempt to re-subscribe in a loop. Errors will be returned to clients 
   until subscription is successful again. In the case of commit errors, 
   it will retry until successful. This will be abstracted from the client.

## Produce Messages
Clients will be able to produce messages that are proxied to the Kafka 
cluster.

### Message Oriented Mode
1. Clients will be able to send messages to the server.
2. The server shall not block until it has accumulated more than 1,000 
   messages that have not been successfully produced to the Kafka cluster.
3. The server shall send a response to the client for each message 
   indicating success or failure.
