package broker

import (
	"context"
)

// Header represents a single key-value pair in a message header.
type Header struct {
	Key   string
	Value []byte
}

// Message is a truly broker-agnostic representation of a message.
type Message struct {
	Topic   string
	Key     []byte
	Value   []byte
	Headers []Header

	// AckID is an opaque identifier created by the broker implementation.
	// It contains all the necessary information for that broker to acknowledge the message.
	AckID interface{}

	// ShardID is a string representation of the 'shard' this message came from
	// (e.g., a Kafka partition number or a RabbitMQ queue name).
	// The proxy uses this for consistent routing of messages to consumers.
	ShardID string
}

// ProduceReceipt is the result of a produce operation.
type ProduceReceipt struct {
	// We keep this generic. For Kafka, it could be topic-partition-offset.
	// For other brokers, it might be just an ID.
}

// Producer is the interface for producing messages to a broker.
type Producer interface {
	// ProduceAsync sends a message to the broker asynchronously.
	// The callback is invoked with the receipt or an error when the message is produced.
	ProduceAsync(ctx context.Context, message *Message, callback func(*ProduceReceipt, error))
	// Close shuts down the producer.
	Close() error
}

// Consumer is the interface for consuming messages from a broker.
type Consumer interface {
	// PollMessages polls for new messages from the broker.
	PollMessages(ctx context.Context) ([]*Message, error)

	// MarkConsumed marks a message as processed. The broker implementation
	// uses the AckID to track messages for committing offsets.
	MarkConsumed(ctx context.Context, ackID interface{}) error

	Close() error
}

// Broker is the interface for creating producers and consumers.
type Broker interface {
	// NewProducer creates a new producer.
	NewProducer(ctx context.Context) (Producer, error)
	// NewConsumer creates a new consumer for the given topics and subscription.
	NewConsumer(ctx context.Context, topics []string, subscription string) (Consumer, error)
}
