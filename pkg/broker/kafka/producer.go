package kafka

import (
	"context"

	"github.com/tnewman/kafka-proxy/pkg/broker"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap" // Assuming zap for logging
)

// Producer implements the broker.Producer interface for Kafka.
type Producer struct {
	client *kgo.Client
	logger *zap.Logger
}

// NewProducer creates a new Kafka producer.
// It takes a slice of seed brokers and a logger.
func NewProducer(ctx context.Context, seedBrokers []string, logger *zap.Logger) (broker.Producer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(seedBrokers...),
		kgo.WithLogger(&kgoLogger{logger}),
	)
	if err != nil {
		return nil, err
	}

	return &Producer{client: client, logger: logger}, nil
}

// ProduceAsync sends a message to Kafka asynchronously.
func (p *Producer) ProduceAsync(ctx context.Context, message *broker.Message, callback func(*broker.ProduceReceipt, error)) {
	record := &kgo.Record{
		Topic: message.Topic,
		Key:   message.Key,
		Value: message.Value,
	}

	// Convert generic broker.Header to kgo.Record.Header
	if len(message.Headers) > 0 {
		record.Headers = make([]kgo.RecordHeader, len(message.Headers))
		for i, h := range message.Headers {
			record.Headers[i] = kgo.RecordHeader{Key: h.Key, Value: h.Value}
		}
	}

	p.client.Produce(ctx, record, func(r *kgo.Record, err error) {
		if callback != nil {
			if err != nil {
				p.logger.Error("Failed to produce message to Kafka", zap.Error(err), zap.String("topic", r.Topic))
				callback(nil, err)
			} else {
				receipt := &broker.ProduceReceipt{
					// For Kafka, a ProduceReceipt can include topic, partition, offset
					// But our generic ProduceReceipt is empty for now.
					// If we need Kafka-specific details, we should expand broker.ProduceReceipt.
				}
				callback(receipt, nil)
			}
		}
	})
}

// Close shuts down the Kafka producer.
func (p *Producer) Close() error {
	p.client.Close()
	return nil
}
