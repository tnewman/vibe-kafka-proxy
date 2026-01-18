package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/tnewman/kafka-proxy/pkg/broker"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
)

// AckID represents the Kafka-specific information needed to acknowledge a message.
type AckID struct {
	Topic     string
	Partition int32
	Offset    int64 // Corrected back to int64, as record.Offset is int64
}

// Consumer implements the broker.Consumer interface for Kafka.
type Consumer struct {
	client       *kgo.Client
	logger       *zap.Logger
	topics       []string
	subscription string // Corresponds to Kafka consumer group

	// For ack/commit management
	// A map to hold all messages that have been polled but not yet ACKed.
	// Key: ShardID, Value: map[Offset]Message (where Offset is int64)
	inFlight map[string]map[int64]*broker.Message
	mu       sync.Mutex

	// For storing the highest acknowledged offset per partition, for committing
	acknowledgedOffsets map[string]map[int32]int64 // topic -> partition -> highest_acked_offset (int64)
	commitTimer         *time.Timer
	commitInterval      time.Duration

	// For pausing/resuming partitions
	maxUnackedMessagesPerPartition int
	unackedMessagesCount           map[string]map[int32]int      // topic -> partition -> count
	pausedPartitions               map[string]map[int32]struct{} // topic -> partition -> exists (for tracking paused state)
	cancelFunc                     context.CancelFunc
}

// NewConsumer creates a new Kafka consumer.
func NewConsumer(ctx context.Context, seedBrokers []string, topics []string, subscription string, maxUnackedMessagesPerPartition int, commitInterval time.Duration, logger *zap.Logger) (broker.Consumer, error) {
	// Franz-go client options for consumer
	opts := []kgo.Opt{
		kgo.SeedBrokers(seedBrokers...),
		kgo.ConsumerGroup(subscription),
		kgo.ConsumeTopics(topics...),
		kgo.WithLogger(&kgoLogger{logger}), // Use kgoLogger wrapper
		kgo.DisableAutoCommit(),            // We will manually commit
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	if maxUnackedMessagesPerPartition == 0 {
		maxUnackedMessagesPerPartition = 1000 // Default value
	}

	c := &Consumer{
		client:                         client,
		logger:                         logger,
		topics:                         topics,
		subscription:                   subscription,
		inFlight:                       make(map[string]map[int64]*broker.Message),
		acknowledgedOffsets:            make(map[string]map[int32]int64), // Initialize with int64
		commitInterval:                 commitInterval,
		maxUnackedMessagesPerPartition: maxUnackedMessagesPerPartition,
		unackedMessagesCount:           make(map[string]map[int32]int),
		pausedPartitions:               make(map[string]map[int32]struct{}),
	}

	commitCtx, commitCancel := context.WithCancel(context.Background())
	c.cancelFunc = commitCancel

	// Start a background goroutine for committing offsets
	go c.startCommitLoop(commitCtx)

	return c, nil
}

// PollMessages polls for new messages from Kafka.
func (c *Consumer) PollMessages(ctx context.Context) ([]*broker.Message, error) {
	var messages []*broker.Message // Keep this, it's the one we'll return

	fetches := c.client.PollFetches(ctx)

	var pollErr error
	fetches.EachError(func(topic string, partition int32, err error) {
		c.logger.Error("Error polling Kafka record", zap.Error(err), zap.String("topic", topic), zap.Int32("partition", partition))
		if pollErr == nil {
			pollErr = err
		}
	})
	if pollErr != nil {
		return nil, pollErr
	}

	fetches.EachRecord(func(record *kgo.Record) {
		c.mu.Lock()
		defer c.mu.Unlock()

		shardID := fmt.Sprintf("%s-%d", record.Topic, record.Partition)

		if _, ok := c.unackedMessagesCount[record.Topic]; !ok {
			c.unackedMessagesCount[record.Topic] = make(map[int32]int)
		}
		if _, ok := c.pausedPartitions[record.Topic]; !ok {
			c.pausedPartitions[record.Topic] = make(map[int32]struct{})
		}

		// Always process and add the message. Then check for pausing.
		msg := &broker.Message{
			Topic:   record.Topic,
			Key:     record.Key,
			Value:   record.Value,
			ShardID: shardID,
			AckID: AckID{
				Topic:     record.Topic,
				Partition: record.Partition,
				Offset:    record.Offset,
			},
		}

		if len(record.Headers) > 0 {
			msg.Headers = make([]broker.Header, len(record.Headers))
			for i, h := range record.Headers {
				msg.Headers[i] = broker.Header{Key: h.Key, Value: h.Value}
			}
		}

		if _, ok := c.inFlight[shardID]; !ok {
			c.inFlight[shardID] = make(map[int64]*broker.Message)
		}
		c.inFlight[shardID][record.Offset] = msg

		c.unackedMessagesCount[record.Topic][record.Partition]++

		// If current count exceeds max, pause the partition
		if c.unackedMessagesCount[record.Topic][record.Partition] > c.maxUnackedMessagesPerPartition {
			if _, paused := c.pausedPartitions[record.Topic][record.Partition]; !paused {
				c.client.PauseFetchPartitions(map[string][]int32{record.Topic: {record.Partition}})
				c.pausedPartitions[record.Topic][record.Partition] = struct{}{}
				c.logger.Info("Kafka partition paused due to exceeding unacked message limit", zap.String("topic", record.Topic), zap.Int32("partition", record.Partition), zap.Int("unacked_count", c.unackedMessagesCount[record.Topic][record.Partition]))
			}
		}

		messages = append(messages, msg)
	})

	return messages, pollErr
}

// MarkConsumed marks that a Kafka message has been processed.
func (c *Consumer) MarkConsumed(ctx context.Context, ackID interface{}) error {
	kAckID, ok := ackID.(AckID)
	if !ok {
		return fmt.Errorf("invalid AckID type for Kafka consumer")
	}

	shardID := fmt.Sprintf("%s-%d", kAckID.Topic, kAckID.Partition)

	c.mu.Lock()
	defer c.mu.Unlock()

	// Remove from in-flight messages
	if _, ok := c.inFlight[shardID]; !ok {
		return fmt.Errorf("shard %s not found in in-flight messages", shardID)
	}
	if _, ok := c.inFlight[shardID][kAckID.Offset]; !ok { // Use kAckID.Offset (int64)
		return fmt.Errorf("message with offset %d not found in in-flight messages for shard %s", kAckID.Offset, shardID)
	}
	delete(c.inFlight[shardID], kAckID.Offset) // Use kAckID.Offset (int64)

	// Decrement unackedMessagesCount and check for resume threshold
	if _, ok := c.unackedMessagesCount[kAckID.Topic]; ok {
		if count, ok := c.unackedMessagesCount[kAckID.Topic][kAckID.Partition]; ok && count > 0 {
			c.unackedMessagesCount[kAckID.Topic][kAckID.Partition]--

			// Resume if count drops below threshold and partition was paused
			if c.unackedMessagesCount[kAckID.Topic][kAckID.Partition] < c.maxUnackedMessagesPerPartition { // Resume if count is less than max
				if _, paused := c.pausedPartitions[kAckID.Topic][kAckID.Partition]; paused {
					// Resume partition
					c.client.ResumeFetchPartitions(map[string][]int32{kAckID.Topic: {kAckID.Partition}})
					delete(c.pausedPartitions[kAckID.Topic], kAckID.Partition)
					c.logger.Info("Kafka partition resumed due to unacked message count falling below limit", zap.String("topic", kAckID.Topic), zap.Int32("partition", kAckID.Partition), zap.Int("unacked_count", c.unackedMessagesCount[kAckID.Topic][kAckID.Partition]))
				}
			}
		}
	}

	// Update highest acknowledged offset for this partition
	if _, ok := c.acknowledgedOffsets[kAckID.Topic]; !ok {
		c.acknowledgedOffsets[kAckID.Topic] = make(map[int32]int64) // Initialize with int64
	}
	// Compare and assign int64 directly
	if kAckID.Offset > c.acknowledgedOffsets[kAckID.Topic][kAckID.Partition] {
		c.acknowledgedOffsets[kAckID.Topic][kAckID.Partition] = kAckID.Offset
	}

	// The commit logic will check acknowledgedOffsets periodically.
	// No need to reset timer here, as the timer loop handles it.

	return nil
}

// startCommitLoop runs in a background goroutine to periodically commit offsets.
func (c *Consumer) startCommitLoop(ctx context.Context) {
	c.commitTimer = time.NewTimer(c.commitInterval)
	defer c.commitTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Kafka commit loop shutting down due to context cancellation")
			return
		case <-c.commitTimer.C:
			c.doCommit(ctx)
			c.commitTimer.Reset(c.commitInterval) // Reset timer for next interval
		}
	}
}

// doCommit performs the actual Kafka offset commit.
func (c *Consumer) doCommit(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// This map stores int64 offsets directly, as p.Offset in kmsg expects int64
	offsetsToCommit := make(map[string]map[int32]int64)

	for topic, partitions := range c.acknowledgedOffsets {
		if _, ok := offsetsToCommit[topic]; !ok {
			offsetsToCommit[topic] = make(map[int32]int64)
		}
		for partition, acknowledgedOffset := range partitions {
			// acknowledgedOffset is int64.
			offsetsToCommit[topic][partition] = acknowledgedOffset + 1
		}
	}

	if len(offsetsToCommit) == 0 {
		return // Nothing to commit
	}

	req := kmsg.NewOffsetCommitRequest()
	for topic, partitions := range offsetsToCommit { // partitions now holds int64
		t := kmsg.NewOffsetCommitRequestTopic()
		t.Topic = topic
		for partition, offset := range partitions { // 'offset' here is int64
			p := kmsg.NewOffsetCommitRequestTopicPartition()
			p.Partition = partition
			p.Offset = offset // Directly assign int64
			p.Metadata = nil
			t.Partitions = append(t.Partitions, p)
		}
		req.Topics = append(req.Topics, t)
	}

	const maxCommitRetries = 3
	var retryDelay = 100 * time.Millisecond

	for i := 0; i < maxCommitRetries; i++ {
		c.mu.Lock()
		offsetsToCommit := make(map[string]map[int32]int64)
		for topic, partitions := range c.acknowledgedOffsets {
			offsetsToCommit[topic] = make(map[int32]int64)
			for partition, acknowledgedOffset := range partitions {
				offsetsToCommit[topic][partition] = acknowledgedOffset + 1
			}
		}

		if len(offsetsToCommit) == 0 {
			c.mu.Unlock()
			return // Nothing to commit
		}

		req := kmsg.NewOffsetCommitRequest()
		for topic, partitions := range offsetsToCommit {
			t := kmsg.NewOffsetCommitRequestTopic()
			t.Topic = topic
			for partition, offset := range partitions {
				p := kmsg.NewOffsetCommitRequestTopicPartition()
				p.Partition = partition
				p.Offset = offset
				p.Metadata = nil
				t.Partitions = append(t.Partitions, p)
			}
			req.Topics = append(req.Topics, t)
		}
		c.mu.Unlock()

		// Send the request and wait for the response
		future := c.client.RequestFuture(ctx, &req)
		resp, err := future.Block() // Blocks until response is received or context is done
		if err != nil {
			c.logger.Error("Failed to send Kafka offset commit request", zap.Error(err), zap.Any("offsets", offsetsToCommit))
			if i < maxCommitRetries-1 {
				time.Sleep(retryDelay)
				retryDelay *= 2 // Exponential backoff
				continue
			}
			return // All retries failed
		}

		commitResp, ok := resp.(*kmsg.OffsetCommitResponse)
		if !ok {
			c.logger.Error("Received unexpected response type for OffsetCommitRequest", zap.Any("response", resp))
			return // Should not happen
		}

		allCommitted := true
		c.mu.Lock()
		for _, rTopic := range commitResp.Topics {
			for _, rPartition := range rTopic.Partitions {
				if rPartition.ErrorCode != kmsg.None {
					allCommitted = false
					c.logger.Error("Failed to commit Kafka offsets for partition",
						zap.String("topic", rTopic.Topic),
						zap.Int32("partition", rPartition.Partition),
						zap.Error(fmt.Errorf("kafka error code: %d", rPartition.ErrorCode)),
					)
					// Do not clear acknowledgedOffsets for this failed partition
				} else {
					// Successfully committed for this partition, clear from acknowledgedOffsets
					if c.acknowledgedOffsets[rTopic.Topic] != nil {
						delete(c.acknowledgedOffsets[rTopic.Topic], rPartition.Partition)
						if len(c.acknowledgedOffsets[rTopic.Topic]) == 0 {
							delete(c.acknowledgedOffsets, rTopic.Topic)
						}
					}
				}
			}
		}
		c.mu.Unlock()

		if allCommitted {
			c.logger.Info("Successfully committed Kafka offsets", zap.Any("offsets", offsetsToCommit))
			return // All committed, exit retry loop
		}
		c.logger.Warn("Partial Kafka offset commit failure, retrying...", zap.Int("attempt", i+1), zap.Any("offsets", offsetsToCommit))
		time.Sleep(retryDelay)
		retryDelay *= 2 // Exponential backoff
	}
	c.logger.Error("Failed to commit Kafka offsets after multiple retries", zap.Any("offsets", offsetsToCommit))
}

// Close shuts down the Kafka consumer.
func (c *Consumer) Close() error {
	if c.cancelFunc != nil {
		c.cancelFunc() // Signal the commit loop to stop
	}
	c.client.Close()
	return nil
}
