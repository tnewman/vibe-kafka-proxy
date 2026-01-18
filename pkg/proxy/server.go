package proxy

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time" // Added import for time

	"github.com/tnewman/kafka-proxy/pkg/broker"
	"github.com/tnewman/kafka-proxy/pkg/broker/kafka" // NEW import for kafka.AckID

	"github.com/google/uuid" // New import for UUID generation
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// messageProxyService implements the MessageProxyServer interface for the gRPC service.
type MessageProxyService struct {
	// embed the unimplemented server to ensure forward compatibility.
	// NOTE: This must be embedded by value, not by pointer.
	UnimplementedMessageProxyServer

	producer broker.Producer
	consumer broker.Consumer // Added consumer
	logger   *zap.Logger

	// For tracking pending produce messages for backpressure
	maxPendingProduces   int64                 // Configurable max pending produces
	pendingProducesCount atomic.Int64          // Atomic counter for pending produces
	produceResponseChan  chan *ProduceResponse // Buffered channel for produce backpressure

	// pendingAcks tracks Kafka AckIDs that have been sent to clients
	// and are awaiting acknowledgment. Key: generated message_id, Value: kafka.AckID
	pendingAcks sync.Map
	ConsumeDone chan struct{}

	maxUnackedClientMessages int64                 // Configurable max unacked messages for client backpressure
	unackedMessagesInFlight  atomic.Int64          // Lock-free counter for delivered but unacked messages
	clientMessageChan        chan *ConsumeResponse // Buffered channel for backpressure
	unackedSlots             chan struct{}         // Semaphore for unacked messages
	pollError                atomic.Value          // Stores the last non-cancellation error from PollMessages
}

// NewMessageProxyServer creates a new gRPC server for the MessageProxy service.
func NewMessageProxyServer(producer broker.Producer, consumer broker.Consumer, logger *zap.Logger, maxPendingProduces int64, maxUnackedClientMessages int64) MessageProxyServer {
	logger.Info("NewMessageProxyServer initialized",
		zap.Int64("maxPendingProduces", maxPendingProduces),
		zap.Int64("maxUnackedClientMessages", maxUnackedClientMessages))
	svc := &MessageProxyService{
		producer:                 producer,
		consumer:                 consumer, // Initialize consumer
		logger:                   logger,
		maxPendingProduces:       maxPendingProduces,
		pendingProducesCount:     atomic.Int64{},                                  // Initialize atomic counter to 0
		produceResponseChan:      make(chan *ProduceResponse, maxPendingProduces), // Initialize buffered channel for produce backpressure
		pendingAcks:              sync.Map{},
		ConsumeDone:              make(chan struct{}),
		maxUnackedClientMessages: maxUnackedClientMessages,
		unackedMessagesInFlight:  atomic.Int64{},                                        // Initialize atomic counter to 0
		clientMessageChan:        make(chan *ConsumeResponse, maxUnackedClientMessages), // Initialize buffered channel for backpressure
		unackedSlots:             make(chan struct{}, maxUnackedClientMessages),         // Initialize semaphore
		pollError:                atomic.Value{},                                        // Initialize to zero value
	}
	// Fill unackedSlots with empty structs initially, representing available slots
	for i := 0; i < int(maxUnackedClientMessages); i++ {
		svc.unackedSlots <- struct{}{}
	}
	svc.logger.Debug("clientMessageChan capacity", zap.Int("capacity", cap(svc.clientMessageChan)))
	return svc
}

// Consume handles the bi-directional streaming RPC for consuming messages.
func (s *MessageProxyService) Consume(stream MessageProxy_ConsumeServer) error {
	defer close(s.ConsumeDone)
	ctx := stream.Context()
	s.logger.Info("New Consume stream opened")

	// Context for this specific client's processing
	clientCtx, clientCancel := context.WithCancel(ctx)
	defer clientCancel() // Ensure this client's context is cancelled when the stream ends

	// Goroutine to send messages to the client
	go func() {
		for {
			select {
			case <-clientCtx.Done():
				s.logger.Info("Consume sender goroutine shutting down due to client context cancellation")
				return
			case msg := <-s.clientMessageChan:
				err := stream.Send(msg)
				if err != nil {
					s.logger.Error("Failed to send consumed message to client", zap.Error(err))
					clientCancel() // Cancel client context if sending fails
					return
				}
			}
		}
	}()

	// Goroutine to poll messages from broker.Consumer and forward to clientMessageChan

	go func() {

		const (
			initialBackoff = 100 * time.Millisecond

			maxBackoff = 5 * time.Second

			maxRetries = 5
		)

		currentBackoff := initialBackoff

		retries := 0

		for {

			select {

			case <-clientCtx.Done():

				s.logger.Info("Broker consumer polling goroutine shutting down due to client context cancellation")

				return

			default: // Use default so PollMessages isn't blocked if unackedSlots is full and there's a problem

				messages, err := s.consumer.PollMessages(clientCtx)

				if err != nil {

					if status.Code(err) == codes.Canceled {

						s.logger.Info("Broker consumer polling canceled", zap.Error(err))

						return // Exit on cancellation

					}

					// Handle transient errors with retry and backoff

					retries++

					s.logger.Error("Error polling messages from broker consumer, retrying...",

						zap.Error(err),

						zap.Int("retries", retries),

						zap.Duration("backoff", currentBackoff))

					if retries >= maxRetries {

						s.logger.Error("Max retries for polling messages exhausted, shutting down consumer stream", zap.Error(err))

						s.pollError.Store(err) // Store the actual error

						clientCancel() // Trigger shutdown

						return

					}

					time.Sleep(currentBackoff) // Backoff before next retry

					currentBackoff *= 2 // Exponential backoff

					if currentBackoff > maxBackoff {

						currentBackoff = maxBackoff

					}

					// No slot acquired in this iteration, so no slot to release

					continue // Continue loop to retry polling

				}

				// Reset backoff on successful poll

				retries = 0

				currentBackoff = initialBackoff

				for _, brokerMsg := range messages {

					// Acquire a slot BEFORE sending the message to clientMessageChan

					select {

					case <-clientCtx.Done(): // Check context before acquiring slot

						s.logger.Info("Broker consumer polling goroutine shutting down while waiting for unacked slot")

						return

					case <-s.unackedSlots: // Acquire a slot - this will block if no slots are available

						// Slot acquired, proceed to process and send message

					}

					// Convert broker.Message to ConsumeResponse

					kafkaAckID, ok := brokerMsg.AckID.(kafka.AckID)

					if !ok {

						s.logger.Error("Failed to type assert AckID to kafka.AckID", zap.Any("ack_id", brokerMsg.AckID))

						// Release the acquired slot if we skip this message due to bad AckID

						select {

						case s.unackedSlots <- struct{}{}:

						case <-clientCtx.Done():

							s.logger.Info("Client context canceled while releasing unacked slot due to bad AckID")

							return

						}

						continue // Skip this message if AckID is not valid

					}

					// Generate a unique message_id for client acknowledgment

					msgID := uuid.New().String()

					// Store kafka.AckID in pendingAcks sync.Map

					s.pendingAcks.Store(msgID, kafkaAckID)

					resp := &ConsumeResponse{

						Topic: kafkaAckID.Topic,

						Partition: kafkaAckID.Partition,

						Offset: kafkaAckID.Offset,

						Key: brokerMsg.Key,

						Value: brokerMsg.Value,

						MessageId: msgID, // Use the generated unique message_id

						Headers: make([]*MessageHeader, len(brokerMsg.Headers)),
					}

					for i, h := range brokerMsg.Headers {

						resp.Headers[i] = &MessageHeader{Key: h.Key, Value: h.Value}

					}

					s.logger.Debug("Attempting to send message to clientMessageChan",

						zap.Int("channel_len", len(s.clientMessageChan)),

						zap.Int("channel_cap", cap(s.clientMessageChan)))

					s.clientMessageChan <- resp // This will block if the channel is full (backpressure)

					s.unackedMessagesInFlight.Add(1) // Increment count after message is successfully sent to channel

					s.logger.Debug("Client unacked message count incremented",

						zap.Int64("current_count", s.unackedMessagesInFlight.Load()))

				}

			}

		}

	}()

	// Main loop to receive client requests (e.g., Acks, subscription changes)
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			s.logger.Info("Consume stream closed by client")
			return nil
		}
		if err != nil {
			if clientCtx.Err() != nil { // If client context is done, check for stored error
				if storedErr := s.pollError.Load(); storedErr != nil {
					s.logger.Error("Returning stored error from PollMessages", zap.Error(storedErr.(error)))
					return status.Errorf(codes.Unavailable, "Kafka consumer error: %v", storedErr.(error))
				}
				s.logger.Info("Client stream canceled due to context cancellation", zap.Error(err))
				return nil // Clean disconnect, no specific pollError
			}
			s.logger.Error("Error receiving consume request", zap.Error(err))
			return status.Errorf(codes.Internal, "Error receiving consume request: %v", err) // Generic internal error
		}

		// TODO: Handle ConsumeRequest: subscriptions. ACKs are handled below.
		s.logger.Debug("Received ConsumeRequest from client", zap.Any("request", req))

		if ack := req.GetAck(); ack != nil {
			// Retrieve kafka.AckID from pendingAcks sync.Map
			value, found := s.pendingAcks.LoadAndDelete(ack.MessageId)
			if !found {
				s.logger.Warn("Received ACK for unknown message ID", zap.String("ack_message_id", ack.MessageId))
				continue // Skip processing this ACK
			}
			kafkaAckID, ok := value.(kafka.AckID)
			if !ok {
				s.logger.Error("Failed to type assert loaded AckID from sync.Map", zap.Any("ack_id", value))
				continue // Skip processing this ACK
			}

			// Call s.consumer.MarkConsumed
			if err := s.consumer.MarkConsumed(clientCtx, kafkaAckID); err != nil {
				s.logger.Error("Failed to mark Kafka message as consumed", zap.Error(err), zap.String("ack_message_id", ack.MessageId))
				// If MarkConsumed fails, return an error to the client. This will terminate the stream.
				// The client is expected to handle this error and potentially re-subscribe.
				s.unackedMessagesInFlight.Add(-1) // Still decrement unacked count to prevent deadlock from full unackedSlots
				select {
				case s.unackedSlots <- struct{}{}: // Release a slot even if MarkConsumed failed
				case <-clientCtx.Done():
					s.logger.Info("Client context canceled while releasing unacked slot after MarkConsumed error")
				}
				return status.Errorf(codes.Internal, "Failed to mark message %s as consumed: %v", ack.MessageId, err)
			}
			s.logger.Debug("Successfully marked Kafka message as consumed", zap.String("ack_message_id", ack.MessageId))
			s.unackedMessagesInFlight.Add(-1) // Decrement unacked message count as message is ACKed
			s.logger.Debug("Client unacked message count decremented",
				zap.Int64("current_count", s.unackedMessagesInFlight.Load()))

			select {
			case s.unackedSlots <- struct{}{}: // Release a slot
			case <-clientCtx.Done(): // Context might be done during shutdown
				s.logger.Info("Client context canceled while releasing unacked slot")
			}
		}
	}
}

// Produce handles the bi-directional streaming RPC for producing messages.
func (s *MessageProxyService) Produce(stream MessageProxy_ProduceServer) error {
	ctx := stream.Context()
	s.logger.Info("New Produce stream opened")

	// Goroutine to send responses back to the client stream safely.
	// This ensures that stream.Send is called from a single goroutine,
	// preventing concurrent write issues.
	go func() {
		for {
			select {
			case <-ctx.Done():
				s.logger.Info("Produce response sender shutting down due to context cancellation")
				close(s.produceResponseChan) // Close the channel when context is done
				return
			case res, ok := <-s.produceResponseChan:
				if !ok { // Channel closed, exit goroutine
					return
				}
				err := stream.Send(res)
				if err != nil {
					s.logger.Error("Failed to send produce response to client", zap.Error(err), zap.String("client_message_id", res.GetClientMessageId()))
					// If sending fails, it's likely the client has disconnected.
					return // Exit sender goroutine on send failure
				}
			}
		}
	}()

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			s.logger.Info("Produce stream closed by client")
			return nil
		}
		if err != nil {
			if status.Code(err) == codes.Canceled {
				s.logger.Info("Client stream canceled during receive", zap.Error(err))
				return nil // Return nil because it's a clean disconnect
			}
			s.logger.Error("Error receiving produce request", zap.Error(err))
			return err
		}

		// Increment pending produces count
		s.pendingProducesCount.Add(1)

		brokerMessage := &broker.Message{
			Topic:   req.Topic,
			Key:     req.Key,
			Value:   req.Value,
			Headers: make([]broker.Header, len(req.Headers)),
		}
		for i, h := range req.Headers {
			brokerMessage.Headers[i] = broker.Header{Key: h.Key, Value: h.Value}
		}

		// Define the callback for asynchronous produce completion
		produceCallback := func(receipt *broker.ProduceReceipt, produceErr error) {
			s.pendingProducesCount.Add(-1) // Decrement pending count
			res := &ProduceResponse{
				ClientMessageId: req.ClientMessageId,
			}
			if produceErr != nil {
				res.Success = false
				res.ErrorMessage = produceErr.Error()
				s.logger.Error("Failed to produce message", zap.Error(produceErr), zap.String("client_message_id", req.ClientMessageId))
			} else {
				res.Success = true
				s.logger.Debug("Message produced successfully", zap.String("client_message_id", req.ClientMessageId))
			}
			// Send response to the sender goroutine.
			select {
			case s.produceResponseChan <- res:
				// Response successfully queued
			case <-ctx.Done():
				s.logger.Warn("Context cancelled while queuing produce callback response", zap.String("client_message_id", req.ClientMessageId))
				// No need to return error here, the main loop or stream sender will handle context cancellation.
			}
		}

		s.producer.ProduceAsync(ctx, brokerMessage, produceCallback)
	}
}
