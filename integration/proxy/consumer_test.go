package proxy

import (
	"context"
	"fmt"
	"net"
	"os" // New import for os.Stderr
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/tnewman/kafka-proxy/pkg/broker/kafka"
	proxy_pkg "github.com/tnewman/kafka-proxy/pkg/proxy"

	tc_kafka "github.com/testcontainers/testcontainers-go/modules/kafka"
)

func TestMessageProxy_Consume(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Setup Kafka container
	kafkaContainer, err := tc_kafka.Run(ctx, "confluentinc/confluent-local:7.5.0",
		tc_kafka.WithClusterID("test-cluster"))
	require.NoError(t, err, "failed to start kafka container")
	defer func() {
		if err := kafkaContainer.Terminate(context.Background()); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}()

	kafkaSeedBrokers, err := kafkaContainer.Brokers(ctx)
	require.NoError(t, err, "failed to get kafka seed brokers")

	// Create Kafka producer to put messages into the topic
	logger, _ := zap.NewDevelopment()
	kafkaProducer, err := kafka.NewProducer(ctx, kafkaSeedBrokers, logger)
	require.NoError(t, err, "failed to create kafka producer")
	defer kafkaProducer.Close()

	// Create Kafka consumer for the proxy service
	consumerTopics := []string{"test-consume-topic"}
	consumerGroup := "test-consumer-group"
	kafkaConsumer, err := kafka.NewConsumer(ctx, kafkaSeedBrokers, consumerTopics, consumerGroup, 1000, 5*time.Second, logger) // 1000 max unacked messages, 5s commit interval
	require.NoError(t, err, "failed to create kafka consumer")
	defer kafkaConsumer.Close()

	// Create Kafka Admin Client
	kgoClientForAdmin, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaSeedBrokers...),
		kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelInfo, nil)), // Add logger here
	)
	require.NoError(t, err, "failed to create kgo client for admin API")
	defer func() {
		if kgoClientForAdmin != nil {
			kgoClientForAdmin.Close()
		}
	}()
	adminClient := kadm.NewClient(kgoClientForAdmin)

	// Create the topic for producing messages to
	topic := "test-consume-topic"
	_, err = adminClient.CreateTopic(ctx, 1, 1, nil, topic) // 1 partition, 1 replication factor
	require.NoError(t, err, fmt.Sprintf("failed to create topic %s", topic))
	time.Sleep(1 * time.Second) // Allow some time for topic creation to propagate

	// --- Setup gRPC Server ---
	// Need to provide the consumer to the server
	proxyServer := proxy_pkg.NewMessageProxyServer(kafkaProducer, kafkaConsumer, logger, 100, 1000)

	const bufSize = 1024 * 1024
	lis := bufconn.Listen(bufSize)
	s := grpc.NewServer()
	proxy_pkg.RegisterMessageProxyServer(s, proxyServer)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			t.Logf("Server exited with error: %v", err)
		}
	}()
	defer func() {
		s.GracefulStop() // Use GracefulStop for clean shutdown
		wg.Wait()        // Wait for the Serve goroutine to finish
	}()

	// --- Setup gRPC Client ---
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err, "failed to dial bufnet")
	defer conn.Close()

	client := proxy_pkg.NewMessageProxyClient(conn)

	// --- Test Logic ---
	// 1. Produce a message to Kafka directly
	produceClient, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaSeedBrokers...),
		kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelInfo, nil)), // Provide os.Stderr
	)
	require.NoError(t, err, "failed to create kgo client for producing")
	defer produceClient.Close()

	record := &kgo.Record{
		Topic: topic,
		Key:   []byte("test-key"),
		Value: []byte("test-value"),
	}
	produceCtx, produceCancel := context.WithTimeout(ctx, 5*time.Second)
	defer produceCancel()
	results := produceClient.ProduceSync(produceCtx, record)
	require.NoError(t, results.FirstErr(), "failed to produce message to Kafka")

	// 2. Start consuming from the proxy
	consumeStream, err := client.Consume(ctx)
	require.NoError(t, err, "failed to open consume stream")

	// Send a Subscribe request
	subscribeReq := &proxy_pkg.ConsumeRequest{
		RequestType: &proxy_pkg.ConsumeRequest_Subscribe{
			Subscribe: &proxy_pkg.SubscribeRequest{
				Subscription: consumerGroup,
				Topics:       consumerTopics,
			},
		},
	}
	err = consumeStream.Send(subscribeReq)
	require.NoError(t, err, "failed to send subscribe request")

	// 3. Receive message from proxy
	receivedMsg, err := consumeStream.Recv()
	require.NoError(t, err, "failed to receive message from proxy")

	assert.Equal(t, topic, receivedMsg.GetTopic(), "received message topic mismatch")
	assert.Equal(t, "test-key", string(receivedMsg.GetKey()), "received message key mismatch")
	assert.Equal(t, "test-value", string(receivedMsg.GetValue()), "received message value mismatch")
	assert.NotEmpty(t, receivedMsg.GetMessageId(), "received message ID should not be empty")

	// 4. Send ACK back to proxy
	ackReq := &proxy_pkg.ConsumeRequest{
		RequestType: &proxy_pkg.ConsumeRequest_Ack{
			Ack: &proxy_pkg.AckMessage{
				MessageId: receivedMsg.GetMessageId(),
			},
		},
	}
	err = consumeStream.Send(ackReq)
	require.NoError(t, err, "failed to send ACK to proxy")

	// Give some time for the ACK to be processed and committed internally
	time.Sleep(2 * time.Second)

	// Optional: Verify offset commit (requires internal access or a new verification mechanism)
	// For now, assume if MarkConsumed doesn't return an error, it's fine.

	// Cleanly close the consume stream
	err = consumeStream.CloseSend()
	require.NoError(t, err, "failed to close consume stream send direction")

	// Ensure the server side also recognizes the stream closure by waiting for the Consume method to return
	select {
	case <-proxyServer.(*proxy_pkg.MessageProxyService).ConsumeDone: // Cast to access the exported field
	case <-time.After(10 * time.Second):
		t.Error("Server did not shut down consume stream cleanly")
	}
}

func TestMessageProxy_ConsumeBackpressure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Setup Kafka container
	kafkaContainer, err := tc_kafka.Run(ctx, "confluentinc/confluent-local:7.5.0",
		tc_kafka.WithClusterID("test-cluster"))
	require.NoError(t, err, "failed to start kafka container")
	defer func() {
		if err := kafkaContainer.Terminate(context.Background()); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}()

	kafkaSeedBrokers, err := kafkaContainer.Brokers(ctx)
	require.NoError(t, err, "failed to get kafka seed brokers")

	// Create Kafka producer to put messages into the topic
	logger, _ := zap.NewDevelopment()
	kafkaProducer, err := kafka.NewProducer(ctx, kafkaSeedBrokers, logger)
	require.NoError(t, err, "failed to create kafka producer")
	defer kafkaProducer.Close()

	// Use a small number for max unacked messages for backpressure testing
	maxUnackedMessages := 5
	consumerTopics := []string{"test-consume-topic-backpressure"}
	consumerGroup := "test-consumer-group-backpressure"
	kafkaConsumer, err := kafka.NewConsumer(ctx, kafkaSeedBrokers, consumerTopics, consumerGroup, maxUnackedMessages, 5*time.Second, logger)
	require.NoError(t, err, "failed to create kafka consumer")
	defer kafkaConsumer.Close()

	// Create Kafka Admin Client
	kgoClientForAdmin, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaSeedBrokers...),
		kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelInfo, nil)),
	)
	require.NoError(t, err, "failed to create kgo client for admin API")
	defer func() {
		if kgoClientForAdmin != nil {
			kgoClientForAdmin.Close()
		}
	}()
	adminClient := kadm.NewClient(kgoClientForAdmin)

	// Create the topic for producing messages to
	topic := "test-consume-topic-backpressure"
	_, err = adminClient.CreateTopic(ctx, 1, 1, nil, topic) // 1 partition, 1 replication factor
	require.NoError(t, err, fmt.Sprintf("failed to create topic %s", topic))
	time.Sleep(1 * time.Second) // Allow some time for topic creation to propagate

	// --- Setup gRPC Server ---
	proxyServer := proxy_pkg.NewMessageProxyServer(kafkaProducer, kafkaConsumer, logger, 100, int64(maxUnackedMessages))

	const bufSize = 1024 * 1024
	lis := bufconn.Listen(bufSize)
	s := grpc.NewServer()
	proxy_pkg.RegisterMessageProxyServer(s, proxyServer)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			t.Logf("Server exited with error: %v", err)
		}
	}()
	defer func() {
		s.GracefulStop()
		wg.Wait()
	}()

	// --- Setup gRPC Client ---
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err, "failed to dial bufnet")
	defer conn.Close()

	client := proxy_pkg.NewMessageProxyClient(conn)

	// --- Test Logic ---
	// 1. Produce more messages than maxUnackedMessages
	produceClient, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaSeedBrokers...),
		kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelInfo, nil)),
	)
	require.NoError(t, err, "failed to create kgo client for producing")
	defer produceClient.Close()

	numMessages := maxUnackedMessages + 1 // Produce one more message than maxUnacked
	producedMessageIDs := make(chan string, numMessages)
	for i := 0; i < numMessages; i++ {
		record := &kgo.Record{
			Topic: topic,
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		}
		produceCtx, produceCancel := context.WithTimeout(ctx, 5*time.Second)
		defer produceCancel()
		results := produceClient.ProduceSync(produceCtx, record)
		require.NoError(t, results.FirstErr(), fmt.Sprintf("failed to produce message %d to Kafka", i))
		producedMessageIDs <- fmt.Sprintf("message-%d", i) // Store some identifier for verification if needed
	}
	close(producedMessageIDs)

	// 2. Start consuming from the proxy
	consumeStream, err := client.Consume(ctx)
	require.NoError(t, err, "failed to open consume stream")

	subscribeReq := &proxy_pkg.ConsumeRequest{
		RequestType: &proxy_pkg.ConsumeRequest_Subscribe{
			Subscribe: &proxy_pkg.SubscribeRequest{
				Subscription: consumerGroup,
				Topics:       consumerTopics,
			},
		},
	}
	err = consumeStream.Send(subscribeReq)
	require.NoError(t, err, "failed to send subscribe request")

	// 3. Receive maxUnackedMessages messages to trigger backpressure
	receivedMessageIDsSlice := make([]string, 0, numMessages)
	for i := 0; i < maxUnackedMessages; i++ {
		msg, err := consumeStream.Recv()
		require.NoError(t, err, fmt.Sprintf("failed to receive message %d", i))
		receivedMessageIDsSlice = append(receivedMessageIDsSlice, msg.GetMessageId())
		t.Logf("Test received message %d: %s", i, msg.GetMessageId())
	}
	assert.Len(t, receivedMessageIDsSlice, maxUnackedMessages, "Should have received maxUnackedMessages messages")

	// 4. Assert backpressure: The next message should block
	// Try to receive the (maxUnackedMessages + 1)-th message with a timeout context
	t.Logf("Attempting to receive message %d (beyond limit), expecting backpressure", maxUnackedMessages)

	resultChan := make(chan struct {
		msg *proxy_pkg.ConsumeResponse
		err error
	}, 1)

	go func() {
		msg, err := consumeStream.Recv()
		resultChan <- struct {
			msg *proxy_pkg.ConsumeResponse
			err error
		}{msg, err}
	}()

	select {
	case res := <-resultChan:
		// If a message was received, backpressure failed
		require.NoError(t, res.err, "Expected no error, but got one from Recv during backpressure test")
		t.Fatalf("Received message %d (%s) beyond maxUnackedMessages limit, backpressure failed", maxUnackedMessages, res.msg.GetMessageId())
	case <-time.After(1 * time.Second): // Timeout to confirm blocking behavior
		t.Logf("Backpressure working: did not receive message beyond %d unacked", maxUnackedMessages)
	}

	assert.Len(t, receivedMessageIDsSlice, maxUnackedMessages, "No new messages should have been received during backpressure")

	// 5. Send one ACK and expect message flow to resume
	firstMsgID := receivedMessageIDsSlice[0]
	ackReq := &proxy_pkg.ConsumeRequest{
		RequestType: &proxy_pkg.ConsumeRequest_Ack{
			Ack: &proxy_pkg.AckMessage{
				MessageId: firstMsgID,
			},
		},
	}
	t.Logf("Sending ACK for message: %s", firstMsgID)
	err = consumeStream.Send(ackReq)
	require.NoError(t, err, "failed to send ACK for first message")

	// Expect the next message (the one previously blocked) to arrive now
	t.Log("Waiting for message to resume after ACK")

	go func() {
		msg, err := consumeStream.Recv()
		resultChan <- struct {
			msg *proxy_pkg.ConsumeResponse
			err error
		}{msg, err}
	}()

	select {
	case res := <-resultChan:
		require.NoError(t, res.err, "failed to receive message after ACK")
		receivedMessageIDsSlice = append(receivedMessageIDsSlice, res.msg.GetMessageId())
		t.Logf("Received message after ACK: %s", res.msg.GetMessageId())
	case <-time.After(30 * time.Second):
		t.Fatal("Timed out waiting for message after ACK, backpressure did not resume")
	}
	assert.Len(t, receivedMessageIDsSlice, numMessages, "Should have received all messages after ACK")

	// Cleanly close the consume stream
	err = consumeStream.CloseSend()
	require.NoError(t, err, "failed to close consume stream send direction")

	// Ensure the server side also recognizes the stream closure by waiting for the Consume method to return
	select {
	case <-proxyServer.(*proxy_pkg.MessageProxyService).ConsumeDone:
	case <-time.After(10 * time.Second):
		t.Error("Server did not shut down consume stream cleanly")
	}
}
