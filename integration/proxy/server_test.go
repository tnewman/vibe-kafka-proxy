package proxy

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kadm" // New import for Kafka Admin API
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"os" // New import for os.Stderr

	"github.com/tnewman/kafka-proxy/pkg/broker/kafka" // Direct import
	proxy_pkg "github.com/tnewman/kafka-proxy/pkg/proxy"

	tc_kafka "github.com/testcontainers/testcontainers-go/modules/kafka"

)

func TestMessageProxy_Produce(t *testing.T) {
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

	kafkaSeedBrokers, err := kafkaContainer.Brokers(ctx) // Reverting to GetBrokerURLs
	require.NoError(t, err, "failed to get kafka seed brokers")

	// Create Kafka producer and consumer for the test
	logger, _ := zap.NewDevelopment()
	kafkaProducer, err := kafka.NewProducer(ctx, kafkaSeedBrokers, logger) // Use direct 'kafka' package
	require.NoError(t, err, "failed to create kafka producer")
	defer kafkaProducer.Close()

	// We don't need a real consumer for the proxy service in this test
	// We don't need a real consumer for the proxy service in this test, using nil
	// For maxUnackedMessages, using a default value as it's not relevant for produce tests
	proxyServer := proxy_pkg.NewMessageProxyServer(kafkaProducer, nil, logger, 100, 1000)

	// Setup gRPC server on a bufconn listener
	const bufSize = 1024 * 1024
	lis := bufconn.Listen(bufSize)
	s := grpc.NewServer()
	proxy_pkg.RegisterMessageProxyServer(s, proxyServer)
	go func() {
		if err := s.Serve(lis); err != nil {
			t.Logf("Server exited with error: %v", err)
		}
	}()
	defer s.Stop()

	// Setup gRPC client
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err, "failed to dial bufnet")
	defer conn.Close()

	client := proxy_pkg.NewMessageProxyClient(conn)

	// --- Test Logic ---
	topic := "test-topic"
	clientMessageID := uuid.New().String()

	// Create Kafka Admin Client
	// First, create a kgo.Client
	kgoClient, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaSeedBrokers...),
	)
	require.NoError(t, err, "failed to create kgo client for admin API")
	defer func() {
		// Only close if it was successfully created
		if kgoClient != nil {
			kgoClient.Close()
		}
	}()

	adminClient := kadm.NewClient(kgoClient)

	// Create the topic
	_, err = adminClient.CreateTopic(ctx, 1, 1, nil, topic) // 1 partition, 1 replication factor
	require.NoError(t, err, "failed to create topic")
	// Allow some time for topic creation to propagate
	time.Sleep(1 * time.Second)

	stream, err := client.Produce(ctx)
	require.NoError(t, err, "failed to open produce stream")

	// Send a produce request
	req := &proxy_pkg.ProduceRequest{ // Access ProduceRequest via proxy_pkg
		ClientMessageId: clientMessageID,
		Topic:           topic,
		Key:             []byte("test-key"),
		Value:           []byte("test-value"),
	}
	err = stream.Send(req)
	require.NoError(t, err, "failed to send produce request")

	// Receive the response
	res, err := stream.Recv()
	require.NoError(t, err, "failed to receive produce response")

	// Assertions on the response
	assert.True(t, res.Success, "expected successful produce response")
	assert.Equal(t, clientMessageID, res.ClientMessageId, "response client message ID mismatch")
	assert.Empty(t, res.ErrorMessage, "expected no error message on success")

	// --- Verification ---
	// Use a franz-go client to consume the message from Kafka and verify it was produced correctly
	verifyConsumer, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaSeedBrokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelInfo, nil)), // Provide os.Stderr
	)
	require.NoError(t, err, "failed to create verification consumer")
	defer verifyConsumer.Close()

	// Create the topic before consuming
	// NOTE: This is racy, but usually fine in tests. A better approach would be to use the Admin API.
	time.Sleep(2 * time.Second) // Give producer time to create the topic
	
	fetches := verifyConsumer.PollFetches(ctx)
	require.NoError(t, fetches.Err(), "failed to poll for fetches")

	var foundRecord bool
	fetches.EachRecord(func(record *kgo.Record) {
		if string(record.Key) == "test-key" {
			foundRecord = true
			assert.Equal(t, "test-value", string(record.Value), "message value mismatch")
			t.Logf("Successfully verified message in topic %s", topic)
		}
	})

	assert.True(t, foundRecord, "did not find the produced message in topic")
}