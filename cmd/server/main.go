package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/tnewman/kafka-proxy/pkg/broker/kafka"
	"github.com/tnewman/kafka-proxy/pkg/proxy"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// Config holds all configurable parameters for the server.
type Config struct {
	GRPCPort                 string
	KafkaSeedBrokers         []string
	MaxPendingProduces       int64
	MaxUnackedClientMessages int64
	ConsumerGroup            string
	KafkaCommitInterval      time.Duration
}

var cfgFile string
var config Config
var logger *zap.Logger

var rootCmd = &cobra.Command{
	Use:   "kafka-proxy",
	Short: "A gRPC proxy for Kafka messages",
	Long:  `A gRPC proxy for Kafka messages, providing consume and produce capabilities with backpressure and abstraction.`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		// Initialize logger before any command runs
		var err error
		logger, err = zap.NewDevelopment() // Use NewProduction in production
		if err != nil {
			return fmt.Errorf("failed to initialize logger: %w", err)
		}
		// Assuming configuration will not change after startup, no need to watch config.
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		startServer()
	},
}

func Execute() {
	cobra.CheckErr(rootCmd.Execute())
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.kafka-proxy.yaml)")

	rootCmd.PersistentFlags().String("grpc-port", "50051", "Port for the gRPC server to listen on")
	viper.BindPFlag("grpc-port", rootCmd.PersistentFlags().Lookup("grpc-port"))

	rootCmd.PersistentFlags().String("kafka-seed-brokers", "localhost:9092", "Comma-separated list of Kafka seed brokers")
	viper.BindPFlag("kafka-seed-brokers", rootCmd.PersistentFlags().Lookup("kafka-seed-brokers"))

	rootCmd.PersistentFlags().Int64("max-pending-produces", 1000, "Maximum number of pending produce requests before blocking")
	viper.BindPFlag("max-pending-produces", rootCmd.PersistentFlags().Lookup("max-pending-produces"))

	rootCmd.PersistentFlags().Int64("max-unacked-client-messages", 1000, "Maximum number of unacknowledged messages per client before pausing consumption")
	viper.BindPFlag("max-unacked-client-messages", rootCmd.PersistentFlags().Lookup("max-unacked-client-messages"))

	rootCmd.PersistentFlags().String("kafka-consumer-group", "proxy-consumer-group", "Kafka consumer group name for the proxy")
	viper.BindPFlag("kafka-consumer-group", rootCmd.PersistentFlags().Lookup("kafka-consumer-group"))

	rootCmd.PersistentFlags().Duration("kafka-commit-interval", 5*time.Second, "Interval for committing Kafka offsets")
	viper.BindPFlag("kafka-commit-interval", rootCmd.PersistentFlags().Lookup("kafka-commit-interval"))
}

func initConfig() {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)

		viper.AddConfigPath(home)
		viper.SetConfigType("yaml")
		viper.SetConfigName(".kafka-proxy")
	}

	viper.AutomaticEnv() // read in environment variables that match

	if err := viper.ReadInConfig(); err == nil {
		fmt.Fprintln(os.Stderr, "Using config file:", viper.ConfigFileUsed())
	} else if _, ok := err.(viper.ConfigFileNotFoundError); ok {
		// Config file not found, but it's not a fatal error if we rely on env vars or flags
		fmt.Fprintln(os.Stderr, "No config file found, relying on environment variables or flags.")
	} else {
		cobra.CheckErr(err) // Handle other errors reading the config file
	}

	// Unmarshal the config into the struct
	if err := viper.Unmarshal(&config); err != nil {
		cobra.CheckErr(err)
	}
}

func main() {
	Execute()
}

func startServer() {
	defer func() {
		_ = logger.Sync() // Flushes buffer, if any
	}()

	logger.Info("Starting Kafka Message Proxy Server...", zap.Any("config", config))

	// --- Broker (Kafka) setup ---
	kafkaSeedBrokers := config.KafkaSeedBrokers

	producerCtx, producerCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer producerCancel()

	kafkaProducer, err := kafka.NewProducer(producerCtx, kafkaSeedBrokers, logger)
	if err != nil {
		logger.Fatal("Failed to create Kafka producer", zap.Error(err))
	}
	defer func() {
		if err := kafkaProducer.Close(); err != nil {
			logger.Error("Failed to close Kafka producer", zap.Error(err))
		}
	}()
	logger.Info("Kafka producer initialized")

	consumerCtx, consumerCancel := context.WithCancel(context.Background())
	defer consumerCancel()

	kafkaConsumer, err := kafka.NewConsumer(consumerCtx, kafkaSeedBrokers, []string{}, config.ConsumerGroup, int(config.MaxUnackedClientMessages), config.KafkaCommitInterval, logger)
	if err != nil {
		logger.Fatal("Failed to create Kafka consumer", zap.Error(err))
	}
	defer func() {
		if err := kafkaConsumer.Close(); err != nil {
			logger.Error("Failed to close Kafka consumer", zap.Error(err))
		}
	}()
	logger.Info("Kafka consumer initialized")

	// --- gRPC Server setup ---
	lis, err := net.Listen("tcp", ":"+config.GRPCPort)
	if err != nil {
		logger.Fatal("Failed to listen", zap.String("port", config.GRPCPort), zap.Error(err))
	}

	grpcServer := grpc.NewServer()

	messageProxyServer := proxy.NewMessageProxyServer(kafkaProducer, kafkaConsumer, logger, config.MaxPendingProduces, config.MaxUnackedClientMessages)
	proxy.RegisterMessageProxyServer(grpcServer, messageProxyServer)

	reflection.Register(grpcServer)

	logger.Info("gRPC server starting", zap.String("port", config.GRPCPort))
	go func() {
		if serveErr := grpcServer.Serve(lis); serveErr != nil {
			logger.Error("gRPC server failed to serve", zap.Error(serveErr))
		}
	}()

	// --- Graceful Shutdown ---
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logger.Info("Shutting down server...")

	// Create a context for graceful shutdown with a timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second) // 5 seconds is an arbitrary default
	defer shutdownCancel()

	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
		logger.Info("gRPC server gracefully stopped.")
	case <-shutdownCtx.Done():
		logger.Warn("gRPC server did not stop gracefully within timeout, forcing shutdown.")
		grpcServer.Stop() // Force stop if graceful stop times out
	}

	logger.Info("Server stopped.")
}
