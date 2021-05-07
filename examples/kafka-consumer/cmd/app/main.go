package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	consumers "github.com/smsidki/lily/examples/kafka-consumer/consumer"
	"github.com/smsidki/lily/pkg/kafka/consumer"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"runtime"
	"syscall"
	"time"
)

func init() {
	log.SetReportCaller(true)
	log.SetFormatter(
		&log.TextFormatter{CallerPrettyfier: func(frame *runtime.Frame) (function string, file string) {
			function = path.Base(frame.Function)
			file = fmt.Sprintf("%s:%d", filepath.Base(frame.File), frame.Line)
			return
		}},
	)

	sarama.Logger = log.StandardLogger()
}

func main() {
	consumerConfig := &consumer.Config{
		Version:                     sarama.V0_11_0_0.String(),
		AutoOffsetReset:             "earliest",
		BootstrapServers:            []string{"localhost:9092"},
		PartitionAssignmentStrategy: "sticky",
	}

	orderConsumer := consumers.NewOrderConsumer()
	productConsumer := consumers.NewProductConsumer()
	inventoryConsumer := consumers.NewInventoryConsumer()
	kafkaConsumers := []consumer.Consumer{orderConsumer, productConsumer, inventoryConsumer}
	consumerContainer, err := consumer.NewEndpoint(kafkaConsumers, consumerConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		consumerContainer.Stop()
	}()
	consumerContainer.Start()

	<-time.Tick(5 * time.Second)
	consumerContainer.StopID("inventory")

	<-time.Tick(5 * time.Second)
	consumerContainer.StartID("inventory")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sigterm:
		log.Println("Terminating kafka consumer: via signal")
	}
	consumerContainer.Stop()
	log.Info("Exited")
}
