package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	listeners "github.com/smsidki/lily/examples/kafka-listener/listener"
	"github.com/smsidki/lily/pkg/kafka/listener"
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
	listenerConfig := &listener.Config{
		Version:                     sarama.V0_11_0_0.String(),
		AutoOffsetReset:             "earliest",
		BootstrapServers:            []string{"localhost:9092"},
		PartitionAssignmentStrategy: "sticky",
	}

	orderListener := listeners.NewOrderListener()
	productListener := listeners.NewProductListener()
	inventoryListener := listeners.NewInventoryListener()
	kafkaListeners := []listener.KafkaListener{orderListener, productListener, inventoryListener}
	kafkaContainer, err := listener.NewKafkaContainer(kafkaListeners, listenerConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		kafkaContainer.Stop()
	}()
	kafkaContainer.Run()

	<-time.Tick(5 * time.Second)
	kafkaContainer.StopByID("inventory")

	<-time.Tick(5 * time.Second)
	kafkaContainer.RunByID("inventory")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sigterm:
		log.Println("Terminating kafka consumer: via signal")
	}
	kafkaContainer.Stop()
	log.Info("Exited")
}
