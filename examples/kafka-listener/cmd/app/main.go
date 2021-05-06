package main

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	listeners "github.com/smsidki/lily/examples/kafka-listener/listener"
	"github.com/smsidki/lily/pkg/kafka/listener"
	"golang.org/x/sync/errgroup"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"runtime"
	"syscall"
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

	ctx, cancelCtx := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(ctx)
	kafkaContainer, err := listener.NewKafkaContainer(kafkaListeners, listenerConfig)
	if err != nil {
		log.Fatal(err)
	}
	g.Go(func() error {
		return kafkaContainer.Run(ctx)
	})

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sigterm:
		log.Println("Terminating kafka consumer: via signal")
	case <-ctx.Done():
		log.Println("Terminating kafka consumer: context cancelled")
	}
	cancelCtx()
	if err := g.Wait(); err != nil {
		log.Error(err)
	}
	log.Info("Exited")
}
