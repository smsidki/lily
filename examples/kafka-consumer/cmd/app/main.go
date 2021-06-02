package main

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/gin-gonic/gin"
	errors "github.com/rotisserie/eris"
	log "github.com/sirupsen/logrus"
	consumers "github.com/smsidki/lily/examples/kafka-consumer/consumer"
	"github.com/smsidki/lily/pkg/kafka/consumer"
	"net/http"
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
		&log.TextFormatter{
			TimestampFormat: "Jan _2 15:04:05.000000000",
			CallerPrettyfier: func(frame *runtime.Frame) (function string, file string) {
				function = path.Base(frame.Function)
				file = fmt.Sprintf("%s:%d", filepath.Base(frame.File), frame.Line)
				return
			},
		},
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
		log.Info("Exited")
	}()
	consumerContainer.Start()

	router := gin.Default()
	router.Handle("GET", "/api/utils/kafka/consumers/_start", func(c *gin.Context) {
		consumerIDs := c.QueryArray("consumerID")
		if len(consumerIDs) == 0 {
			consumerContainer.Start()
		} else {
			for _, consumerID := range consumerIDs {
				consumerContainer.StartID(consumerID)
			}
		}
		c.JSON(http.StatusOK, gin.H{
			"success": true,
		})
	})
	router.Handle("GET", "/api/utils/kafka/consumers/_stop", func(c *gin.Context) {
		consumerIDs := c.QueryArray("consumerID")
		if len(consumerIDs) == 0 {
			consumerContainer.Stop()
		} else {
			for _, consumerID := range consumerIDs {
				consumerContainer.StopID(consumerID)
			}
		}
		c.JSON(http.StatusOK, gin.H{
			"success": true,
		})
	})

	server := &http.Server{Addr: ":8080", Handler: router}
	go func() {
		if err := server.ListenAndServe(); err != nil {
			if errors.Is(err, http.ErrServerClosed) {
				log.Infof("App stopped to serve API: %v", err)
				return
			}
			log.Infof("Failed to serve API: %+v", err)
		}
	}()

	sigterm := make(chan os.Signal)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm

	log.Info("App will exit")
	ctx, cancelCtx := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelCtx()
	if err := server.Shutdown(ctx); err != nil {
		log.Errorf("Failed to shutdown server: %+v", err)
	}
}
