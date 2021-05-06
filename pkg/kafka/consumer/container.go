package consumer

import (
	log "github.com/sirupsen/logrus"
	"time"
)

type (
	Container interface {
		Run()
		Stop()
		RunByID(consumerID string)
		StopByID(consumerID string)
	}

	container struct {
		consumerManagers map[string]*Manager
	}
)

func NewContainer(consumers []Consumer, config *Config) (Container, error) {
	saramaConfig, err := config.ToSaramaConfig()
	if err != nil {
		return nil, err
	}

	consumerManagers := make(map[string]*Manager, len(consumers))
	for _, consumer := range consumers {
		consumerConfig := *saramaConfig
		consumerConfig.ClientID = consumer.Id()
		consumerManager := NewManager(&consumerConfig, consumer, config.BootstrapServers)
		consumerManagers[consumer.Id()] = &consumerManager
	}

	return &container{
		consumerManagers: consumerManagers,
	}, nil
}

func (c *container) Run() {
	for _, consumerManager := range c.consumerManagers {
		consumerManager := *consumerManager
		go func() {
			if err := consumerManager.StartConsumer(); err != nil {
				log.Error(err)
			}
		}()
	}
}

func (c *container) Stop() {
	for _, consumerManager := range c.consumerManagers {
		consumerManager := *consumerManager
		consumerManager.StopConsumer()
	}
	// buffer to close all connection to brokers
	// todo should be configurable
	<-time.Tick(500 * time.Millisecond)
}

func (c *container) RunByID(consumerID string) {
	consumerManager := *c.consumerManagers[consumerID]
	if &consumerManager != nil {
		go func() {
			if err := consumerManager.StartConsumer(); err != nil {
				log.Error(err)
			}
		}()
	}
}

func (c *container) StopByID(consumerID string) {
	consumerManager := *c.consumerManagers[consumerID]
	if &consumerManager != nil {
		consumerManager.StopConsumer()
	}
}
