package consumer

import (
	"time"
)

type (
	Endpoint interface {

		// Start start all consumer containers managed by this endpoint
		Start()

		// Stop stop all consumer containers managed by this endpoint
		Stop()

		// StartID start consumer container with matching consumerID
		StartID(consumerID string)

		// StopID stop consumer container with matching consumerID
		StopID(consumerID string)
	}

	endpoint struct {
		consumerContainers map[string]Container
	}
)

func NewEndpoint(consumers []Consumer, config *Config) (Endpoint, error) {
	saramaConfig, err := config.ToSaramaConfig()
	if err != nil {
		return nil, err
	}

	consumerContainers := make(map[string]Container, len(consumers))
	for _, consumer := range consumers {
		consumerConfig := *saramaConfig
		consumerConfig.ClientID = consumer.Id()
		consumerContainers[consumer.Id()] = NewContainer(&consumerConfig, consumer, config.BootstrapServers)
	}

	return &endpoint{
		consumerContainers: consumerContainers,
	}, nil
}

func (c *endpoint) Start() {
	for _, consumerContainer := range c.consumerContainers {
		consumerContainer.Start()
	}
}

func (c *endpoint) Stop() {
	for _, consumerContainer := range c.consumerContainers {
		consumerContainer.Stop()
	}
	// buffer to close all connection to brokers
	// todo should be configurable
	<-time.Tick(500 * time.Millisecond)
}

func (c *endpoint) StartID(consumerID string) {
	if consumerContainer, ok := c.consumerContainers[consumerID]; ok {
		consumerContainer.Start()
	}
}

func (c *endpoint) StopID(consumerID string) {
	if consumerContainer, ok := c.consumerContainers[consumerID]; ok {
		consumerContainer.Stop()
	}
}
