package listener

import (
	log "github.com/sirupsen/logrus"
	"time"
)

type (
	KafkaContainer interface {
		Run()
		Stop()
		RunByID(consumerID string)
		StopByID(consumerID string)
	}

	kafkaContainer struct {
		listenerManagers map[string]*Manager
	}
)

func NewKafkaContainer(kafkaListeners []KafkaListener, config *Config) (KafkaContainer, error) {
	saramaConfig, err := config.ToSaramaConfig()
	if err != nil {
		return nil, err
	}

	listenerManagers := make(map[string]*Manager, len(kafkaListeners))
	for _, kafkaListener := range kafkaListeners {
		listenerConfig := *saramaConfig
		listenerConfig.ClientID = kafkaListener.Id()
		listenerManager := NewManager(&listenerConfig, kafkaListener, config.BootstrapServers)
		listenerManagers[kafkaListener.Id()] = &listenerManager
	}

	return &kafkaContainer{
		listenerManagers: listenerManagers,
	}, nil
}

func (c *kafkaContainer) Run() {
	for _, listenerManager := range c.listenerManagers {
		listenerManager := *listenerManager
		go func() {
			if err := listenerManager.StartConsumer(); err != nil {
				log.Error(err)
			}
		}()
	}
}

func (c *kafkaContainer) Stop() {
	for _, listenerManager := range c.listenerManagers {
		listenerManager := *listenerManager
		listenerManager.StopConsumer()
	}
	// buffer to close all connection to brokers
	// todo should be configurable
	<-time.Tick(500 * time.Millisecond)
}

func (c *kafkaContainer) RunByID(consumerID string) {
	listenerManager := *c.listenerManagers[consumerID]
	if &listenerManager != nil {
		go func() {
			if err := listenerManager.StartConsumer(); err != nil {
				log.Error(err)
			}
		}()
	}
}

func (c *kafkaContainer) StopByID(consumerID string) {
	listenerManager := *c.listenerManagers[consumerID]
	if &listenerManager != nil {
		listenerManager.StopConsumer()
	}
}
