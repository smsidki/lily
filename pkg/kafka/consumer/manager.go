package consumer

import (
	"context"
	"errors"
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"sync"
)

type (
	Manager interface {
		StartConsumer() error
		StopConsumer()
	}

	manager struct {
		sync.Mutex
		started          bool
		ctx              context.Context
		cancelCtx        context.CancelFunc
		consumerGroup    sarama.ConsumerGroup
		saramaConfig     *sarama.Config
		consumer         Consumer
		bootstrapServers []string
	}
)

func NewManager(saramaConfig *sarama.Config, consumer Consumer, bootstrapServers []string) Manager {
	return &manager{
		started:          false,
		saramaConfig:     saramaConfig,
		consumer:         consumer,
		bootstrapServers: bootstrapServers,
	}
}

func (m *manager) StartConsumer() (err error) {
	if m.started {
		return errors.New("consumer already started")
	}
	m.SetStarted()
	m.consumerGroup, err = sarama.NewConsumerGroup(m.bootstrapServers, m.consumer.GroupId(), m.saramaConfig)
	if err != nil {
		return
	}
	for {
		if err = m.consumerGroup.Consume(m.ctx, m.consumer.Topics(), NewConsumerGroupHandler(m.consumer)); err != nil {
			log.Error(err)
		}
		if err = m.ctx.Err(); err != nil {
			log.Warnf(
				"Consumer group %s topic %s stopped: %v", m.consumer.GroupId(), m.consumer.Topics(), err,
			)
			break
		}
	}
	err = nil
	return
}

func (m *manager) StopConsumer() {
	m.SetStopped()
	if m.consumerGroup != nil {
		if err := m.consumerGroup.Close(); err != nil {
			log.Errorf("Error closing consumer group: %+v", err)
		}
	}
}

func (m *manager) SetStarted() {
	m.Lock()
	m.started = true
	m.ctx, m.cancelCtx = context.WithCancel(context.Background())
	m.Unlock()
}

func (m *manager) SetStopped() {
	m.Lock()
	m.cancelCtx()
	<-m.ctx.Done()
	m.started = false
	m.Unlock()
}
