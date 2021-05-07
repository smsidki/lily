package consumer

import (
	"context"
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"sync"
)

type (
	Container interface {
		Start()
		Stop()
	}

	container struct {
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

func NewContainer(saramaConfig *sarama.Config, consumer Consumer, bootstrapServers []string) Container {
	return &container{
		started:          false,
		saramaConfig:     saramaConfig,
		consumer:         consumer,
		bootstrapServers: bootstrapServers,
	}
}

func (m *container) Start() {
	var err error
	if m.started {
		log.Infof("Consumer group %s topic %s already running", m.consumer.GroupId(), m.consumer.Topics())
	}
	m.SetStarted()
	m.consumerGroup, err = sarama.NewConsumerGroup(m.bootstrapServers, m.consumer.GroupId(), m.saramaConfig)
	if err != nil {
		log.Errorf("Failed to start sonsumer group %s topic %s: %+v", m.consumer.GroupId(), m.consumer.Topics(), err)
		return
	}
	go func() {
		for {
			if err = m.consumerGroup.Consume(m.ctx, m.consumer.Topics(), NewConsumerGroupHandler(m.consumer)); err != nil {
				log.Error(err)
			}
			if err = m.ctx.Err(); err != nil {
				log.Warnf(
					"Consumer group %s topic %s stopped: %v", m.consumer.GroupId(), m.consumer.Topics(), err,
				)
				return
			}
		}
	}()
}

func (m *container) Stop() {
	m.SetStopped()
	if m.consumerGroup != nil {
		if err := m.consumerGroup.Close(); err != nil {
			log.Errorf("Error closing consumer group: %+v", err)
		}
	}
}

func (m *container) SetStarted() {
	m.Lock()
	m.started = true
	m.ctx, m.cancelCtx = context.WithCancel(context.Background())
	m.Unlock()
}

func (m *container) SetStopped() {
	m.Lock()
	m.cancelCtx()
	<-m.ctx.Done()
	m.started = false
	m.Unlock()
}
