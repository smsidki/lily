package listener

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
		cg               sarama.ConsumerGroup
		saramaConfig     *sarama.Config
		kafkaListener    KafkaListener
		bootstrapServers []string
	}
)

func NewManager(saramaConfig *sarama.Config, kafkaListener KafkaListener, bootstrapServers []string) Manager {
	return &manager{
		started:          false,
		saramaConfig:     saramaConfig,
		kafkaListener:    kafkaListener,
		bootstrapServers: bootstrapServers,
	}
}

func (m *manager) StartConsumer() (err error) {
	if m.started {
		return errors.New("consumer already started")
	}
	m.SetStarted()
	m.cg, err = sarama.NewConsumerGroup(m.bootstrapServers, m.kafkaListener.GroupId(), m.saramaConfig)
	if err != nil {
		return
	}
	for {
		if err = m.cg.Consume(m.ctx, m.kafkaListener.Topics(), NewConsumerGroupHandler(m.kafkaListener)); err != nil {
			log.Error(err)
		}
		if err = m.ctx.Err(); err != nil {
			log.Warnf(
				"Consumer group %s topic %s stopped: %v", m.kafkaListener.GroupId(), m.kafkaListener.Topics(), err,
			)
			break
		}
	}
	err = nil
	return
}

func (m *manager) StopConsumer() {
	m.SetStopped()
	if m.cg != nil {
		if err := m.cg.Close(); err != nil {
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
