package consumer

import (
	"errors"
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"github.com/smsidki/lily/pkg/kafka/consumer"
	"strconv"
)

type (
	OrderConsumer consumer.Consumer

	orderConsumer struct{}
)

func NewOrderConsumer() OrderConsumer {
	return &orderConsumer{}
}

func (c *orderConsumer) Id() string {
	return "order"
}

func (c *orderConsumer) GroupId() string {
	return "product_group"
}

func (c *orderConsumer) AckMode() string {
	return ""
}

func (c *orderConsumer) Topics() []string {
	return []string{"x.order"}
}

func (c *orderConsumer) OnEventFunc() func(record *sarama.ConsumerMessage) error {
	return func(record *sarama.ConsumerMessage) error {
		value := string(record.Value)
		log.Infof("order %s - %s", record.Topic, value)
		number, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		if number%2 == 0 {
			return errors.New("invalid order")
		}
		return nil
	}
}

func (c *orderConsumer) OnEventAckFunc() func(record *sarama.ConsumerMessage, session sarama.ConsumerGroupSession) {
	return nil
}
