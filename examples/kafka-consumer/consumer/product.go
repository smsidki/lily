package consumer

import (
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"github.com/smsidki/lily/pkg/kafka/consumer"
)

type (
	ProductConsumer consumer.Consumer

	productConsumer struct{}
)

func NewProductConsumer() ProductConsumer {
	return &productConsumer{}
}

func (c *productConsumer) Id() string {
	return "product"
}

func (c *productConsumer) GroupId() string {
	return "product_group"
}

func (c *productConsumer) AckMode() string {
	return "manual"
}

func (c *productConsumer) Topics() []string {
	return []string{"x.product"}
}

func (c *productConsumer) OnEventFunc() func(record *sarama.ConsumerMessage) error {
	return nil
}

func (c *productConsumer) OnEventAckFunc() func(record *sarama.ConsumerMessage, session sarama.ConsumerGroupSession) {
	return func(record *sarama.ConsumerMessage, session sarama.ConsumerGroupSession) {
		log.Infof("product %s - %s", record.Topic, string(record.Value))
		session.MarkMessage(record, "")
	}
}
