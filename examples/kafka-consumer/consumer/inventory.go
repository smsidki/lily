package consumer

import (
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"github.com/smsidki/lily/pkg/kafka/consumer"
)

type (
	InventoryConsumer consumer.Consumer

	inventoryConsumer struct{}
)

func NewInventoryConsumer() InventoryConsumer {
	return &inventoryConsumer{}
}

func (c *inventoryConsumer) Id() string {
	return "inventory"
}

func (c *inventoryConsumer) GroupId() string {
	return "product_group"
}

func (c *inventoryConsumer) AckMode() string {
	return "manual"
}

func (c *inventoryConsumer) Topics() []string {
	return []string{"x.inventory"}
}

func (c *inventoryConsumer) OnEventFunc() func(record *sarama.ConsumerMessage) error {
	return nil
}

func (c *inventoryConsumer) OnEventAckFunc() func(record *sarama.ConsumerMessage, session sarama.ConsumerGroupSession) {
	return func(record *sarama.ConsumerMessage, session sarama.ConsumerGroupSession) {
		log.Infof("inventory %s - %s", record.Topic, string(record.Value))
		session.MarkMessage(record, "")
	}
}
