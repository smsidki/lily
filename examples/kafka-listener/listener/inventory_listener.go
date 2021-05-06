package listener

import (
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"github.com/smsidki/lily/pkg/kafka/listener"
)

type (
	InventoryListener listener.KafkaListener

	inventoryListener struct{}
)

func NewInventoryListener() InventoryListener {
	return &inventoryListener{}
}

func (l *inventoryListener) Id() string {
	return "inventory"
}

func (l *inventoryListener) GroupId() string {
	return "product_group"
}

func (l *inventoryListener) AckMode() string {
	return "manual"
}

func (l *inventoryListener) Topics() []string {
	return []string{"x.inventory"}
}

func (l *inventoryListener) OnEventFunc() func(record *sarama.ConsumerMessage) error {
	return nil
}

func (l *inventoryListener) OnEventAckFunc() func(record *sarama.ConsumerMessage, session sarama.ConsumerGroupSession) {
	return func(record *sarama.ConsumerMessage, session sarama.ConsumerGroupSession) {
		log.Infof("inventory %s - %s", record.Topic, string(record.Value))
		session.MarkMessage(record, "")
	}
}
