package listener

import (
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"github.com/smsidki/lily/pkg/kafka/listener"
)

type (
	ProductListener listener.KafkaListener

	productListener struct{}
)

func NewProductListener() ProductListener {
	return &productListener{}
}

func (l *productListener) Id() string {
	return "product"
}

func (l *productListener) GroupId() string {
	return "product_group"
}

func (l *productListener) AckMode() string {
	return "manual"
}

func (l *productListener) Topics() []string {
	return []string{"x.product"}
}

func (l *productListener) OnEventFunc() func(record *sarama.ConsumerMessage) error {
	return nil
}

func (l *productListener) OnEventAckFunc() func(record *sarama.ConsumerMessage, session sarama.ConsumerGroupSession) {
	return func(record *sarama.ConsumerMessage, session sarama.ConsumerGroupSession) {
		log.Infof("product %s - %s", record.Topic, string(record.Value))
		session.MarkMessage(record, "")
	}
}
