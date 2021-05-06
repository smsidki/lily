package listener

import (
	"errors"
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"github.com/smsidki/lily/pkg/kafka/listener"
	"strconv"
)

type (
	OrderListener listener.KafkaListener

	orderListener struct{}
)

func NewOrderListener() OrderListener {
	return &orderListener{}
}

func (l *orderListener) Id() string {
	return "order"
}

func (l *orderListener) GroupId() string {
	return "product_group"
}

func (l *orderListener) AckMode() string {
	return ""
}

func (l *orderListener) Topics() []string {
	return []string{"x.order"}
}

func (l *orderListener) OnEventFunc() func(record *sarama.ConsumerMessage) error {
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

func (l *orderListener) OnEventAckFunc() func(record *sarama.ConsumerMessage, session sarama.ConsumerGroupSession) {
	return nil
}
