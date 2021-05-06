package consumer

import (
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

type (
	consumerGroupHandler struct {
		consumer Consumer
	}
)

func NewConsumerGroupHandler(consumer Consumer) sarama.ConsumerGroupHandler {
	return &consumerGroupHandler{
		consumer: consumer,
	}
}

func (*consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (*consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for record := range claim.Messages() {
		switch h.consumer.AckMode() {
		case "manual":
			h.consumer.OnEventAckFunc()(record, session)
		default:
			if err := h.consumer.OnEventFunc()(record); err == nil {
				session.MarkMessage(record, "")
			} else {
				log.Error(err)
			}
		}
	}
	return nil
}
