package listener

import (
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

type (
	consumerGroupHandler struct {
		kafkaListener KafkaListener
	}
)

func NewConsumerGroupHandler(kafkaListener KafkaListener) sarama.ConsumerGroupHandler {
	return &consumerGroupHandler{
		kafkaListener: kafkaListener,
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
		switch h.kafkaListener.AckMode() {
		case "manual":
			h.kafkaListener.OnEventAckFunc()(record, session)
		default:
			if err := h.kafkaListener.OnEventFunc()(record); err == nil {
				session.MarkMessage(record, "")
			} else {
				log.Error(err)
			}
		}
	}
	return nil
}
