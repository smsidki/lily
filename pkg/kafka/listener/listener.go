package listener

import "github.com/Shopify/sarama"

type (
	KafkaListener interface {
		Id() string

		GroupId() string

		// Supported value: manual.
		AckMode() string

		Topics() []string

		OnEventFunc() func(record *sarama.ConsumerMessage) error

		// Required if AckMode equals to "manual".
		OnEventAckFunc() func(record *sarama.ConsumerMessage, session sarama.ConsumerGroupSession)
	}
)
