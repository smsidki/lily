package listener

import (
	"context"
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type (
	KafkaContainer interface {
		Run(ctx context.Context) error
	}

	kafkaContainer struct {
		bootstrapServers []string
		saramaConfig     *sarama.Config
		kafkaListeners   []KafkaListener
	}
)

func NewKafkaContainer(kafkaListeners []KafkaListener, listenerConfig *Config) (KafkaContainer, error) {
	saramaConfig, err := listenerConfig.ToSaramaConfig()
	if err != nil {
		return nil, err
	}

	return &kafkaContainer{
		saramaConfig:     saramaConfig,
		kafkaListeners:   kafkaListeners,
		bootstrapServers: listenerConfig.BootstrapServers,
	}, nil
}

func (c *kafkaContainer) Run(ctx context.Context) error {
	consumerGroups := make([]sarama.ConsumerGroup, 0)
	defer func() {
		for _, consumerGroup := range consumerGroups {
			if err := consumerGroup.Close(); err != nil {
				log.Error(err)
			}
		}
	}()

	g, ctx := errgroup.WithContext(ctx)
	for _, kafkaListener := range c.kafkaListeners {
		saramaConfig := c.saramaConfig
		saramaConfig.ClientID = kafkaListener.Id()
		consumerGroup, err := sarama.NewConsumerGroup(c.bootstrapServers, kafkaListener.GroupId(), saramaConfig)
		if err != nil {
			log.Error(err)
		} else {
			kafkaListener := kafkaListener
			consumerGroups = append(consumerGroups, consumerGroup)
			g.Go(func() error {
				c.Consume(ctx, consumerGroup, kafkaListener)
				return nil
			})
		}
	}
	return g.Wait()
}

func (c *kafkaContainer) Consume(ctx context.Context, cg sarama.ConsumerGroup, kafkaListener KafkaListener) {
	for {
		if err := cg.Consume(ctx, kafkaListener.Topics(), NewConsumerGroupHandler(kafkaListener)); err != nil {
			log.Error(err)
		}
		if ctx.Err() != nil {
			return
		}
	}
}
