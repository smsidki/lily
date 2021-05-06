package listener

import "github.com/Shopify/sarama"

type (
	Config struct {
		// Kafka version.
		Version string

		// Supported values: earliest, latest.
		AutoOffsetReset string

		BootstrapServers []string

		// Supported values: range, sticky, round-robin.
		PartitionAssignmentStrategy string
	}
)

func (c *Config) ToSaramaConfig() (*sarama.Config, error) {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Consumer.Return.Errors = false

	// configurable
	kafkaVersion, err := sarama.ParseKafkaVersion(c.Version)
	if err != nil {
		return nil, err
	}
	saramaConfig.Version = kafkaVersion
	partitionAssignor, err := ParseBalanceStrategy(c.PartitionAssignmentStrategy)
	if err != nil {
		return nil, err
	}
	saramaConfig.Consumer.Group.Rebalance.Strategy = partitionAssignor
	initialOffset, err := ParseInitialOffset(c.AutoOffsetReset)
	if err != nil {
		return nil, err
	}
	saramaConfig.Consumer.Offsets.Initial = initialOffset

	return saramaConfig, nil
}
