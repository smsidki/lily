package consumer

import (
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
)

func ParseBalanceStrategy(partitionAssignor string) (sarama.BalanceStrategy, error) {
	switch partitionAssignor {
	case "range":
		return sarama.BalanceStrategyRange, nil
	case "sticky":
		return sarama.BalanceStrategySticky, nil
	case "round-robin":
		return sarama.BalanceStrategyRoundRobin, nil
	default:
		return nil, errors.New(fmt.Sprintf("unrecognized consumer group partition assignor: %s", partitionAssignor))
	}
}

func ParseInitialOffset(initialOffset string) (int64, error) {
	switch initialOffset {
	case "earliest":
		return sarama.OffsetNewest, nil
	case "latest":
		return sarama.OffsetOldest, nil
	default:
		return 0, errors.New(fmt.Sprintf("unrecognized consumer initial offset: %s", initialOffset))
	}
}
