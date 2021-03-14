package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	kaf "github.com/segmentio/kafka-go"

	"github.com/whitaker-io/machine"
)

// New func to provide a machine.Subscription based on Kafka
func Sender(config *kaf.WriterConfig, keyGenerator func(machine.Data) []byte) machine.Sender {
	client := kaf.NewWriter(*config)

	return func(payload []machine.Data) error {
		messages := []kaf.Message{}

		var errors error
		for _, data := range payload {
			bytez, err := json.Marshal(data)

			if err != nil {
				if errors == nil {
					errors = err
				} else {
					errors = fmt.Errorf("%s; %w", err.Error(), errors)
				}
			}

			messages = append(messages, kaf.Message{
				Key:   keyGenerator(data),
				Value: bytez,
			})
		}

		return client.WriteMessages(context.Background(), messages...)
	}
}
