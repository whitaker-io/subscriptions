package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	kaf "github.com/segmentio/kafka-go"

	"github.com/whitaker-io/machine"
)

type publisher struct {
	client       *kaf.Writer
	keyGenerator func(machine.Data) []byte
}

// New func to provide a machine.Publisher based on Kafka
func New(config *kaf.WriterConfig, keyGenerator func(machine.Data) []byte) machine.Publisher {
	client := kaf.NewWriter(*config)

	return &publisher{
		client:       client,
		keyGenerator: keyGenerator,
	}
}

func (p *publisher) Send(payload []machine.Data) error {
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
			Key:   p.keyGenerator(data),
			Value: bytez,
		})
	}

	return p.client.WriteMessages(context.Background(), messages...)
}
