package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	kaf "github.com/segmentio/kafka-go"

	"github.com/whitaker-io/components/utils"
	"github.com/whitaker-io/machine"
)

type writerConfig struct {
	*kaf.WriterConfig
}

type publisher struct {
	client *kaf.Writer
}

// New func to provide a machine.Publisher based on Kafka
func New(attributes map[string]interface{}) machine.Publisher {
	r := &writerConfig{}
	r.fromMap(attributes)

	client := kaf.NewWriter(*r.WriterConfig)

	return &publisher{
		client: client,
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
			Key:   []byte(uuid.NewString()),
			Value: bytez,
		})
	}

	return p.client.WriteMessages(context.Background(), messages...)
}

func (r *writerConfig) fromMap(m map[string]interface{}) {
	var ok bool

	if r.Brokers, ok = utils.StringSlice("brokers", m); !ok {
		panic(fmt.Errorf("required field brokers missing"))
	}

	if r.Topic, ok = utils.String("topic", m); !ok {
		panic(fmt.Errorf("required field topic missing"))
	}

	if x, ok := utils.Integer("max_attempts", m); ok {
		r.MaxAttempts = x
	}

	if x, ok := utils.Integer("batch_size", m); ok {
		r.BatchSize = x
	}

	if x, ok := utils.Integer("batch_bytes", m); ok {
		r.BatchBytes = x
	}

	if x, ok := utils.Duration("batch_bytes", m); ok {
		r.BatchTimeout = x
	}

	if x, ok := utils.Duration("read_timeout", m); ok {
		r.ReadTimeout = x
	}

	if x, ok := utils.Duration("write_timeout", m); ok {
		r.WriteTimeout = x
	}

	if x, ok := utils.Integer("required_acks", m); ok {
		r.RequiredAcks = x
	}

	if x, ok := utils.Boolean("async", m); ok {
		r.Async = x
	}
}
