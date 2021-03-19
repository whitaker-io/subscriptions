package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	ps "cloud.google.com/go/pubsub"

	"github.com/whitaker-io/components/utils"
	"github.com/whitaker-io/machine"
)

type ReadConfig struct {
	projectID    string
	subscription string
	topic        string
	*ps.SubscriptionConfig
}

type pubsub struct {
	subscription *ps.Subscription
}

func (k *pubsub) Read(ctx context.Context) []machine.Data {
	payload := []machine.Data{}
	packet := machine.Data{}

	err := k.subscription.Receive(context.Background(), func(ctx context.Context, message *ps.Message) {
		if err := json.Unmarshal(message.Data, &packet); err == nil {
			payload = []machine.Data{packet}
		} else if err := json.Unmarshal(message.Data, &payload); err != nil {
			panic(fmt.Sprintf("error unmarshalling from pubsub - %v", err))
		}
		message.Ack()
	})

	if err != nil {
		panic(fmt.Sprintf("error reading from pubsub - %v", err))
	}

	return payload
}

func (k *pubsub) Close() error {
	return nil
}

// New func to provide a machine.Subscription based on Google Pub/Sub
func New(attributes map[string]interface{}) machine.Subscription {
	r := &ReadConfig{}

	r.fromMap(attributes)

	client, err := ps.NewClient(context.Background(), r.projectID)

	if err != nil {
		panic(err)
	}

	r.SubscriptionConfig.Topic = client.Topic(r.topic)

	sub, err := client.CreateSubscription(context.Background(), r.subscription, *r.SubscriptionConfig)

	if err != nil {
		panic(err)
	}

	return &pubsub{
		subscription: sub,
	}
}

func (r *ReadConfig) fromMap(m map[string]interface{}) {
	var ok bool
	if r.projectID, ok = utils.String("project_id", m); !ok {
		panic("missing required value project_id")
	}

	if r.subscription, ok = utils.String("subscription", m); !ok {
		panic("missing required value subscription")
	}

	if r.topic, ok = utils.String("topic", m); !ok {
		panic("missing required value topic")
	}

	if x, ok := utils.Duration("ack_deadline", m); ok {
		r.AckDeadline = x
	}

	if x, ok := utils.Boolean("retain_ack_messages", m); ok {
		r.RetainAckedMessages = x
	}

	if x, ok := utils.Duration("retention_duration", m); ok {
		r.RetentionDuration = x
	}

	r.ExpirationPolicy = time.Duration(0)

	if x, ok := utils.MapStringString("labels", m); ok {
		r.Labels = x
	}

	if x, ok := utils.Boolean("enable_message_ordering", m); ok {
		r.EnableMessageOrdering = x
	}

	if m2, ok := utils.MapStringInterface("dead_letter_policy", m); ok {
		r.DeadLetterPolicy = &ps.DeadLetterPolicy{}
		if x, ok := utils.String("topic", m2); ok {
			r.DeadLetterPolicy.DeadLetterTopic = x
		}
		if x, ok := utils.Integer("max_attempts", m2); ok {
			r.DeadLetterPolicy.MaxDeliveryAttempts = x
		}
	}

	if x, ok := utils.String("filter", m); ok {
		r.Filter = x
	}

	if x, ok := utils.String("filter", m); ok {
		r.Filter = x
	}

	if m2, ok := utils.MapStringInterface("dead_letter_policy", m); ok {
		r.RetryPolicy = &ps.RetryPolicy{}
		if x, ok := utils.Duration("min_backoff", m2); ok {
			r.RetryPolicy.MinimumBackoff = x
		}
		if x, ok := utils.Duration("max_backoff", m2); ok {
			r.RetryPolicy.MaximumBackoff = x
		}
	}

	if x, ok := utils.Boolean("detached", m); ok {
		r.Detached = x
	}
}
