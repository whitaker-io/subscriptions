package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	ps "cloud.google.com/go/pubsub"

	"github.com/whitaker-io/machine"
)

// Sender func to provide a machine.Sender based on Google Pub/Sub
func Sender(projectID, topicName string) (machine.Sender, error) {
	client, err := ps.NewClient(context.Background(), projectID)

	if err != nil {
		return nil, err
	}

	topic, err := client.CreateTopic(context.Background(), topicName)

	if err != nil {
		return nil, err
	}

	return func(payload []machine.Data) error {
		var errors error
		waiter := &sync.WaitGroup{}
		results := []*ps.PublishResult{}
		for _, data := range payload {
			bytez, err := json.Marshal(data)

			if err != nil {
				if errors == nil {
					errors = err
				} else {
					errors = fmt.Errorf("%s; %w", err.Error(), errors)
				}
			}

			waiter.Add(1)
			result := topic.Publish(context.Background(), &ps.Message{Data: bytez})
			results = append(results, result)
			go waitFor(result, waiter)
		}

		waiter.Wait()

		for _, r := range results {
			_, err := r.Get(context.Background())
			if err != nil {
				if errors == nil {
					errors = err
				} else {
					errors = fmt.Errorf("%s; %w", err.Error(), errors)
				}
			}
		}

		return errors
	}, nil
}

func waitFor(r *ps.PublishResult, waiter *sync.WaitGroup) {
	<-r.Ready()
	waiter.Done()
}
