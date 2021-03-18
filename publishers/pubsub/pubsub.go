package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	ps "cloud.google.com/go/pubsub"

	"github.com/whitaker-io/machine"
)

type publisher struct {
	topic *ps.Topic
}

// New func to provide a machine.Publisher based on Google Pub/Sub
func New(projectID, topicName string) machine.Publisher {
	p := &publisher{}

	if client, err := ps.NewClient(context.Background(), projectID); err != nil {
		panic(err)
	} else if p.topic, err = client.CreateTopic(context.Background(), topicName); err != nil {
		panic(err)
	}

	return p
}

func waitFor(r *ps.PublishResult, waiter *sync.WaitGroup) {
	<-r.Ready()
	waiter.Done()
}

func (p *publisher) Send(payload []machine.Data) error {
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
		result := p.topic.Publish(context.Background(), &ps.Message{Data: bytez})
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
}
