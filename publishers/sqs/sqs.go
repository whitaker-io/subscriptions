package sqs

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	ps "github.com/aws/aws-sdk-go/service/sqs"

	"github.com/whitaker-io/machine"
)

type publisher struct {
	client            *ps.SQS
	queueURL          string
	messageGroupId    *string
	delaySeconds      *int64
	dedupeIDGenerator func(machine.Data) *string
}

// New func to provide a machine.Publisher based on AWS SQS
func New(queueURL string, region string, messageGroupId *string, delaySeconds *int64, dedupeIDGenerator func(machine.Data) *string) machine.Publisher {
	s := session.Must(session.NewSession())

	return &publisher{
		client:            ps.New(s, aws.NewConfig().WithRegion(region)),
		queueURL:          queueURL,
		messageGroupId:    messageGroupId,
		delaySeconds:      delaySeconds,
		dedupeIDGenerator: dedupeIDGenerator,
	}
}

func (p *publisher) Send(payload []machine.Data) error {
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

		message := string(bytez)
		var dedupeID *string

		if p.dedupeIDGenerator != nil {
			dedupeID = p.dedupeIDGenerator(data)
		}

		if _, err = p.client.SendMessage(&ps.SendMessageInput{
			MessageBody:            &message,
			QueueUrl:               &p.queueURL,
			MessageDeduplicationId: dedupeID,
			DelaySeconds:           p.delaySeconds,
			MessageGroupId:         p.messageGroupId,
		}); err != nil {
			if errors == nil {
				errors = err
			} else {
				errors = fmt.Errorf("%s; %w", err.Error(), errors)
			}
		}
	}

	return errors
}
