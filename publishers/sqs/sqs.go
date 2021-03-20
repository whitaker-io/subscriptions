package sqs

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	ps "github.com/aws/aws-sdk-go/service/sqs"
	"github.com/google/uuid"

	"github.com/whitaker-io/components/utils"
	"github.com/whitaker-io/machine"
)

type publisher struct {
	client         *ps.SQS
	queueURL       string
	messageGroupId *string
	delaySeconds   *int64
}

// New func to provide a machine.Publisher based on AWS SQS
func New(attributes map[string]interface{}) machine.Publisher {
	var ok bool
	var queueURL, region, messageGroupId string
	delaySeconds := int64(0)

	if queueURL, ok = utils.String("queue_url", attributes); !ok {
		panic(fmt.Errorf("required field queue_url missing"))
	}

	if region, ok = utils.String("region", attributes); !ok {
		panic(fmt.Errorf("required field region missing"))
	}

	if messageGroupId, ok = utils.String("message_group_id", attributes); !ok {
		panic(fmt.Errorf("required field message_group_id missing"))
	}

	if delay, ok := utils.Integer("delay_seconds", attributes); ok {
		delaySeconds = int64(delay)
	}

	s := session.Must(session.NewSession())

	return &publisher{
		client:         ps.New(s, aws.NewConfig().WithRegion(region)),
		queueURL:       queueURL,
		messageGroupId: &messageGroupId,
		delaySeconds:   &delaySeconds,
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
		dedupeID := uuid.NewString()

		if _, err = p.client.SendMessage(&ps.SendMessageInput{
			MessageBody:            &message,
			QueueUrl:               &p.queueURL,
			MessageDeduplicationId: &dedupeID,
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
