package sqs

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	ps "github.com/aws/aws-sdk-go/service/sqs"

	"github.com/whitaker-io/machine"
)

// Sender func to provide a machine.Sender based on AWS SQS
func Sender(queueURL string, region string, messageGroupId *string, delaySeconds *int64, dedupeIDGenerator func(machine.Data) *string) machine.Sender {
	s := session.Must(session.NewSession())
	svc := ps.New(s, aws.NewConfig().WithRegion(region))

	return func(payload []machine.Data) error {
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

			if dedupeIDGenerator != nil {
				dedupeID = dedupeIDGenerator(data)
			}

			if _, err = svc.SendMessage(&ps.SendMessageInput{
				MessageBody:            &message,
				QueueUrl:               &queueURL,
				MessageDeduplicationId: dedupeID,
				DelaySeconds:           delaySeconds,
				MessageGroupId:         messageGroupId,
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
}
