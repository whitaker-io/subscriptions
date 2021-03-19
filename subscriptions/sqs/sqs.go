package sqs

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	ps "github.com/aws/aws-sdk-go/service/sqs"
	"github.com/google/uuid"

	"github.com/whitaker-io/components/utils"
	"github.com/whitaker-io/machine"
)

type sqs struct {
	subscription *ps.SQS
	config       *readConfig
}

// ReadConfig config used for reading messages values match sqs.ReceiveMessageInput from github.com/aws/aws-sdk-go/service/sqs
type readConfig struct {
	region                string
	maxNumberOfMessages   int64
	queueURL              string
	visibilityTimeout     int64
	waitTimeSeconds       int64
	attributeNames        []*string
	messageAttributeNames []*string
}

func (k *sqs) Read(ctx context.Context) []machine.Data {
	payload := []machine.Data{}

	id := uuid.New().String()

	input := &ps.ReceiveMessageInput{
		MaxNumberOfMessages:     &k.config.maxNumberOfMessages,
		QueueUrl:                &k.config.queueURL,
		VisibilityTimeout:       &k.config.visibilityTimeout,
		WaitTimeSeconds:         &k.config.waitTimeSeconds,
		AttributeNames:          k.config.attributeNames,
		MessageAttributeNames:   k.config.messageAttributeNames,
		ReceiveRequestAttemptId: &id,
	}

	output, err := k.subscription.ReceiveMessage(input)

	if err != nil {
		panic(fmt.Sprintf("error reading from sqs - %v", err))
	} else {
		for _, message := range output.Messages {
			m := map[string]interface{}{}
			err := json.Unmarshal([]byte(*message.Body), &m)
			if err != nil {
				panic(fmt.Sprintf("error unmarshalling from sqs - %v", err))
			} else {
				payload = append(payload, m)
			}
		}
	}

	return payload
}

func (k *sqs) Close() error {
	return nil
}

// New func to provide a machine.Subscription based on AWS SQS
func New(attributes map[string]interface{}) machine.Subscription {
	r := &readConfig{}
	r.fromMap(attributes)

	s := session.Must(session.NewSession())
	svc := ps.New(s, aws.NewConfig().WithRegion(r.region))

	return &sqs{
		subscription: svc,
		config:       r,
	}
}

func (r *readConfig) fromMap(m map[string]interface{}) {
	var ok bool
	if r.region, ok = utils.String("region", m); !ok {
		panic("missing required value project_id")
	}

	if r.queueURL, ok = utils.String("queue_url", m); !ok {
		panic("missing required value queue_url")
	}

	if x, ok := utils.PStringSlice("attribute_names", m); ok {
		r.attributeNames = x
	}

	if x, ok := utils.Integer("max_number_of_messages", m); ok {
		r.maxNumberOfMessages = int64(x)
	}

	if x, ok := utils.PStringSlice("message_attribute_names", m); ok {
		r.messageAttributeNames = x
	}

	if x, ok := utils.Integer("visibility_timeout", m); ok {
		r.visibilityTimeout = int64(x)
	}

	if x, ok := utils.Integer("wait_time_seconds", m); ok {
		r.waitTimeSeconds = int64(x)
	}
}
