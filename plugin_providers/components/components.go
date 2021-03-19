package components

import (
	"fmt"

	"github.com/whitaker-io/components/forks/logical"
	"github.com/whitaker-io/components/subscriptions/kafka"
	"github.com/whitaker-io/components/subscriptions/pubsub"
	"github.com/whitaker-io/components/subscriptions/sqs"
	"github.com/whitaker-io/machine"
)

type componentsProvider struct{}

var subscriptionsMap = map[string]func(attributes map[string]interface{}) machine.Subscription{
	"kafka":  kafka.New,
	"pubsub": pubsub.New,
	"sqs":    sqs.New,
}


var forksMap = map[string]func(attributes map[string]interface{}) machine.Fork{
	"logical":  func(attributes map[string]interface{}) machine.Fork {
		if expression, ok := attributes["expression"]; ok {
			return logical.ForkExpression(expression)
		}

		panic("missing expression field in attributes")
	},
	"duplicate":  func(attributes map[string]interface{}) machine.Fork {
		return machine.ForkDuplicate
	},
	"error":  func(attributes map[string]interface{}) machine.Fork {
		return machine.ForkError
	},
}

func (g *componentsProvider) Load(pd *machine.PluginDefinition) (interface{}, error) {
	switch pd.Payload {
	case "subscription":
		return subscriptionsMap[pd.Symbol](pd.Attributes), nil
	case "forks":
		return forksMap[pd.Symbol](pd.Attributes), nil
	}

	return nil, fmt.Errorf("invalid payload type %s", pd.Payload)
}

func init() {
	machine.RegisterPluginProvider("components", &componentsProvider{})
}
