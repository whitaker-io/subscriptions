package logical

import (
	"github.com/Knetic/govaluate"

	"github.com/whitaker-io/machine"
)

// ForkProvider Vertex Provider that uses https://github.com/Knetic/govaluate
// to create a machine.Fork.
// attributes must contain the key "expression" and it must be a string
func ForkProvider(pd *machine.PluginDefinition) machine.Fork {
	return ForkExpression(pd.Payload)
}

// ForkExpression uses https://github.com/Knetic/govaluate
// to create a machine.Fork.
func ForkExpression(expression string) machine.Fork {
	return logical(expression).Handler
}

func logical(e string) machine.ForkRule {
	expression, err := govaluate.NewEvaluableExpression(e)

	if err != nil {
		panic(err)
	}

	return func(data machine.Data) bool {
		result, err := expression.Evaluate(data)

		if val, ok := result.(bool); err != nil || !ok {
			return false
		} else {
			return val
		}
	}
}
