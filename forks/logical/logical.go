package logical

import (
	"fmt"

	"github.com/Knetic/govaluate"

	"github.com/whitaker-io/machine"
)

// ForkProvider Vertex Provider that uses https://github.com/Knetic/govaluate
// to create a machine.Fork.
// attributes must contain the key "expression" and it must be a string
func ForkProvider(attributes map[string]interface{}) machine.Fork {
	if exp, ok := attributes["expression"]; !ok {
		panic(fmt.Errorf("missing expression"))
	} else if expression, ok := exp.(string); !ok {
		panic(fmt.Errorf("invalid expression type"))
	} else {
		return ForkExpression(expression)
	}
}

// ForkExpression uses https://github.com/Knetic/govaluate
// to create a machine.Fork.
func ForkExpression(expression string) machine.Fork {
	return Fork(logical(expression))
}

// Fork provides a machine.Fork based on the expression func provided
func Fork(expression func(machine.Data) bool) machine.Fork {
	return func(list []*machine.Packet) (a []*machine.Packet, b []*machine.Packet) {
		payloadA := []*machine.Packet{}
		payloadB := []*machine.Packet{}

		for _, packet := range list {
			if expression(packet.Data) {
				payloadA = append(payloadA, packet)
			} else {
				payloadB = append(payloadB, packet)
			}
		}

		return payloadA, payloadB
	}
}

func logical(e string) func(machine.Data) bool {
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
