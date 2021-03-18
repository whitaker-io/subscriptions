package redis

import (
	"encoding/json"
	"fmt"

	ps "github.com/gomodule/redigo/redis"

	"github.com/whitaker-io/machine"
)

type publisher struct {
	client *ps.PubSubConn
	topic  string
}

// New func to provide a machine.Publisher based on Redis
func New(pool *ps.Pool, topic string) machine.Publisher {
	client := &ps.PubSubConn{
		Conn: pool.Get(),
	}

	return &publisher{
		client: client,
		topic:  topic,
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

		if _, err = p.client.Conn.Do("PUBLISH", p.topic, bytez); err != nil {
			if errors == nil {
				errors = err
			} else {
				errors = fmt.Errorf("%s; %w", err.Error(), errors)
			}
		}
	}

	return errors
}
