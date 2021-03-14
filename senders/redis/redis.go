package redis

import (
	"encoding/json"
	"fmt"

	ps "github.com/gomodule/redigo/redis"

	"github.com/whitaker-io/machine"
)

// Sender func to provide a machine.Sender based on Redis
func Sender(pool *ps.Pool, topic string) machine.Sender {
	client := &ps.PubSubConn{
		Conn: pool.Get(),
	}

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

			if _, err = client.Conn.Do("PUBLISH", topic, bytez); err != nil {
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
