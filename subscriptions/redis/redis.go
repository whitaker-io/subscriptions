package redis

import (
	"context"
	"encoding/json"
	"fmt"

	ps "github.com/gomodule/redigo/redis"

	"github.com/whitaker-io/machine"
)

type redis struct {
	client *ps.PubSubConn
}

func (k *redis) Read(ctx context.Context) []machine.Data {
	payload := []machine.Data{}
	packet := machine.Data{}

	switch v := k.client.Receive().(type) {
	case ps.Message:
		if err := json.Unmarshal(v.Data, &packet); err == nil {
			payload = []machine.Data{packet}
		} else if err := json.Unmarshal(v.Data, &payload); err != nil {
			panic(fmt.Sprintf("error unmarshalling from redis - %v", err))
		}
	case error:
		panic(fmt.Sprintf("error reading from redis - %v", v))
	}

	return payload
}

func (k *redis) Close() error {
	return k.client.Close()
}

// New func to provide a machine.Subscription based on Redis
func New(pool *ps.Pool) machine.Subscription {
	return &redis{
		client: &ps.PubSubConn{
			Conn: pool.Get(),
		},
	}
}
