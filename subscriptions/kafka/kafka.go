package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	kaf "github.com/segmentio/kafka-go"

	"github.com/whitaker-io/components/utils"
	"github.com/whitaker-io/machine"
)

type readerConfig struct {
	*kaf.ReaderConfig
}

type kafka struct {
	client *kaf.Reader
}

func (k *kafka) Read(ctx context.Context) []machine.Data {
	payload := []machine.Data{}
	packet := machine.Data{}

	if message, err := k.client.ReadMessage(ctx); err != nil {
		panic(fmt.Sprintf("error reading from kafka - %v", err))
	} else if err := json.Unmarshal(message.Value, &packet); err == nil {
		payload = []machine.Data{packet}
	} else if err := json.Unmarshal(message.Value, &payload); err != nil {
		panic(fmt.Sprintf("error unmarshalling from kafka - %v", err))
	}

	return payload
}

func (k *kafka) Close() error {
	return k.client.Close()
}

// New func to provide a machine.Subscription based on Kafka
func New(attributes map[string]interface{}) machine.Subscription {
	r := &readerConfig{}
	r.fromMap(attributes)

	return &kafka{
		client: kaf.NewReader(*r.ReaderConfig),
	}
}

func (r *readerConfig) fromMap(m map[string]interface{}) {
	var ok bool

	if r.Brokers, ok = utils.StringSlice("brokers", m); !ok {
		panic(fmt.Errorf("required field brokers missing"))
	}

	if r.Topic, ok = utils.String("topic", m); !ok {
		panic(fmt.Errorf("required field topic missing"))
	}

	if x, ok := utils.String("group_id", m); ok {
		r.GroupID = x
	}

	if x, ok := utils.Integer("partition", m); ok && r.GroupID == "" {
		r.Partition = x
	}

	if x, ok := utils.Integer("queue_capacity", m); ok {
		r.QueueCapacity = x
	}

	if x, ok := utils.Integer("min_bytes", m); ok {
		r.MinBytes = x
	}

	if x, ok := utils.Integer("max_bytes", m); ok {
		r.MaxBytes = x
	}

	if x, ok := utils.Duration("max_wait", m); ok {
		r.MaxWait = x
	}

	if x, ok := utils.Duration("read_lag_interval", m); ok {
		r.ReadLagInterval = x
	}

	if x, ok := utils.Duration("heartbeat_interval", m); ok {
		r.HeartbeatInterval = x
	}

	if x, ok := utils.Duration("commit_interval", m); ok {
		r.CommitInterval = x
	}

	if x, ok := utils.Boolean("watch_for_partition_changes", m); ok {
		r.WatchPartitionChanges = x
	}

	if x, ok := utils.Duration("partition_watch_interval", m); ok {
		r.PartitionWatchInterval = x
	}

	if x, ok := utils.Duration("session_timeout", m); ok {
		r.SessionTimeout = x
	}

	if x, ok := utils.Duration("rebalance_timeout", m); ok {
		r.RebalanceTimeout = x
	}

	if x, ok := utils.Duration("retention_time", m); ok {
		r.RetentionTime = x
	}

	if x, ok := utils.Integer("start_offset", m); ok {
		r.StartOffset = int64(x)
	}

	if x, ok := utils.Duration("join_group_backoff", m); ok {
		r.JoinGroupBackoff = x
	}

	if x, ok := utils.Duration("read_backoff_min", m); ok {
		r.ReadBackoffMin = x
	}

	if x, ok := utils.Integer("isolation_level", m); ok {
		r.IsolationLevel = kaf.IsolationLevel(x)
	}

	if x, ok := utils.Integer("max_attempts", m); ok {
		r.MaxAttempts = x
	}
}
