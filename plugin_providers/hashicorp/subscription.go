package hashicorp

import (
	"context"
	"fmt"
	"net/rpc"
	"reflect"

	"github.com/hashicorp/go-plugin"
	"github.com/whitaker-io/machine"
)

type SubscriptionRPC struct{ client *rpc.Client }

func (g *SubscriptionRPC) Read(ctx context.Context) []machine.Data {
	var resp []machine.Data
	err := g.client.Call("Plugin.Read", ctx, &resp)
	if err != nil {
		panic(err)
	}

	return resp
}

func (g *SubscriptionRPC) Close() error {
	var resp error
	if err := g.client.Call("Plugin.Read", new(interface{}), &resp); err != nil {
		return err
	}

	return resp
}

type SubscriptionRPCServer struct {
	Impl machine.Subscription
}

func (s *SubscriptionRPCServer) Read(args interface{}, resp *[]machine.Data) error {
	if _, ok := args.(context.Context); !ok {
		return fmt.Errorf("incorrect arguments type %v", reflect.TypeOf(args))
	}

	*resp = s.Impl.Read(args.(context.Context))
	return nil
}

func (s *SubscriptionRPCServer) Close() error {
	return s.Impl.Close()
}

type SubscriptionPlugin struct {
	Impl machine.Subscription
}

func (p *SubscriptionPlugin) Server(*plugin.MuxBroker) (interface{}, error) {
	return &SubscriptionRPCServer{Impl: p.Impl}, nil
}

func (SubscriptionPlugin) Client(b *plugin.MuxBroker, c *rpc.Client) (interface{}, error) {
	return &SubscriptionRPC{client: c}, nil
}
