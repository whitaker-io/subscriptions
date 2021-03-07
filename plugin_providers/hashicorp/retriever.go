package hashicorp

import (
	"context"
	"fmt"
	"net/rpc"
	"reflect"

	"github.com/hashicorp/go-plugin"
	"github.com/whitaker-io/machine"
)

type Retriever interface {
	Retriever(ctx context.Context) chan []machine.Data
}

type RetrieverRPC struct{ client *rpc.Client }

func (g *RetrieverRPC) Retriever(ctx context.Context) chan []machine.Data {
	var resp chan []machine.Data
	err := g.client.Call("Plugin.Retriever", ctx, &resp)
	if err != nil {
		panic(err)
	}

	return resp
}

type RetrieverRPCServer struct {
	Impl Retriever
}

func (s *RetrieverRPCServer) Read(args interface{}, resp *chan []machine.Data) error {
	if _, ok := args.(context.Context); !ok {
		return fmt.Errorf("incorrect arguments type %v", reflect.TypeOf(args))
	}

	*resp = s.Impl.Retriever(args.(context.Context))
	return nil
}

type RetrieverPlugin struct {
	Impl Retriever
}

func (p *RetrieverPlugin) Server(*plugin.MuxBroker) (interface{}, error) {
	return &RetrieverRPCServer{Impl: p.Impl}, nil
}

func (RetrieverPlugin) Client(b *plugin.MuxBroker, c *rpc.Client) (interface{}, error) {
	return &RetrieverRPC{client: c}, nil
}
