package hashicorp

import (
	"fmt"
	"net/rpc"

	"github.com/hashicorp/go-plugin"
	"github.com/whitaker-io/machine"
)

type Fold interface {
	Fold(aggregate, data machine.Data) machine.Data
}

type FoldRPC struct{ client *rpc.Client }

func (g *FoldRPC) Fold(aggregate, data machine.Data) machine.Data {
	var resp machine.Data
	err := g.client.Call("Plugin.Fold", []machine.Data{aggregate, data}, &resp)
	if err != nil {
		panic(err)
	}

	return resp
}

type FoldRPCServer struct {
	Impl Fold
}

func (s *FoldRPCServer) Fold(args interface{}, resp *machine.Data) error {
	var arguments []machine.Data
	var ok bool
	if arguments, ok = args.([]machine.Data); !ok {
		panic(fmt.Errorf("invalid fold arguments"))
	}

	*resp = s.Impl.Fold(arguments[0], arguments[1])
	return nil
}

type FoldPlugin struct {
	Impl Fold
}

func (p *FoldPlugin) Server(*plugin.MuxBroker) (interface{}, error) {
	return &FoldRPCServer{Impl: p.Impl}, nil
}

func (FoldPlugin) Client(b *plugin.MuxBroker, c *rpc.Client) (interface{}, error) {
	return &FoldRPC{client: c}, nil
}
