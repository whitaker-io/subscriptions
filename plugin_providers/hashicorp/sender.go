package hashicorp

import (
	"fmt"
	"net/rpc"

	"github.com/hashicorp/go-plugin"
	"github.com/whitaker-io/machine"
)

type Sender interface {
	Sender(data []machine.Data) error
}

type SenderRPC struct{ client *rpc.Client }

func (g *SenderRPC) Sender(data []machine.Data) error {
	var resp error
	err := g.client.Call("Plugin.Sender", data, &resp)
	if err != nil {
		panic(err)
	}

	return resp
}

type SenderRPCServer struct {
	Impl Sender
}

func (s *SenderRPCServer) Sender(args interface{}, resp *error) error {
	if _, ok := args.([]machine.Data); !ok {
		panic(fmt.Errorf("invalid fold arguments"))
	}

	*resp = s.Impl.Sender(args.([]machine.Data))
	return nil
}

type SenderPlugin struct {
	Impl Sender
}

func (p *SenderPlugin) Server(*plugin.MuxBroker) (interface{}, error) {
	return &SenderRPCServer{Impl: p.Impl}, nil
}

func (SenderPlugin) Client(b *plugin.MuxBroker, c *rpc.Client) (interface{}, error) {
	return &SenderRPC{client: c}, nil
}
