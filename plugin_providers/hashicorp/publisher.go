package hashicorp

import (
	"fmt"
	"net/rpc"

	"github.com/hashicorp/go-plugin"
	"github.com/whitaker-io/machine"
)

type Publisher interface {
	Send(data []machine.Data) error
}

type PublisherRPC struct{ client *rpc.Client }

func (g *PublisherRPC) Send(data []machine.Data) error {
	var resp error
	err := g.client.Call("Plugin.Publisher", data, &resp)
	if err != nil {
		panic(err)
	}

	return resp
}

type PublisherRPCServer struct {
	Impl Publisher
}

func (s *PublisherRPCServer) Publisher(args interface{}, resp *error) error {
	if _, ok := args.([]machine.Data); !ok {
		panic(fmt.Errorf("invalid fold arguments"))
	}

	*resp = s.Impl.Send(args.([]machine.Data))
	return nil
}

type PublisherPlugin struct {
	Impl Publisher
}

func (p *PublisherPlugin) Server(*plugin.MuxBroker) (interface{}, error) {
	return &PublisherRPCServer{Impl: p.Impl}, nil
}

func (PublisherPlugin) Client(b *plugin.MuxBroker, c *rpc.Client) (interface{}, error) {
	return &PublisherRPC{client: c}, nil
}
