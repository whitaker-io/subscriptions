package hashicorp

import (
	"fmt"
	"net/rpc"

	"github.com/hashicorp/go-plugin"
	"github.com/whitaker-io/machine"
)

type Fork interface {
	Fork([]*machine.Packet) ([]*machine.Packet, []*machine.Packet)
}

type ForkRPC struct{ client *rpc.Client }

func (g *ForkRPC) Fork(list []*machine.Packet) ([]*machine.Packet, []*machine.Packet) {
	var resp [][]*machine.Packet
	err := g.client.Call("Plugin.Fork", list, &resp)
	if err != nil {
		panic(err)
	}

	return resp[0], resp[1]
}

type ForkRPCServer struct {
	Impl Fork
}

func (s *ForkRPCServer) Fork(args interface{}, resp *[][]*machine.Packet) error {
	if _, ok := args.([]*machine.Packet); !ok {
		panic(fmt.Errorf("invalid fold arguments"))
	}

	a, b := s.Impl.Fork(args.([]*machine.Packet))
	*resp = [][]*machine.Packet{a, b}
	return nil
}

type ForkPlugin struct {
	Impl Fork
}

func (p *ForkPlugin) Server(*plugin.MuxBroker) (interface{}, error) {
	return &ForkRPCServer{Impl: p.Impl}, nil
}

func (ForkPlugin) Client(b *plugin.MuxBroker, c *rpc.Client) (interface{}, error) {
	return &ForkRPC{client: c}, nil
}
