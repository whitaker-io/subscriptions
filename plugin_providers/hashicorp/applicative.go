package hashicorp

import (
	"fmt"
	"net/rpc"
	"reflect"

	"github.com/hashicorp/go-plugin"
	"github.com/whitaker-io/machine"
)

type Applicative interface {
	Applicative(data machine.Data) error
}

type ApplicativeRPC struct{ client *rpc.Client }

func (g *ApplicativeRPC) Applicative(data machine.Data) error {
	var resp error
	err := g.client.Call("Plugin.Applicative", data, &resp)
	if err != nil {
		panic(err)
	}
	return resp
}

type ApplicativeRPCServer struct {
	Impl Applicative
}

func (s *ApplicativeRPCServer) Applicative(args interface{}, resp *error) error {
	if _, ok := args.(machine.Data); !ok {
		return fmt.Errorf("incorrect arguments type %v", reflect.TypeOf(args))
	}

	*resp = s.Impl.Applicative(args.(machine.Data))
	return nil
}

type ApplicativePlugin struct {
	Impl Applicative
}

func (p *ApplicativePlugin) Server(*plugin.MuxBroker) (interface{}, error) {
	return &ApplicativeRPCServer{Impl: p.Impl}, nil
}

func (ApplicativePlugin) Client(b *plugin.MuxBroker, c *rpc.Client) (interface{}, error) {
	return &ApplicativeRPC{client: c}, nil
}
