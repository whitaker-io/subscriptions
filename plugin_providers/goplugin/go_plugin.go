package goplugin

import (
	"fmt"
	"plugin"

	"github.com/whitaker-io/machine"
)

type goPluginProvider struct{}

func (g *goPluginProvider) Load(pd *machine.PluginDefinition) (interface{}, error) {
	p, err := plugin.Open(pd.Payload)

	if err != nil {
		return nil, fmt.Errorf("error opening plugin %w", err)
	}

	sym, err := p.Lookup(pd.Symbol)

	if err != nil {
		return nil, fmt.Errorf("error looking up symbol %w", err)
	}

	return sym, nil
}

func init() {
	machine.RegisterPluginProvider("goplugin", &goPluginProvider{})
}
