package yaegi

import (
	"fmt"
	"reflect"

	"github.com/traefik/yaegi/interp"
	"github.com/traefik/yaegi/stdlib"
	"github.com/whitaker-io/machine"
)

type yaegiProvider struct{}

func (y *yaegiProvider) Load(pd *machine.PluginDefinition) (interface{}, error) {
	i := interp.New(interp.Options{})
	i.Use(stdlib.Symbols)
	i.Use(symbols)

	if _, err := i.Eval(pd.Payload); err != nil {
		return nil, fmt.Errorf("error evaluating script %w", err)
	}

	sym, err := i.Eval(pd.Symbol)

	if err != nil {
		return nil, fmt.Errorf("error evaluating symbol %w", err)
	}

	if sym.Kind() != reflect.Func {
		return nil, fmt.Errorf("symbol is not of kind func")
	}

	return sym.Interface(), nil
}

func init() {
	machine.RegisterPluginProvider("yaegi", &yaegiProvider{})
}
