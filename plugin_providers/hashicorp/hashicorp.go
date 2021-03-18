package hashicorp

import (
	"fmt"
	"os/exec"

	hcplugin "github.com/hashicorp/go-plugin"
	"github.com/whitaker-io/machine"
)

type hashicorpProvider struct{}

func (hc *hashicorpProvider) Load(pd *machine.PluginDefinition) (interface{}, error) {
	protocolVersion, ok := pd.Attributes["protocol_version"]

	if _, isInt := protocolVersion.(uint); !ok || !isInt {
		return nil, fmt.Errorf("attributes missing protocol_version or bad type")
	}

	magicCookieKey, ok := pd.Attributes["magic_cookie_key"]

	if _, isString := magicCookieKey.(string); !ok || !isString {
		return nil, fmt.Errorf("attributes missing magic_cookie_key or bad type")
	}

	magicCookieValue, ok := pd.Attributes["magic_cookie_value"]

	if _, isString := magicCookieValue.(string); !ok || !isString {
		return nil, fmt.Errorf("attributes missing protocol_version or bad type")
	}

	handshakeConfig := hcplugin.HandshakeConfig{
		ProtocolVersion:  protocolVersion.(uint),
		MagicCookieKey:   magicCookieKey.(string),
		MagicCookieValue: magicCookieValue.(string),
	}

	pluginMap := map[string]hcplugin.Plugin{
		"subscription": &SubscriptionPlugin{},
		"applicative":  &ApplicativePlugin{},
		"fold":         &FoldPlugin{},
		"fork":         &ForkPlugin{},
		"sender":       &PublisherPlugin{},
	}

	client := hcplugin.NewClient(&hcplugin.ClientConfig{
		HandshakeConfig: handshakeConfig,
		Plugins:         pluginMap,
		Cmd:             exec.Command(pd.Payload),
		AllowedProtocols: []hcplugin.Protocol{
			hcplugin.ProtocolGRPC,
		},
	})

	var raw interface{}

	if rpcClient, err := client.Client(); err != nil {
		return nil, err
	} else if raw, err = rpcClient.Dispense(pd.Symbol); err != nil {
		return nil, err
	}

	switch pd.Symbol {
	case "subscription":
		return raw.(machine.Subscription), nil
	case "applicative":
		return raw.(Applicative).Applicative, nil
	case "fold":
		return raw.(Fold).Fold, nil
	case "fork":
		return raw.(Fork).Fork, nil
	case "publisher":
		return raw.(Publisher), nil
	case "retriever":
		return nil, fmt.Errorf("retriever symbol not supported")
	default:
		return nil, fmt.Errorf("invalid symbol %s", pd.Symbol)
	}
}

func init() {
	machine.RegisterPluginProvider("hashicorp", &hashicorpProvider{})
}
