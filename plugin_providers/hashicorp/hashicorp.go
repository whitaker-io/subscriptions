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

	providerType, ok := pd.Attributes["provider_type"]

	if _, isString := providerType.(string); !ok || !isString {
		return nil, fmt.Errorf("attributes missing provider_type or bad type")
	}

	handshakeConfig := hcplugin.HandshakeConfig{
		ProtocolVersion:  protocolVersion.(uint),
		MagicCookieKey:   magicCookieKey.(string),
		MagicCookieValue: magicCookieValue.(string),
	}

	pluginMap := map[string]hcplugin.Plugin{}

	switch providerType.(string) {
	case "subscription":
		pluginMap["subscription"] = &SubscriptionPlugin{}
	case "retriever":
		pluginMap["retriever"] = &RetrieverPlugin{}
	case "applicative":
		pluginMap["applicative"] = &ApplicativePlugin{}
	case "fold":
		pluginMap["fold"] = &FoldPlugin{}
	case "fork":
		pluginMap["fork"] = &ForkPlugin{}
	case "sender":
		pluginMap["sender"] = &SenderPlugin{}
	default:
		return nil, fmt.Errorf("invalid provider_type %s", providerType.(string))
	}

	client := hcplugin.NewClient(&hcplugin.ClientConfig{
		HandshakeConfig: handshakeConfig,
		Plugins:         pluginMap,
		Cmd:             exec.Command(pd.Payload),
	})

	rpcClient, err := client.Client()
	if err != nil {
		return nil, err
	}

	raw, err := rpcClient.Dispense(pd.Symbol)
	if err != nil {
		return nil, err
	}

	switch providerType.(string) {
	case "subscription":
		return func(map[string]interface{}) machine.Subscription {
			return raw.(machine.Subscription)
		}, nil
	case "retriever":
		return func(map[string]interface{}) machine.Retriever {
			return raw.(Retriever).Retriever
		}, nil
	case "applicative":
		return func(map[string]interface{}) machine.Applicative {
			return raw.(Applicative).Applicative
		}, nil
	case "fold":
		return func(map[string]interface{}) machine.Fold {
			return raw.(Fold).Fold
		}, nil
	case "fork":
		return func(map[string]interface{}) machine.Fork {
			return raw.(Fork).Fork
		}, nil
	case "sender":
		return func(map[string]interface{}) machine.Sender {
			return raw.(Sender).Sender
		}, nil
	default:
		return nil, fmt.Errorf("invalid provider_type %s", providerType.(string))
	}
}

func init() {
	machine.RegisterPluginProvider("hashicorp", &hashicorpProvider{})
}
