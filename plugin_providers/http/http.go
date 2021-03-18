package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/whitaker-io/machine"
)

type httpProvider struct{}

func (h *httpProvider) Load(pd *machine.PluginDefinition) (interface{}, error) {
	client := clientFromAttributes(pd.Payload, pd.Attributes)

	switch pd.Symbol {
	case "subscription":
		return client.clientFromAttributes(pd.Attributes), nil
	case "applicative":
		return client.clientFromAttributes(pd.Attributes).Applicative, nil
	case "fold":
		return client.clientFromAttributes(pd.Attributes).Fold, nil
	case "fork":
		return client.clientFromAttributes(pd.Attributes).Fork, nil
	case "publisher":
		return client.clientFromAttributes(pd.Attributes), nil
	case "retriever":
		return nil, fmt.Errorf("retriever symbol not supported")
	default:
		return nil, fmt.Errorf("invalid symbol %s", pd.Symbol)
	}
}

func init() {
	machine.RegisterPluginProvider("http", &httpProvider{})
}

type client struct {
	url     string
	headers http.Header
	client  *http.Client
}

func (hs *client) Read(ctx context.Context) []machine.Data {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, hs.url, nil)

	if err != nil {
		panic(err)
	}

	request.Header = hs.headers

	resp, err := hs.client.Do(request)

	if err != nil {
		panic(err)
	}

	defer resp.Body.Close()

	payload := []machine.Data{}
	payloadBytes := []byte{}

	_, err = resp.Body.Read(payloadBytes)

	if err != nil {
		panic(err)
	}

	err = json.Unmarshal(payloadBytes, &payload)

	if err != nil {
		panic(err)
	}

	return payload
}

func (hs *client) Close() error { return nil }

func (hs *client) Applicative(data machine.Data) error {
	bytez, err := json.Marshal(data)

	if err != nil {
		return err
	}

	request, err := http.NewRequest(http.MethodPost, hs.url, bytes.NewBuffer(bytez))

	if err != nil {
		return err
	}

	request.Header = hs.headers

	resp, err := hs.client.Do(request)

	if err != nil {
		return err
	}

	defer resp.Body.Close()

	payload := machine.Data{}
	payloadBytes := []byte{}

	_, err = resp.Body.Read(payloadBytes)

	if err != nil {
		return err
	}

	err = json.Unmarshal(payloadBytes, &payload)

	if err != nil {
		return err
	}

	for k, v := range payload {
		data[k] = v
	}

	for k := range data {
		if _, ok := payload[k]; !ok {
			delete(data, k)
		}
	}

	return nil
}

func (hs *client) Fold(aggregate, next machine.Data) machine.Data {
	bytez, err := json.Marshal(map[string]interface{}{
		"aggregate": aggregate,
		"next":      next,
	})

	if err != nil {
		panic(err)
	}

	request, err := http.NewRequest(http.MethodPost, hs.url, bytes.NewBuffer(bytez))

	if err != nil {
		panic(err)
	}

	request.Header = hs.headers

	resp, err := hs.client.Do(request)

	if err != nil {
		panic(err)
	}

	defer resp.Body.Close()

	payload := machine.Data{}
	payloadBytes := []byte{}

	_, err = resp.Body.Read(payloadBytes)

	if err != nil {
		panic(err)
	}

	err = json.Unmarshal(payloadBytes, &payload)

	if err != nil {
		panic(err)
	}

	return payload
}

func (hs *client) Fork(list []*machine.Packet) ([]*machine.Packet, []*machine.Packet) {
	bytez, err := json.Marshal(list)

	if err != nil {
		panic(err)
	}

	request, err := http.NewRequest(http.MethodPost, hs.url, bytes.NewBuffer(bytez))

	if err != nil {
		panic(err)
	}

	request.Header = hs.headers

	resp, err := hs.client.Do(request)

	if err != nil {
		panic(err)
	}

	defer resp.Body.Close()

	payload := [][]*machine.Packet{}
	payloadBytes := []byte{}

	_, err = resp.Body.Read(payloadBytes)

	if err != nil {
		panic(err)
	}

	err = json.Unmarshal(payloadBytes, &payload)

	if err != nil {
		panic(err)
	}

	if len(payload) != 2 {
		panic(fmt.Errorf("wrong payload size"))
	}

	return payload[0], payload[1]
}

func (hs *client) Send(data []machine.Data) error {
	bytez, err := json.Marshal(data)

	if err != nil {
		return err
	}

	request, err := http.NewRequest(http.MethodPost, hs.url, bytes.NewBuffer(bytez))

	if err != nil {
		return err
	}

	request.Header = hs.headers

	resp, err := hs.client.Do(request)

	if err != nil {
		return err
	}

	defer resp.Body.Close()

	return nil
}

func clientFromAttributes(url string, attributes map[string]interface{}) *client {
	hs := &client{}

	hs.url = url

	if headers, ok := attributes["headers"]; ok {
		if hs.headers, ok = headers.(http.Header); !ok {
			panic(fmt.Errorf("invalid headers type in subscription attributes"))
		}
	} else {
		hs.headers = map[string][]string{}
	}

	hs.client = &http.Client{}

	if timeout, ok := attributes["timeout"]; ok {
		switch val := timeout.(type) {
		case int64:
			hs.client.Timeout = time.Duration(val)
		case int:
			hs.client.Timeout = time.Duration(val)
		case string:
		default:
			panic(fmt.Errorf("invalid timeout type in subscription attributes"))
		}
	} else {
		hs.client.Timeout = 30 * time.Second
	}

	return hs
}

func (hs *client) clientFromAttributes(attributes map[string]interface{}) *client {
	hs2 := &client{
		url:     hs.url,
		headers: hs.headers,
		client: &http.Client{
			Timeout: hs.client.Timeout,
		},
	}

	if headers, ok := attributes["headers"]; ok {
		if hs2.headers, ok = headers.(http.Header); !ok {
			panic(fmt.Errorf("invalid headers type in subscription attributes"))
		}
	}

	hs2.client = &http.Client{}

	if timeout, ok := attributes["timeout"]; ok {
		switch val := timeout.(type) {
		case int64:
			hs2.client.Timeout = time.Duration(val)
		case int:
			hs2.client.Timeout = time.Duration(val)
		case string:
		default:
			panic(fmt.Errorf("invalid timeout type in subscription attributes"))
		}
	}

	return hs2
}
