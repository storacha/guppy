package client

import (
	"net/url"

	uclient "github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/principal"
	"github.com/storacha/guppy/pkg/agentdata"
)

// Option is an option configuring a Client.
type Option func(c *Client) error

// WithConnection configures the connection for the client to use. If one is
// not provided, the default connection will be used.
func WithConnection(conn uclient.Connection) Option {
	return func(c *Client) error {
		c.connection = conn
		return nil
	}
}

// WithReceiptsURL configures the URL for the client to fetch receipts from. If
// one is not provided, the default receipts URL will be used.
func WithReceiptsURL(receiptsURL *url.URL) Option {
	return func(c *Client) error {
		c.receiptsURL = receiptsURL
		return nil
	}
}

// WithPrincipal configures the principal for the client to use. If one is
// not provided, a new principal will be generated.
func WithPrincipal(principal principal.Signer) Option {
	return func(c *Client) error {
		c.data.Principal = principal
		return nil
	}
}

// WithData configures the agent data for the client to use. If one is not
// provided, a new agent data will be created with a new principal.
func WithData(data agentdata.AgentData) Option {
	return func(c *Client) error {
		c.data = data
		return nil
	}
}

// WithSaveFn configures the save function for the client to use. This
// function will be called to save the agent data whenever it changes. If
// one is not provided, saving will be silently ignored.
func WithSaveFn(saveFn func(agentdata.AgentData) error) Option {
	return func(c *Client) error {
		c.saveFn = saveFn
		return nil
	}
}
