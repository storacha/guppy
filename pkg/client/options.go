package client

import (
	uclient "github.com/storacha/go-ucanto/client"

	"github.com/storacha/guppy/pkg/agentstore"
	"github.com/storacha/guppy/pkg/receipt"
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

// WithReceiptsClient configures the client to use for fetching receipts.
func WithReceiptsClient(receiptsClient *receipt.Client) Option {
	return func(c *Client) error {
		c.receiptsClient = receiptsClient
		return nil
	}
}

func WithAgentStore(store agentstore.Store) Option {
	return func(c *Client) error {
		c.store = store
		return nil
	}
}
