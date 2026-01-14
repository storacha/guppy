package testutil

import (
	"context"
	"io"
	"net/http"

	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/server"
	"github.com/storacha/guppy/pkg/client"
	"github.com/storacha/guppy/pkg/preparation/storacha"
)

type clientServerConfig struct {
	serverOptions []server.Option
	clientOptions []client.Option
}

type Option func(*clientServerConfig)

// WithServerOptions appends options to the server configuration.
func WithServerOptions(opts ...server.Option) Option {
	return func(c *clientServerConfig) {
		c.serverOptions = append(c.serverOptions, opts...)
	}
}

// WithClientOptions appends options to the client configuration.
func WithClientOptions(opts ...client.Option) Option {
	return func(c *clientServerConfig) {
		c.clientOptions = append(c.clientOptions, opts...)
	}
}

// Client creates an entire [client.Client] with a connection to an in-process
// server, each configured with the given options.
func Client(options ...Option) (*client.Client, error) {
	config := &clientServerConfig{}
	for _, opt := range options {
		opt(config)
	}
	connection := NewTestServerConnection(config.serverOptions...)
	config.clientOptions = append(config.clientOptions, client.WithConnection(connection))
	return client.NewClient(config.clientOptions...)
}

// ComposeOptions combines multiple options into one. It's written generically
// so that it might someday move somewhere more generic, but so far it's only
// being used here anyhow.
func ComposeOptions[C any](opts ...func(C)) func(C) {
	return func(c C) {
		for _, opt := range opts {
			opt(c)
		}
	}
}

// ClientWithCustomPut is a [client.Client] that uses a custom client for PUT
// requests from [SpaceBlobAdd].
type ClientWithCustomPut struct {
	*client.Client
	PutClient *http.Client
}

var _ storacha.Client = (*ClientWithCustomPut)(nil)

func (c *ClientWithCustomPut) SpaceBlobAdd(ctx context.Context, content io.Reader, space did.DID, options ...client.SpaceBlobAddOption) (client.AddedBlob, error) {
	return c.Client.SpaceBlobAdd(ctx, content, space, append(options, client.WithPutClient(c.PutClient))...)
}

// Ptr returns a pointer to the given value.
func Ptr[T any](v T) *T {
	return &v
}
