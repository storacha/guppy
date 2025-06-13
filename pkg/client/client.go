package client

import (
	uclient "github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	ed25519 "github.com/storacha/go-ucanto/principal/ed25519/signer"
)

type Client struct {
	issuer     principal.Signer
	connection uclient.Connection
}

func NewClient(connection uclient.Connection) (*Client, error) {
	newPrincipal, err := ed25519.Generate()
	if err != nil {
		return nil, err
	}

	return &Client{
		issuer:     newPrincipal,
		connection: connection,
	}, nil
}

// DID returns the DID of the client.
func (c *Client) DID() did.DID {
	if c.issuer == nil {
		return did.DID{}
	}
	return c.issuer.DID()
}

// Issuer returns the issuing signer of the client.
func (c *Client) Issuer() principal.Signer {
	return c.issuer
}
