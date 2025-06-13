package helpers

import (
	uclient "github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/did"
	ed25519 "github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/principal/signer"
	"github.com/storacha/go-ucanto/server"
	uhelpers "github.com/storacha/go-ucanto/testing/helpers"
)

// NewServerConnection creates a new Ucanto server and a connection to it. It
// accepts `server.Option`s to configure the server. This is mainly (if not
// exclusively) to provide service methods.
//
// The server generates its own service principal. It has a `did:web:` DID for
// realism and readability in errors and failures, but calling code should use
// `server.ID()` to get it rather than assume knowledge of the DID it picks.
func NewServerConnection(options ...server.Option) uclient.Connection {
	servicePrincipal := uhelpers.Must(signer.Wrap(
		uhelpers.Must(ed25519.Generate()),
		uhelpers.Must(did.Parse("did:web:storage.example.com")),
	))

	server := uhelpers.Must(server.NewServer(servicePrincipal, options...))
	connection := uhelpers.Must(uclient.NewConnection(server.ID(), server))

	return connection
}
