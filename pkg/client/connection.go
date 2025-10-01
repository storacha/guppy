package client

import (
	"net/url"

	uclient "github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/transport/car"
	"github.com/storacha/go-ucanto/transport/http"
	"github.com/storacha/guppy/pkg/receipt"
)

var DefaultConnection uclient.Connection
var DefaultReceiptsClient *receipt.Client

func init() {
	// service URL & DID
	serviceURL, err := url.Parse("https://up.web3.storage")
	if err != nil {
		log.Fatal(err)
	}

	servicePrincipal, err := did.Parse("did:web:web3.storage")
	if err != nil {
		log.Fatal(err)
	}

	// HTTP transport and CAR encoding
	channel := http.NewChannel(serviceURL)
	codec := car.NewOutboundCodec()

	conn, err := uclient.NewConnection(servicePrincipal, channel, uclient.WithOutboundCodec(codec))
	if err != nil {
		log.Fatal(err)
	}

	DefaultConnection = conn
	defaultReceiptsURL := serviceURL.JoinPath("receipt")
	DefaultReceiptsClient = receipt.New(defaultReceiptsURL)
}
