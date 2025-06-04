package client

import (
	"log"
	"net/url"

	"github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/transport/car"
	"github.com/storacha/go-ucanto/transport/http"
)

var DefaultConnection client.Connection

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
	channel := http.NewHTTPChannel(serviceURL)
	codec := car.NewCAROutboundCodec()

	conn, err := client.NewConnection(servicePrincipal, channel, client.WithOutboundCodec(codec))
	if err != nil {
		log.Fatal(err)
	}

	DefaultConnection = conn
}
