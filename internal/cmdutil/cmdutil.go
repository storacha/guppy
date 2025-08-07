// Package cmdutil provides utility functions specifically for the Guppy CLI.
package cmdutil

import (
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net/url"
	"os"
	"path"

	uclient "github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/transport/car"
	"github.com/storacha/go-ucanto/transport/http"
	"github.com/storacha/guppy/pkg/agentdata"
	"github.com/storacha/guppy/pkg/client"
	cdg "github.com/storacha/guppy/pkg/delegation"
	receiptclient "github.com/storacha/guppy/pkg/receipt"
)

const defaultServiceName = "staging.up.storacha.network"

// envSigner returns a principal.Signer from the environment variable
// GUPPY_PRIVATE_KEY, if any.
func envSigner() (principal.Signer, error) {
	str := os.Getenv("GUPPY_PRIVATE_KEY") // use env var preferably
	if str == "" {
		return nil, nil // no signer in the environment
	}

	return signer.Parse(str)
}

// MustGetClient creates a new client suitable for the CLI, using stored data,
// if any. If proofs are provided, they will be added to the client, but the
// client will not save changes to disk to avoid storing them.
func MustGetClient(proofs ...delegation.Delegation) *client.Client {
	homedir, err := os.UserHomeDir()
	if err != nil {
		log.Fatalf("obtaining user home directory: %s", err)
	}

	datadir := path.Join(homedir, ".guppy")
	datapath := path.Join(datadir, "config.json")

	data, err := agentdata.ReadFromFile(datapath)

	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			log.Fatalf("reading agent data: %s", err)
		}

		// If the file doesn't exist yet, that's fine, but make sure the directory
		// exists to save into later.
		if err := os.MkdirAll(datadir, 0700); err != nil {
			log.Fatalf("creating data directory: %s", err)
		}
	}

	var clientOptions []client.Option

	// Use the principal from the environment if given.
	if s, err := envSigner(); err != nil {
		log.Fatalf("parsing GUPPY_PRIVATE_KEY: %s", err)
	} else if s != nil {
		// If a principal is provided, use that, and ignore the saved data.
		clientOptions = append(clientOptions, client.WithPrincipal(s))
	} else {
		// Otherwise, read and write the saved data.
		clientOptions = append(clientOptions, client.WithData(data))
	}

	proofsProvided := len(proofs) > 0

	if !proofsProvided {
		// Only enable saving if no proofs are provided
		clientOptions = append(clientOptions,
			client.WithSaveFn(func(data agentdata.AgentData) error {
				return data.WriteToFile(datapath)
			}),
		)
	}

	c, err := client.NewClient(
		append(
			clientOptions,
			client.WithConnection(MustGetConnection()),
			client.WithReceiptsClient(receiptclient.New(MustGetReceiptsURL())),
		)...,
	)
	if err != nil {
		log.Fatalf("creating client: %s", err)
	}

	if proofsProvided {
		c.AddProofs(proofs...)
	}

	return c
}

func MustGetConnection() uclient.Connection {
	// service URL & DID
	serviceURLStr := os.Getenv("STORACHA_SERVICE_URL") // use env var preferably
	if serviceURLStr == "" {
		serviceURLStr = fmt.Sprintf("https://%s", defaultServiceName)
	}

	serviceURL, err := url.Parse(serviceURLStr)
	if err != nil {
		log.Fatal(err)
	}

	serviceDIDStr := os.Getenv("STORACHA_SERVICE_DID")
	if serviceDIDStr == "" {
		serviceDIDStr = fmt.Sprintf("did:web:%s", defaultServiceName)
	}

	servicePrincipal, err := did.Parse(serviceDIDStr)
	if err != nil {
		log.Fatal(err)
	}

	// HTTP transport and CAR encoding
	channel := http.NewHTTPChannel(serviceURL)
	codec := car.NewCAROutboundCodec()

	conn, err := uclient.NewConnection(servicePrincipal, channel, uclient.WithOutboundCodec(codec))
	if err != nil {
		log.Fatal(err)
	}

	return conn
}

func MustGetReceiptsURL() *url.URL {
	receiptsURLStr := os.Getenv("STORACHA_RECEIPTS_URL")
	if receiptsURLStr == "" {
		receiptsURLStr = fmt.Sprintf("https://%s/receipt", defaultServiceName)
	}

	receiptsURL, err := url.Parse(receiptsURLStr)
	if err != nil {
		log.Fatal(err)
	}

	return receiptsURL
}

func MustParseDID(str string) did.DID {
	did, err := did.Parse(str)
	if err != nil {
		log.Fatalf("parsing DID: %s", err)
	}
	return did
}

func MustGetProof(path string) delegation.Delegation {
	b, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("reading proof file: %s", err)
	}

	proof, err := cdg.ExtractProof(b)
	if err != nil {
		log.Fatalf("extracting proof: %s", err)
	}
	return proof
}
