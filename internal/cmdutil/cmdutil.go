// Package cmdutil provides utility functions specifically for the Guppy CLI.
package cmdutil

import (
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	uclient "github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/transport/car"
	uhttp "github.com/storacha/go-ucanto/transport/http"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/guppy/pkg/agentdata"
	"github.com/storacha/guppy/pkg/client"
	cdg "github.com/storacha/guppy/pkg/delegation"
	receiptclient "github.com/storacha/guppy/pkg/receipt"
	indexclient "github.com/storacha/indexing-service/pkg/client"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

const defaultServiceName = "up.forge.storacha.network"
const defaultIndexerName = "indexer.forge.storacha.network"

// envSigner returns a principal.Signer from the environment variable
// GUPPY_PRIVATE_KEY, if any.
func envSigner() (principal.Signer, error) {
	str := os.Getenv("GUPPY_PRIVATE_KEY") // use env var preferably
	if str == "" {
		return nil, nil // no signer in the environment
	}

	return signer.Parse(str)
}

var tracedHttpClient = &http.Client{
	Transport: otelhttp.NewTransport(http.DefaultTransport),
}

// MustGetClient creates a new client suitable for the CLI, using stored data,
// if any. If proofs are provided, they will be added to the client, but the
// client will not save changes to disk to avoid storing them.
func MustGetClient(storePath string, options ...client.Option) *client.Client {
	data, err := agentdata.ReadFromFile(storePath)

	if err != nil {
		// If the file doesn't exist yet, that's fine. The config will be empty
		// until it's written to.
		if !errors.Is(err, fs.ErrNotExist) {
			log.Fatalf("reading agent data: %s", err)
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

	clientOptions = append(clientOptions,
		client.WithSaveFn(func(data agentdata.AgentData) error {
			return data.WriteToFile(storePath)
		}),
	)

	c, err := client.NewClient(
		append(
			clientOptions,
			client.WithConnection(MustGetConnection()),
			client.WithReceiptsClient(receiptclient.New(MustGetReceiptsURL(), receiptclient.WithHTTPClient(tracedHttpClient))),
		)...,
	)
	if err != nil {
		log.Fatalf("creating client: %s", err)
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
	channel := uhttp.NewChannel(serviceURL, uhttp.WithClient(tracedHttpClient))
	codec := car.NewOutboundCodec()

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

func MustGetIndexClient() (*indexclient.Client, ucan.Principal) {
	indexerURLStr := os.Getenv("STORACHA_INDEXING_SERVICE_URL") // use env var preferably
	if indexerURLStr == "" {
		indexerURLStr = fmt.Sprintf("https://%s", defaultIndexerName)
	}

	indexerURL, err := url.Parse(indexerURLStr)
	if err != nil {
		log.Fatal(err)
	}

	indexerDIDStr := os.Getenv("STORACHA_INDEXING_SERVICE_DID")
	if indexerDIDStr == "" {
		indexerDIDStr = fmt.Sprintf("did:web:%s", defaultIndexerName)
	}

	indexerPrincipal, err := did.Parse(indexerDIDStr)
	if err != nil {
		log.Fatal(err)
	}

	client, err := indexclient.New(indexerPrincipal, *indexerURL, indexclient.WithHTTPClient(tracedHttpClient))
	if err != nil {
		log.Fatal(err)
	}

	return client, indexerPrincipal
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

// ParseSize parses a data size string with optional suffix (B, K, M, G).
// Accepts formats like: "1024", "512B", "100K", "50M", "2G". Digits with no
// suffix are interpreted as bytes. Returns the size in bytes.
func ParseSize(s string) (uint64, error) {
	if s == "" {
		return 0, errors.New("data size cannot be empty")
	}

	// Trim any whitespace
	s = strings.TrimSpace(s)

	// Check if it ends with a suffix
	var multiplier uint64 = 1
	var numStr string

	lastChar := strings.ToUpper(s[len(s)-1:])
	switch lastChar {
	case "B":
		multiplier = 1
		numStr = s[:len(s)-1]
	case "K":
		multiplier = 1024
		numStr = s[:len(s)-1]
	case "M":
		multiplier = 1024 * 1024
		numStr = s[:len(s)-1]
	case "G":
		multiplier = 1024 * 1024 * 1024
		numStr = s[:len(s)-1]
	default:
		// No suffix, assume bytes
		numStr = s
	}

	// Parse the numeric part
	num, err := strconv.ParseUint(numStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid shard size format: %w", err)
	}

	// Calculate the final size
	size := num * multiplier

	return size, nil
}

func NewHandledCliError(err error) HandledCliError {
	return HandledCliError{err}
}

// HandledCliError is an error which has already been presented to the user. If
// a HandledCliError is returned from a command, the process should exit with
// a non-zero exit code, but no further error message should be printed.
type HandledCliError struct {
	error
}

func (e HandledCliError) Unwrap() error {
	return e.error
}
