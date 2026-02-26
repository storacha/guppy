// Package cmdutil provides utility functions specifically for the Guppy CLI.
package cmdutil

import (
	"errors"
	"fmt"
	"log"
	"net/http"
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
	"github.com/storacha/guppy/pkg/agentstore"
	"github.com/storacha/guppy/pkg/client"
	cdg "github.com/storacha/guppy/pkg/delegation"
	"github.com/storacha/guppy/pkg/presets"
	receiptclient "github.com/storacha/guppy/pkg/receipt"
	indexclient "github.com/storacha/indexing-service/pkg/client"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

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
// if any. The storePath should be a directory path where agent data will be stored.
func MustGetClient(storePath string, options ...client.Option) *client.Client {
	return MustGetClientForNetwork(storePath, "", options...)
}

// MustGetClientForNetwork is like MustGetClient but allows specifying a network
// configuration by name (which may be empty).
func MustGetClientForNetwork(storePath string, networkName string, options ...client.Option) *client.Client {
	store, err := agentstore.NewFs(storePath)
	if err != nil {
		log.Fatalf("creating agent store: %s", err)
	}

	// Override principal if env var is set
	if s, err := envSigner(); err != nil {
		log.Fatalf("parsing GUPPY_PRIVATE_KEY: %s", err)
	} else if s != nil {
		if err := store.SetPrincipal(s); err != nil {
			log.Fatalf("setting principal: %s", err)
		}
	}

	network := MustGetNetworkConfig(networkName)

	conn, err := uclient.NewConnection(
		network.UploadID,
		uhttp.NewChannel(&network.UploadURL, uhttp.WithClient(tracedHttpClient)),
		uclient.WithOutboundCodec(car.NewOutboundCodec()),
	)
	if err != nil {
		log.Fatal(err)
	}

	c, err := client.NewClient(
		append(
			[]client.Option{client.WithStore(store)},
			append(
				options,
				client.WithConnection(conn),
				client.WithReceiptsClient(receiptclient.New(&network.ReceiptsURL, receiptclient.WithHTTPClient(tracedHttpClient))),
			)...,
		)...,
	)
	if err != nil {
		log.Fatalf("creating client: %s", err)
	}

	return c
}

func MustGetNetworkConfig(name string) presets.NetworkConfig {
	network, err := presets.GetNetworkConfig(name)
	if err != nil {
		log.Fatal(fmt.Errorf("getting network configuration: %w", err))
	}
	return network
}

func MustGetIndexClient() (*indexclient.Client, ucan.Principal) {
	return MustGetIndexClientForNetwork("")
}

func MustGetIndexClientForNetwork(networkName string) (*indexclient.Client, ucan.Principal) {
	network := MustGetNetworkConfig(networkName)

	client, err := indexclient.New(network.IndexerID, network.IndexerURL, indexclient.WithHTTPClient(tracedHttpClient))
	if err != nil {
		log.Fatal(err)
	}

	return client, network.IndexerID
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

// ResolveSpace resolves a space identifier, which can be either a DID or a name.
// If the identifier is a valid DID, it returns that DID directly.
// Otherwise, it looks up the space by name using the provided client.
// Returns an error if the name matches no spaces or multiple spaces.
func ResolveSpace(c *client.Client, identifier string) (did.DID, error) {
	// First, try to parse as a DID
	spaceDID, err := did.Parse(identifier)
	if err == nil {
		return spaceDID, nil
	}

	// Not a valid DID, try to look up by name
	space, err := c.SpaceNamed(identifier)
	if err != nil {
		var notFoundErr client.SpaceNotFoundError
		if errors.As(err, &notFoundErr) {
			return did.DID{}, fmt.Errorf("no space found with name %q", identifier)
		}
		var multipleErr client.MultipleSpacesFoundError
		if errors.As(err, &multipleErr) {
			return did.DID{}, fmt.Errorf("multiple spaces found with name %q; use DID to specify which one", identifier)
		}
		return did.DID{}, err
	}

	return space.DID(), nil
}

// TranslateError translates a technical error into a more user-friendly one.
func TranslateError(err error) error {
	if err == nil {
		return nil
	}

	// If it's already a handled error, don't translate it again.
	var handled HandledCliError
	if errors.As(err, &handled) {
		return err
	}

	msg := err.Error()

	switch {
	case strings.Contains(msg, "expired"):
		return NewHandledCliError(fmt.Errorf("session expired: please run `guppy login` again"))
	case strings.Contains(msg, "insufficient") || strings.Contains(msg, "unauthorized") || strings.Contains(msg, "not authorized"):
		return NewHandledCliError(fmt.Errorf("access denied: you do not have sufficient permissions in this space"))
	case strings.Contains(msg, "missing proof") || strings.Contains(msg, "capability not found"):
		return NewHandledCliError(fmt.Errorf("access denied: missing required authorization for this operation"))
	case strings.Contains(msg, "quota"):
		return NewHandledCliError(fmt.Errorf("storage quota exceeded for this space"))
	}

	return err
}
