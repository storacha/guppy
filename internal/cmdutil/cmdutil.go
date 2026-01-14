// Package cmdutil provides utility functions specifically for the Guppy CLI.
package cmdutil

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/ucan"
	indexclient "github.com/storacha/indexing-service/pkg/client"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/storacha/guppy/pkg/agentstore"
	"github.com/storacha/guppy/pkg/client"
	"github.com/storacha/guppy/pkg/config"
	receiptclient "github.com/storacha/guppy/pkg/receipt"
)

const defaultServiceName = "up.forge.storacha.network"
const defaultIndexerName = "indexer.forge.storacha.network"

var tracedHttpClient = &http.Client{
	Transport: otelhttp.NewTransport(http.DefaultTransport),
}

// GetClient creates a new client suitable for the CLI, using stored data,
// if any. If proofs are provided, they will be added to the client, but the
// client will not save changes to disk to avoid storing them.
func GetClient(store agentstore.Store, cfg config.ClientConfig) (*client.Client, error) {
	uploadConnection, err := cfg.UploadService.AsConnection(tracedHttpClient)
	if err != nil {
		return nil, err
	}
	receiptsURL, err := url.Parse(fmt.Sprintf("https://%s/receipt", cfg.UploadService.URL))
	if err != nil {
		return nil, err
	}
	c, err := client.NewClient(
		client.WithAgentStore(store),
		client.WithConnection(uploadConnection),
		client.WithReceiptsClient(receiptclient.New(receiptsURL, receiptclient.WithHTTPClient(tracedHttpClient))),
	)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func GetIndexClient(cfg config.Config) (*indexclient.Client, ucan.Principal, error) {
	indexerURL, err := url.Parse(cfg.Indexer.URL)
	if err != nil {
		return nil, nil, err
	}

	indexerPrincipal, err := did.Parse(cfg.Indexer.DID)
	if err != nil {
		return nil, nil, err
	}

	c, err := indexclient.New(indexerPrincipal, *indexerURL, indexclient.WithHTTPClient(tracedHttpClient))
	if err != nil {
		return nil, nil, err
	}

	return c, indexerPrincipal, nil
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
