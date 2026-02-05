package presets

import (
	"fmt"
	"net/url"
	"os"

	logging "github.com/ipfs/go-log/v2"
	"github.com/storacha/go-ucanto/did"
)

var log = logging.Logger("pkg/presets")

var (
	forgeIndexerID, _         = did.Parse("did:web:indexer.forge.storacha.network")
	forgeIndexerURL, _        = url.Parse("https://indexer.forge.storacha.network")
	forgeReceiptsURL, _       = url.Parse("https://up.forge.storacha.network/receipt/")
	forgeUploadID, _          = did.Parse("did:web:up.forge.storacha.network")
	forgeUploadURL, _         = url.Parse("https://up.forge.storacha.network")
	hotIndexerID, _           = did.Parse("did:web:indexer.storacha.network")
	hotIndexerURL, _          = url.Parse("https://indexer.storacha.network")
	hotReceiptsURL, _         = url.Parse("https://up.storacha.network/receipt/")
	hotUploadID, _            = did.Parse("did:web:up.storacha.network")
	hotUploadURL, _           = url.Parse("https://up.storacha.network")
	warmStagingIndexerID, _   = did.Parse("did:web:staging.indexer.warm.storacha.network")
	warmStagingIndexerURL, _  = url.Parse("https://staging.indexer.warm.storacha.network")
	warmStagingReceiptsURL, _ = url.Parse("https://staging.up.warm.storacha.network/receipt/")
	warmStagingUploadID, _    = did.Parse("did:web:staging.up.warm.storacha.network")
	warmStagingUploadURL, _   = url.Parse("https://staging.up.warm.storacha.network")
)

type NetworkConfig struct {
	Name                 string // Name of the network.
	IndexerID            did.DID
	IndexerURL           url.URL
	ReceiptsURL          url.URL
	UploadID             did.DID
	UploadURL            url.URL
	AuthorizedRetrievals bool // Support for UCAN authorized retrievals.
}

// Known network configurations.
var Networks = []NetworkConfig{
	{
		Name:                 "forge",
		UploadID:             forgeUploadID,
		UploadURL:            *forgeUploadURL,
		ReceiptsURL:          *forgeReceiptsURL,
		IndexerID:            forgeIndexerID,
		IndexerURL:           *forgeIndexerURL,
		AuthorizedRetrievals: true,
	},
	{
		Name:                 "hot",
		UploadID:             hotUploadID,
		UploadURL:            *hotUploadURL,
		ReceiptsURL:          *hotReceiptsURL,
		IndexerID:            hotIndexerID,
		IndexerURL:           *hotIndexerURL,
		AuthorizedRetrievals: false,
	},
	{
		Name:                 "warm-staging",
		UploadID:             warmStagingUploadID,
		UploadURL:            *warmStagingUploadURL,
		ReceiptsURL:          *warmStagingReceiptsURL,
		IndexerID:            warmStagingIndexerID,
		IndexerURL:           *warmStagingIndexerURL,
		AuthorizedRetrievals: true,
	},
}

var DefaultNetwork = Networks[0]

// GetNetworkConfig returns the network config for the passed name or the
// STORACHA_NETWORK environment variable if set. If both are empty, the default
// network configuration is returned with overrides from the following
// environment variables:
//   - STORACHA_SERVICE_DID: override the upload service DID
//   - STORACHA_SERVICE_URL: override the upload service URL
//   - STORACHA_RECEIPTS_URL: override the receipts service URL
//   - STORACHA_INDEXING_SERVICE_DID: override the indexing service DID
//   - STORACHA_INDEXING_SERVICE_URL: override the indexing service URL
//   - STORACHA_AUTHORIZED_RETRIEVALS: set authorized retrievals to true if "true"
func GetNetworkConfig(name string) (NetworkConfig, error) {
	// use named network if provided from param or env var
	if name == "" {
		name = os.Getenv("STORACHA_NETWORK")
	}
	if name != "" {
		log.Debugw("using network", "name", name)
		for _, n := range Networks {
			if n.Name == name {
				return n, nil
			}
		}
		return NetworkConfig{}, fmt.Errorf("unknown network: %q", name)
	}

	// otherwise use default network, but override with env vars
	network := DefaultNetwork
	if os.Getenv("STORACHA_SERVICE_DID") != "" {
		log.Debugw("using upload service ID from environment", "did", os.Getenv("STORACHA_SERVICE_DID"))
		id, err := did.Parse(os.Getenv("STORACHA_SERVICE_DID"))
		if err != nil {
			return NetworkConfig{}, fmt.Errorf("parsing %q environment variable: %w", "STORACHA_SERVICE_DID", err)
		}
		network.Name = "custom"
		network.UploadID = id
	}
	if os.Getenv("STORACHA_SERVICE_URL") != "" {
		log.Debugw("using upload service URL from environment", "url", os.Getenv("STORACHA_SERVICE_URL"))
		url, err := url.Parse(os.Getenv("STORACHA_SERVICE_URL"))
		if err != nil {
			return NetworkConfig{}, fmt.Errorf("parsing %q environment variable: %w", "STORACHA_SERVICE_URL", err)
		}
		network.Name = "custom"
		network.UploadURL = *url
	}
	if os.Getenv("STORACHA_RECEIPTS_URL") != "" {
		log.Debugw("using receipts API URL from environment", "url", os.Getenv("STORACHA_RECEIPTS_URL"))
		url, err := url.Parse(os.Getenv("STORACHA_RECEIPTS_URL"))
		if err != nil {
			return NetworkConfig{}, fmt.Errorf("parsing %q environment variable: %w", "STORACHA_RECEIPTS_URL", err)
		}
		network.Name = "custom"
		network.ReceiptsURL = *url
	}
	if os.Getenv("STORACHA_INDEXING_SERVICE_DID") != "" {
		log.Debugw("using indexing service ID from environment", "did", os.Getenv("STORACHA_INDEXING_SERVICE_DID"))
		id, err := did.Parse(os.Getenv("STORACHA_INDEXING_SERVICE_DID"))
		if err != nil {
			return NetworkConfig{}, fmt.Errorf("parsing %q environment variable: %w", "STORACHA_INDEXING_SERVICE_DID", err)
		}
		network.Name = "custom"
		network.IndexerID = id
	}
	if os.Getenv("STORACHA_INDEXING_SERVICE_URL") != "" {
		log.Debugw("using indexing service URL from environment", "url", os.Getenv("STORACHA_INDEXING_SERVICE_URL"))
		url, err := url.Parse(os.Getenv("STORACHA_INDEXING_SERVICE_URL"))
		if err != nil {
			return NetworkConfig{}, fmt.Errorf("parsing %q environment variable: %w", "STORACHA_INDEXING_SERVICE_URL", err)
		}
		network.Name = "custom"
		network.IndexerURL = *url
	}
	if os.Getenv("STORACHA_AUTHORIZED_RETRIEVALS") != "" {
		log.Debugw("using authorized retrievals flag from environment", "value", os.Getenv("STORACHA_AUTHORIZED_RETRIEVALS"))
		network.Name = "custom"
		network.AuthorizedRetrievals = os.Getenv("STORACHA_AUTHORIZED_RETRIEVALS") == "true"
	}
	if network == DefaultNetwork {
		log.Debugw("using default network config", "name", network.Name)
	}
	return network, nil
}
