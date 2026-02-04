package presets

import (
	"net/url"

	"github.com/storacha/go-ucanto/did"
)

var (
	forgeIndexerID, _  = did.Parse("did:web:indexer.forge.storacha.network")
	forgeIndexerURL, _ = url.Parse("https://indexer.forge.storacha.network")
	hotIndexerID, _    = did.Parse("did:web:indexer.storacha.network")
	hotIndexerURL, _   = url.Parse("https://indexer.storacha.network")
)

type NetworkInfo struct {
	Name                 string
	IndexerID            did.DID
	IndexerURL           url.URL
	AuthorizedRetrievals bool
}

var Networks = []NetworkInfo{
	{
		Name:                 "forge",
		IndexerID:            forgeIndexerID,
		IndexerURL:           *forgeIndexerURL,
		AuthorizedRetrievals: true,
	},
	{
		Name:                 "hot",
		IndexerID:            hotIndexerID,
		IndexerURL:           *hotIndexerURL,
		AuthorizedRetrievals: false,
	},
}

var DefaultNetwork = Networks[0]
