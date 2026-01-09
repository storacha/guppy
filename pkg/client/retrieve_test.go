package client_test

import (
	"fmt"
	"io"
	"net/url"
	"testing"

	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-libstoracha/blobindex"
	assertcap "github.com/storacha/go-libstoracha/capabilities/assert"
	captypes "github.com/storacha/go-libstoracha/capabilities/types"
	"github.com/storacha/go-libstoracha/digestutil"
	rclient "github.com/storacha/go-ucanto/client/retrieval"
	"github.com/storacha/go-ucanto/core/delegation"
	ed25519signer "github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/guppy/pkg/client/locator"
	"github.com/storacha/guppy/pkg/client/testutil"
	"github.com/stretchr/testify/require"
)

func TestRetrieve(t *testing.T) {
	t.Run("successfully retrieves content", func(t *testing.T) {
		// Setup test data
		testData := []byte("Hello, world! This is test data for retrieval.")
		dataHash, err := multihash.Sum(testData, multihash.SHA2_256, -1)
		require.NoError(t, err)

		// Create space and storage provider
		space, err := ed25519signer.Generate()
		require.NoError(t, err)

		storageProvider, err := ed25519signer.Generate()
		require.NoError(t, err)

		// Create in-process retrieval server with custom HTTP client
		httpClient := testutil.NewRetrievalClient(t, storageProvider, testData)

		// Use URL with hash in path
		serverURL, err := url.Parse(fmt.Sprintf("https://storage1.example.com/blob/%s", digestutil.Format(dataHash)))
		require.NoError(t, err)

		// Create location commitment
		locationCommitment := ucan.NewCapability(
			assertcap.Location.Can(),
			storageProvider.DID().String(),
			assertcap.LocationCaveats{
				Space:   space.DID(),
				Content: captypes.FromHash(dataHash),
				Range: &assertcap.Range{
					Offset: 0,
					Length: uint64Ptr(uint64(len(testData))),
				},
				Location: []url.URL{*serverURL},
			},
		)

		// Create client with space delegation
		c, err := testutil.Client()
		require.NoError(t, err)

		cap := ucan.NewCapability("*", space.DID().String(), ucan.NoCaveats{})
		proof, err := delegation.Delegate(space, c.Issuer(), []ucan.Capability[ucan.NoCaveats]{cap}, delegation.WithNoExpiration())
		require.NoError(t, err)
		err = c.AddProofs(proof)
		require.NoError(t, err)

		location := locator.Location{
			Commitment: locationCommitment,
			Position: blobindex.Position{
				Offset: 0,
				Length: uint64(len(testData)),
			},
		}

		// Retrieve the content using the custom HTTP client
		dataReader, err := c.Retrieve(testContext(t), []locator.Location{location}, rclient.WithClient(httpClient))
		require.NoError(t, err)
		data, err := io.ReadAll(dataReader)
		require.NoError(t, err)
		dataReader.Close()
		require.Equal(t, testData, data)
	})

	t.Run("handles invalid storage provider DID", func(t *testing.T) {
		// Create space
		space, err := ed25519signer.Generate()
		require.NoError(t, err)

		testData := []byte("test data")
		dataHash, err := multihash.Sum(testData, multihash.SHA2_256, -1)
		require.NoError(t, err)

		// Create location commitment with invalid DID
		locationCommitment := ucan.NewCapability(
			assertcap.Location.Can(),
			"not-a-valid-did",
			assertcap.LocationCaveats{
				Space:   space.DID(),
				Content: captypes.FromHash(dataHash),
				Range: &assertcap.Range{
					Offset: 0,
					Length: uint64Ptr(uint64(len(testData))),
				},
				Location: testutil.Urls("https://example.com"),
			},
		)

		// Create client
		c, err := testutil.Client()
		require.NoError(t, err)

		cap := ucan.NewCapability("*", space.DID().String(), ucan.NoCaveats{})
		proof, err := delegation.Delegate(space, c.Issuer(), []ucan.Capability[ucan.NoCaveats]{cap}, delegation.WithNoExpiration())
		require.NoError(t, err)
		err = c.AddProofs(proof)
		require.NoError(t, err)

		location := locator.Location{
			Commitment: locationCommitment,
			Position: blobindex.Position{
				Offset: 0,
				Length: uint64(len(testData)),
			},
		}

		// Retrieve should fail due to invalid DID (no HTTP client needed since it fails before connection)
		_, err = c.Retrieve(testContext(t), []locator.Location{location})
		require.Error(t, err)
		require.Contains(t, err.Error(), "parsing DID")
	})

	t.Run("handles connection errors", func(t *testing.T) {
		// Create space and storage provider
		space, err := ed25519signer.Generate()
		require.NoError(t, err)

		storageProvider, err := ed25519signer.Generate()
		require.NoError(t, err)

		// Use an invalid URL that will fail to connect
		badURL, err := url.Parse("http://localhost:99999")
		require.NoError(t, err)

		testData := []byte("test data")
		dataHash, err := multihash.Sum(testData, multihash.SHA2_256, -1)
		require.NoError(t, err)

		// Create location commitment with bad URL
		locationCommitment := ucan.NewCapability(
			assertcap.Location.Can(),
			storageProvider.DID().String(),
			assertcap.LocationCaveats{
				Space:   space.DID(),
				Content: captypes.FromHash(dataHash),
				Range: &assertcap.Range{
					Offset: 0,
					Length: uint64Ptr(uint64(len(testData))),
				},
				Location: []url.URL{*badURL},
			},
		)

		// Create client
		c, err := testutil.Client()
		require.NoError(t, err)

		cap := ucan.NewCapability("*", space.DID().String(), ucan.NoCaveats{})
		proof, err := delegation.Delegate(space, c.Issuer(), []ucan.Capability[ucan.NoCaveats]{cap}, delegation.WithNoExpiration())
		require.NoError(t, err)
		err = c.AddProofs(proof)
		require.NoError(t, err)

		location := locator.Location{
			Commitment: locationCommitment,
			Position: blobindex.Position{
				Offset: 0,
				Length: uint64(len(testData)),
			},
		}

		// Retrieve should fail due to connection error (use default HTTP client which will fail to connect)
		_, err = c.Retrieve(testContext(t), []locator.Location{location})
		require.Error(t, err)
	})
}

func uint64Ptr(v uint64) *uint64 {
	return &v
}
