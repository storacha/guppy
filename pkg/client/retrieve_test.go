package client_test

import (
	"fmt"
	"net/url"
	"testing"

	"github.com/multiformats/go-multihash"
	assertcap "github.com/storacha/go-libstoracha/capabilities/assert"
	captypes "github.com/storacha/go-libstoracha/capabilities/types"
	rclient "github.com/storacha/go-ucanto/client/retrieval"
	"github.com/storacha/go-ucanto/core/delegation"
	ed25519signer "github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/ucan"
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
		serverURL, err := url.Parse(fmt.Sprintf("https://storage1.example.com/blob/%s", dataHash.B58String()))
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

		// Retrieve the content using the custom HTTP client
		data, err := c.Retrieve(testContext(t), space.DID(), locationCommitment, rclient.WithClient(httpClient))
		require.NoError(t, err)
		require.Equal(t, testData, data)
	})

	t.Run("server validates hash consistency", func(t *testing.T) {
		// Setup test data
		wrongData := []byte("This is the WRONG data!!!")
		wrongHash, err := multihash.Sum(wrongData, multihash.SHA2_256, -1)
		require.NoError(t, err)

		correctData := []byte("This is the correct data.")
		correctHash, err := multihash.Sum(correctData, multihash.SHA2_256, -1)
		require.NoError(t, err)

		// Create space and storage provider
		space, err := ed25519signer.Generate()
		require.NoError(t, err)

		storageProvider, err := ed25519signer.Generate()
		require.NoError(t, err)

		// Create retrieval server with WRONG data
		httpClient := testutil.NewRetrievalClient(t, storageProvider, wrongData)

		// Create a location commitment where:
		// - URL has wrongHash (matching the server's data)
		// - Capability has correctHash (what client expects)
		// This tests that the server rejects requests where URL hash != capability hash
		serverURL, err := url.Parse(fmt.Sprintf("https://storage1.example.com/blob/%s", wrongHash.B58String()))
		require.NoError(t, err)

		locationCommitment := ucan.NewCapability(
			assertcap.Location.Can(),
			storageProvider.DID().String(),
			assertcap.LocationCaveats{
				Space:   space.DID(),
				Content: captypes.FromHash(correctHash),
				Range: &assertcap.Range{
					Offset: 0,
					Length: uint64Ptr(uint64(len(wrongData))),
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

		// Retrieve should fail because server detects URL hash != capability hash
		_, err = c.Retrieve(testContext(t), space.DID(), locationCommitment, rclient.WithClient(httpClient))
		require.Error(t, err)
		require.Contains(t, err.Error(), "400")
	})

	t.Run("client validates received data hash", func(t *testing.T) {
		// Setup test data - we'll create a server that bypasses hash validation
		// to test that the CLIENT properly validates the received data
		wrongData := []byte("This is the WRONG data returned by server")
		correctData := []byte("This is the correct data.")
		correctHash, err := multihash.Sum(correctData, multihash.SHA2_256, -1)
		require.NoError(t, err)

		// Create space and storage provider
		space, err := ed25519signer.Generate()
		require.NoError(t, err)

		storageProvider, err := ed25519signer.Generate()
		require.NoError(t, err)

		// Create retrieval server that will serve wrongData but we'll disable
		// server-side validation by using a custom client that bypasses it
		httpClient := testutil.NewRetrievalClient(t, storageProvider, wrongData, testutil.WithoutHashValidation())

		// Use URL with correct hash
		serverURL, err := url.Parse(fmt.Sprintf("https://storage1.example.com/blob/%s", correctHash.B58String()))
		require.NoError(t, err)

		// Create location commitment with correct hash
		locationCommitment := ucan.NewCapability(
			assertcap.Location.Can(),
			storageProvider.DID().String(),
			assertcap.LocationCaveats{
				Space:   space.DID(),
				Content: captypes.FromHash(correctHash),
				Range: &assertcap.Range{
					Offset: 0,
					Length: uint64Ptr(uint64(len(wrongData))),
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

		// Retrieve should fail because client detects data hash mismatch
		_, err = c.Retrieve(testContext(t), space.DID(), locationCommitment, rclient.WithClient(httpClient))
		require.Error(t, err)
		require.Contains(t, err.Error(), "content hash mismatch")
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

		// Retrieve should fail due to invalid DID (no HTTP client needed since it fails before connection)
		_, err = c.Retrieve(testContext(t), space.DID(), locationCommitment)
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

		// Retrieve should fail due to connection error (use default HTTP client which will fail to connect)
		_, err = c.Retrieve(testContext(t), space.DID(), locationCommitment)
		require.Error(t, err)
	})
}

func uint64Ptr(v uint64) *uint64 {
	return &v
}
