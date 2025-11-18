package testutil

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/multiformats/go-multihash"
	contentcap "github.com/storacha/go-libstoracha/capabilities/space/content"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/receipt/fx"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/core/result/failure"
	"github.com/storacha/go-ucanto/principal"
	"github.com/storacha/go-ucanto/server"
	serverretrieval "github.com/storacha/go-ucanto/server/retrieval"
	thttp "github.com/storacha/go-ucanto/transport/http"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/stretchr/testify/require"
)

// RetrievalClientOption configures a retrieval client.
type RetrievalClientOption func(*retrievalClientConfig)

type retrievalClientConfig struct {
	skipHashValidation bool
}

// WithoutHashValidation disables server-side hash validation.
// This is useful for testing client-side validation.
func WithoutHashValidation() RetrievalClientOption {
	return func(cfg *retrievalClientConfig) {
		cfg.skipHashValidation = true
	}
}

// NewRetrievalClient creates an in-process retrieval server and returns an HTTP client
// that can connect to it directly (without network I/O). By default, the server validates that:
// - URL hash (from path /blob/<hash>)
// - Capability hash (from invocation)
// - Actual data hash
// all match before serving the content.
//
// Use WithoutHashValidation() to disable hash validation for testing client-side validation.
func NewRetrievalClient(t *testing.T, service principal.Signer, testData []byte, opts ...RetrievalClientOption) *http.Client {
	cfg := retrievalClientConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	retrievalServer, err := serverretrieval.NewServer(
		service,
		serverretrieval.WithServiceMethod(
			contentcap.Retrieve.Can(),
			serverretrieval.Provide(
				contentcap.Retrieve,
				func(ctx context.Context, cap ucan.Capability[contentcap.RetrieveCaveats], inv invocation.Invocation, ictx server.InvocationContext, req serverretrieval.Request) (result.Result[contentcap.RetrieveOk, failure.IPLDBuilderFailure], fx.Effects, serverretrieval.Response, error) {
					nb := cap.Nb()
					resultValue := result.Ok[contentcap.RetrieveOk, failure.IPLDBuilderFailure](contentcap.RetrieveOk{})

					// Only validate hashes if not configured to skip validation
					if !cfg.skipHashValidation {
						// Compute the actual hash of the test data
						actualHash, err := multihash.Sum(testData, multihash.SHA2_256, -1)
						if err != nil {
							return nil, nil, serverretrieval.Response{Status: http.StatusInternalServerError}, fmt.Errorf("hashing test data: %w", err)
						}

						// Extract hash from URL path (format: /blob/<hash>)
						urlPath := req.URL.Path
						if len(urlPath) < 7 || urlPath[:6] != "/blob/" {
							return nil, nil, serverretrieval.Response{Status: http.StatusBadRequest}, fmt.Errorf("invalid URL path: expected /blob/<hash>, got %s", urlPath)
						}
						urlHashStr := urlPath[6:] // Remove "/blob/" prefix

						// Parse the URL hash
						urlHash, err := multihash.FromB58String(urlHashStr)
						if err != nil {
							return nil, nil, serverretrieval.Response{Status: http.StatusBadRequest}, fmt.Errorf("parsing URL hash: %w", err)
						}

						// Get hash from capability
						capHash := nb.Blob.Digest

						// Validate all three hashes match
						if !bytes.Equal(urlHash, capHash) {
							return nil, nil, serverretrieval.Response{Status: http.StatusBadRequest}, fmt.Errorf("URL hash %s does not match capability hash %s", urlHash.B58String(), capHash.B58String())
						}
						if !bytes.Equal(capHash, actualHash) {
							return nil, nil, serverretrieval.Response{Status: http.StatusBadRequest}, fmt.Errorf("capability hash %s does not match actual data hash %s", capHash.B58String(), actualHash.B58String())
						}
					}

					// Extract range from request
					start := int(nb.Range.Start)
					end := int(nb.Range.End)

					// Validate range
					if start < 0 || end >= len(testData) || start > end {
						return nil, nil, serverretrieval.Response{Status: http.StatusBadRequest}, nil
					}

					length := end - start + 1
					headers := http.Header{}
					headers.Set("Content-Length", fmt.Sprintf("%d", length))
					headers.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(testData)))

					response := serverretrieval.Response{
						Status:  http.StatusPartialContent,
						Headers: headers,
						Body:    io.NopCloser(bytes.NewReader(testData[start : end+1])),
					}
					return resultValue, nil, response, nil
				},
			),
		),
	)
	require.NoError(t, err)

	// Create an HTTP client that connects directly to the server (in-process)
	httpClient := &http.Client{
		Transport: &inProcessRetrievalTransport{server: retrievalServer},
	}

	return httpClient
}

// inProcessRetrievalTransport implements http.RoundTripper to connect directly to a retrieval server
type inProcessRetrievalTransport struct {
	server *serverretrieval.Server
}

func (t *inProcessRetrievalTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Convert http.Request to transport.HTTPRequest
	inboundReq := thttp.NewInboundRequest(req.URL, req.Body, req.Header)

	// Call the server directly
	resp, err := t.server.Request(req.Context(), inboundReq)
	if err != nil {
		return nil, err
	}

	// Convert transport.HTTPResponse to http.Response
	httpResp := &http.Response{
		Status:     http.StatusText(resp.Status()),
		StatusCode: resp.Status(),
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     resp.Headers(),
		Body:       resp.Body(),
		Request:    req,
	}

	return httpResp, nil
}
