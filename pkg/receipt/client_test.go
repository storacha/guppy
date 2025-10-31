package receipt_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	ucancap "github.com/storacha/go-libstoracha/capabilities/ucan"
	"github.com/storacha/go-libstoracha/testutil"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/message"
	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/go-ucanto/core/receipt/ran"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/core/result/failure"
	"github.com/storacha/go-ucanto/core/result/ok"
	"github.com/storacha/go-ucanto/transport/car/response"
	"github.com/storacha/go-ucanto/ucan"
	receiptclient "github.com/storacha/guppy/pkg/receipt"
	"github.com/stretchr/testify/require"
)

func TestFetch(t *testing.T) {
	t.Run("found", func(t *testing.T) {
		inv, err := invocation.Invoke(
			testutil.Alice,
			testutil.Service,
			ucan.NewCapability(
				"test/receipt",
				testutil.Alice.DID().String(),
				ucan.NoCaveats{},
			),
		)
		require.NoError(t, err)

		rcpt, err := receipt.Issue(
			testutil.Alice,
			result.Ok[ok.Unit, failure.IPLDBuilderFailure](ok.Unit{}),
			ran.FromInvocation(inv),
		)
		require.NoError(t, err)

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			msg, err := message.Build(nil, []receipt.AnyReceipt{rcpt})
			require.NoError(t, err)
			res, err := response.Encode(msg)
			require.NoError(t, err)
			_, err = io.Copy(w, res.Body())
			require.NoError(t, err)
		}))
		defer server.Close()

		endpoint, err := url.Parse(server.URL)
		require.NoError(t, err)

		client := receiptclient.New(endpoint)
		result, err := client.Fetch(t.Context(), inv.Link())
		require.NoError(t, err)
		require.Equal(t, inv.Link(), result.Ran().Link())
	})

	t.Run("found in ucan/conclude invocation", func(t *testing.T) {
		inv, err := invocation.Invoke(
			testutil.Alice,
			testutil.Service,
			ucan.NewCapability(
				"test/receipt",
				testutil.Alice.DID().String(),
				ucan.NoCaveats{},
			),
		)
		require.NoError(t, err)

		rcpt, err := receipt.Issue(
			testutil.Alice,
			result.Ok[ok.Unit, failure.IPLDBuilderFailure](ok.Unit{}),
			ran.FromInvocation(inv),
		)
		require.NoError(t, err)

		ccInv, err := ucancap.Conclude.Invoke(
			testutil.Alice,
			testutil.Service,
			testutil.Alice.DID().String(),
			ucancap.ConcludeCaveats{
				Receipt: rcpt.Root().Link(),
			},
		)
		require.NoError(t, err)

		// attach the receipt to the conclude invocation
		for b, err := range rcpt.Blocks() {
			require.NoError(t, err)
			require.NoError(t, ccInv.Attach(b))
		}

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			msg, err := message.Build([]invocation.Invocation{ccInv}, nil)
			require.NoError(t, err)
			res, err := response.Encode(msg)
			require.NoError(t, err)
			_, err = io.Copy(w, res.Body())
			require.NoError(t, err)
		}))
		defer server.Close()

		endpoint, err := url.Parse(server.URL)
		require.NoError(t, err)

		client := receiptclient.New(endpoint)
		result, err := client.Fetch(t.Context(), inv.Link())
		require.NoError(t, err)
		require.Equal(t, inv.Link(), result.Ran().Link())
	})

	t.Run("found in ucan/conclude receipt", func(t *testing.T) {
		inv, err := invocation.Invoke(
			testutil.Alice,
			testutil.Service,
			ucan.NewCapability(
				"test/receipt",
				testutil.Alice.DID().String(),
				ucan.NoCaveats{},
			),
		)
		require.NoError(t, err)

		rcpt, err := receipt.Issue(
			testutil.Alice,
			result.Ok[ok.Unit, failure.IPLDBuilderFailure](ok.Unit{}),
			ran.FromInvocation(inv),
		)
		require.NoError(t, err)

		ccInv, err := ucancap.Conclude.Invoke(
			testutil.Alice,
			testutil.Service,
			testutil.Alice.DID().String(),
			ucancap.ConcludeCaveats{
				Receipt: rcpt.Root().Link(),
			},
		)
		require.NoError(t, err)

		// attach the receipt to the conclude invocation
		for b, err := range rcpt.Blocks() {
			require.NoError(t, err)
			require.NoError(t, ccInv.Attach(b))
		}

		ccRcpt, err := receipt.Issue(
			testutil.Service,
			result.Ok[ok.Unit, failure.IPLDBuilderFailure](ok.Unit{}),
			ran.FromInvocation(ccInv),
		)
		require.NoError(t, err)

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			msg, err := message.Build(nil, []receipt.AnyReceipt{ccRcpt})
			require.NoError(t, err)
			res, err := response.Encode(msg)
			require.NoError(t, err)
			_, err = io.Copy(w, res.Body())
			require.NoError(t, err)
		}))
		defer server.Close()

		endpoint, err := url.Parse(server.URL)
		require.NoError(t, err)

		client := receiptclient.New(endpoint)
		result, err := client.Fetch(t.Context(), inv.Link())
		require.NoError(t, err)
		require.Equal(t, inv.Link(), result.Ran().Link())
	})

	t.Run("not found", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		endpoint, err := url.Parse(server.URL)
		require.NoError(t, err)

		client := receiptclient.New(endpoint)
		_, err = client.Fetch(t.Context(), testutil.RandomCID(t))
		require.ErrorIs(t, err, receiptclient.ErrNotFound)
	})

	t.Run("error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()

		endpoint, err := url.Parse(server.URL)
		require.NoError(t, err)

		client := receiptclient.New(endpoint)
		_, err = client.Fetch(t.Context(), testutil.RandomCID(t))
		require.Error(t, err)
		require.ErrorContains(t, err, "500")
	})
}

func TestPoll(t *testing.T) {
	inv, err := invocation.Invoke(
		testutil.Alice,
		testutil.Service,
		ucan.NewCapability(
			"test/receipt",
			testutil.Alice.DID().String(),
			ucan.NoCaveats{},
		),
	)
	require.NoError(t, err)

	rcpt, err := receipt.Issue(
		testutil.Alice,
		result.Ok[ok.Unit, failure.IPLDBuilderFailure](ok.Unit{}),
		ran.FromInvocation(inv),
	)
	require.NoError(t, err)

	t.Run("found immediately", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			msg, err := message.Build(nil, []receipt.AnyReceipt{rcpt})
			require.NoError(t, err)
			res, err := response.Encode(msg)
			require.NoError(t, err)
			_, err = io.Copy(w, res.Body())
			require.NoError(t, err)
		}))
		defer server.Close()

		endpoint, err := url.Parse(server.URL)
		require.NoError(t, err)

		client := receiptclient.New(endpoint)
		result, err := client.Poll(t.Context(), inv.Link())
		require.NoError(t, err)
		require.Equal(t, inv.Link(), result.Ran().Link())
	})

	t.Run("found after not found", func(t *testing.T) {
		n := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if n == 0 {
				w.WriteHeader(http.StatusNotFound)
				n++
				return
			}
			msg, err := message.Build(nil, []receipt.AnyReceipt{rcpt})
			require.NoError(t, err)
			res, err := response.Encode(msg)
			require.NoError(t, err)
			_, err = io.Copy(w, res.Body())
			require.NoError(t, err)
		}))
		defer server.Close()

		endpoint, err := url.Parse(server.URL)
		require.NoError(t, err)

		client := receiptclient.New(endpoint)
		result, err := client.Poll(
			t.Context(),
			inv.Link(),
			receiptclient.WithInterval(time.Millisecond),
		)
		require.NoError(t, err)
		require.Equal(t, inv.Link(), result.Ran().Link())
	})

	t.Run("fail after retries", func(t *testing.T) {
		n := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			n++
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		endpoint, err := url.Parse(server.URL)
		require.NoError(t, err)

		client := receiptclient.New(endpoint)
		_, err = client.Poll(
			t.Context(),
			inv.Link(),
			receiptclient.WithInterval(time.Millisecond),
			receiptclient.WithRetries(3),
		)
		require.Error(t, err)
		require.ErrorContains(t, err, "receipt was not found after 4 attempts")
		require.Equal(t, 4, n) // 3 retries = 4 requests
	})

	t.Run("fail when context cancel", func(t *testing.T) {
		n := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			n++
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		endpoint, err := url.Parse(server.URL)
		require.NoError(t, err)

		client := receiptclient.New(endpoint)
		ctx, cancel := context.WithCancel(t.Context())
		go func() {
			time.Sleep(300 * time.Millisecond)
			cancel()
		}()

		_, err = client.Poll(
			ctx,
			inv.Link(),
			receiptclient.WithInterval(time.Millisecond),
			receiptclient.WithRetries(-1), // retry forever!
		)
		require.Error(t, err)
		require.ErrorContains(t, err, "context canceled")
		require.Greater(t, n, 0)
	})
}
