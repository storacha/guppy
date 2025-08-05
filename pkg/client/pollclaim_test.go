package client_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ipld/go-ipld-prime/datamodel"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/storacha/go-libstoracha/capabilities/access"
	uploadcap "github.com/storacha/go-libstoracha/capabilities/upload"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/core/receipt/fx"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/core/result/failure"
	"github.com/storacha/go-ucanto/server"
	uhelpers "github.com/storacha/go-ucanto/testing/helpers"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/guppy/pkg/client"
	"github.com/storacha/guppy/pkg/client/testutil"
	"github.com/stretchr/testify/require"
)

type factBuilder map[string]ipld.Builder

func (fs factBuilder) ToIPLD() (map[string]datamodel.Node, error) {
	result := make(map[string]datamodel.Node)
	for k, v := range fs {
		vn, err := v.ToIPLD()
		if err != nil {
			return nil, err
		}
		result[k] = vn
	}
	return result, nil
}

type linkBuilder struct {
	link ipld.Link
}

func (l linkBuilder) ToIPLD() (datamodel.Node, error) {
	return basicnode.NewLink(l.link), nil
}

func TestPollClaim(t *testing.T) {
	var responses []result.Result[access.ClaimOk, failure.IPLDBuilderFailure]
	var c *client.Client

	claimedChan := make(chan struct{})
	defer close(claimedChan)

	connection := testutil.NewTestServerConnection(
		server.WithServiceMethod(
			access.Claim.Can(),
			server.Provide(
				access.Claim,
				func(
					ctx context.Context,
					cap ucan.Capability[access.ClaimCaveats],
					inv invocation.Invocation,
					context server.InvocationContext,
				) (result.Result[access.ClaimOk, failure.IPLDBuilderFailure], fx.Effects, error) {
					var response result.Result[access.ClaimOk, failure.IPLDBuilderFailure]
					if len(responses) == 0 {
						return nil, nil, fmt.Errorf("no more responses available")
					}
					response, responses = responses[0], responses[1:]
					return response, nil, nil
				},
			),
		),
	)

	c = uhelpers.Must(client.NewClient(connection, nil))

	requestLink := uhelpers.RandomCID()

	unrelatedDel := uhelpers.Must(uploadcap.Get.Delegate(
		c.Issuer(),
		c.Issuer(),
		c.Issuer().DID().String(),
		uploadcap.GetCaveats{Root: uhelpers.RandomCID()},
	))
	relatedDel := uhelpers.Must(uploadcap.Get.Delegate(
		c.Issuer(),
		c.Issuer(),
		c.Issuer().DID().String(),
		uploadcap.GetCaveats{Root: uhelpers.RandomCID()},
		delegation.WithFacts([]ucan.FactBuilder{factBuilder{
			"access/request": linkBuilder{link: requestLink},
		}}),
	))

	t.Run("polls until it finds authorized delegations", func(t *testing.T) {
		responses = []result.Result[access.ClaimOk, failure.IPLDBuilderFailure]{
			result.Ok[access.ClaimOk, failure.IPLDBuilderFailure](access.ClaimOk{Delegations: buildDelegationsModel()}),
			result.Ok[access.ClaimOk, failure.IPLDBuilderFailure](access.ClaimOk{Delegations: buildDelegationsModel(unrelatedDel)}),
			result.Ok[access.ClaimOk, failure.IPLDBuilderFailure](access.ClaimOk{Delegations: buildDelegationsModel(unrelatedDel, relatedDel)}),
		}

		// A channel of three ticks, ready to read
		tickChan := make(chan time.Time, 3)
		tickChan <- time.Now()
		tickChan <- time.Now()
		tickChan <- time.Now()

		resultChan := c.PollClaimWithTick(testContext(t), access.AuthorizeOk{
			Request:    requestLink,
			Expiration: 0,
		}, tickChan)

		claimedDels, err := result.Unwrap(<-resultChan)
		require.NoError(t, err, "expected no error from PollClaim")
		require.Len(t, claimedDels, 1, "expected exactly one delegation to be claimed")
		require.Equal(t, relatedDel.Link().String(), claimedDels[0].Link().String(), "expected the claimed delegation to be only the related one")

		_, ok := <-resultChan
		require.False(t, ok, "expected result channel to be closed after claim")
	})

	t.Run("reports an error during claim", func(t *testing.T) {
		responses = []result.Result[access.ClaimOk, failure.IPLDBuilderFailure]{
			result.Error[access.ClaimOk](failure.FromError(fmt.Errorf("Something went wrong!"))),
		}

		// A channel of a tick, ready to read
		tickChan := make(chan time.Time, 1)
		tickChan <- time.Now()

		resultChan := c.PollClaimWithTick(testContext(t), access.AuthorizeOk{
			Request:    requestLink,
			Expiration: 0,
		}, tickChan)

		claimedDels, err := result.Unwrap(<-resultChan)

		require.Empty(t, claimedDels, "expected no delegations to be claimed due to context cancelation")
		require.ErrorContains(t, err, "Something went wrong!", "expected error from PollClaim")

		_, ok := <-resultChan
		require.False(t, ok, "expected result channel to be closed after context cancelation")
	})

	t.Run("respects the context's cancelation", func(t *testing.T) {
		// A channel that will never tick
		tickChan := make(chan time.Time)
		ctx, cancel := context.WithCancel(testContext(t))

		resultChan := c.PollClaimWithTick(ctx, access.AuthorizeOk{
			Request:    requestLink,
			Expiration: 0,
		}, tickChan)

		// Cancel the context to simulate a timeout
		cancel()

		claimedDels, err := result.Unwrap(<-resultChan)

		require.Empty(t, claimedDels, "expected no delegations to be claimed due to context cancelation")
		require.ErrorContains(t, err, "context canceled", "expected context cancelation error from PollClaim")

		_, ok := <-resultChan
		require.False(t, ok, "expected result channel to be closed after context cancelation")
	})
}
