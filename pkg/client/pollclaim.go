package client

import (
	"context"
	"fmt"
	"time"

	"github.com/storacha/go-libstoracha/capabilities/access"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/core/result"
)

// PollClaim attempts to `access/claim` and retries until it finds delegations
// authorized by way of the given `authOk`. It returns a channel which will
// produce the result and then close.
func (c *Client) PollClaim(ctx context.Context, authOk access.AuthorizeOk) <-chan result.Result[[]delegation.Delegation, error] {
	return c.PollClaimWithTick(ctx, authOk, time.Tick(250*time.Millisecond))
}

// PollClaimWithTick is the same as [PollClaim], but accepts the tick channel
// for timing control over the polling. PollClaimWithTick will poll once for
// each value read on `tickChan`, until the claim succeeds or an error occurs.
func (c *Client) PollClaimWithTick(ctx context.Context, authOk access.AuthorizeOk, tickChan <-chan time.Time) <-chan result.Result[[]delegation.Delegation, error] {
	resultChan := make(chan result.Result[[]delegation.Delegation, error], 1)

	go func() {
		resultChan <- result.Wrap(func() ([]delegation.Delegation, error) {
			return c.pollClaimWithTicker(ctx, authOk, tickChan)
		})
		close(resultChan)
	}()

	return resultChan
}

func (c *Client) pollClaimWithTicker(ctx context.Context, authOk access.AuthorizeOk, tickChan <-chan time.Time) ([]delegation.Delegation, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("context canceled before delegations could be claimed: %w", ctx.Err())
		case <-tickChan:
			dels, err := c.ClaimAccess(ctx)
			if err != nil {
				return nil, fmt.Errorf("failed to claim access: %w", err)
			}

			// Collect all delegations whose "access/request" fact matches the request
			// link in authOk
			relevantDels := make([]delegation.Delegation, 0, len(dels))
			for _, del := range dels {
				for _, fact := range del.Facts() {
					requestLinkValue, ok := fact[access.AuthorizeRequestFactKey]
					if !ok {
						continue // Skip if the fact does not contain "access/request"
					}
					requestLinkValueNode, ok := requestLinkValue.(ipld.Node)
					if !ok {
						continue // Skip if the fact is not a valid IPLD Node for some reason
					}
					requestLink, err := requestLinkValueNode.AsLink()
					if err != nil {
						continue // Skip if the "access/request" fact is not a link
					}
					if requestLink.String() == authOk.Request.String() {
						relevantDels = append(relevantDels, del)
						break // No need to check further facts for this delegation
					}
				}
			}

			if len(relevantDels) > 0 {
				return relevantDels, nil
			}
		}
	}
}
