package client

import (
	"context"
	"fmt"

	providercap "github.com/storacha/go-libstoracha/capabilities/provider"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/did"
)

// ProviderAdd invokes the provider/add capability to provision a space with a customer account.
func (c *Client) ProviderAdd(ctx context.Context, customerAccount did.DID, provider did.DID, consumer did.DID) (providercap.AddOk, error) {
	caveats := providercap.AddCaveats{
		Provider: provider.String(),
		Consumer: consumer.String(),
	}

	res, _, err := invokeAndExecute[providercap.AddCaveats, providercap.AddOk](
		ctx,
		c,
		providercap.Add,
		customerAccount.String(),
		caveats,
		providercap.AddOkType(),
	)
	if err != nil {
		return providercap.AddOk{}, fmt.Errorf("invoking and executing `provider/add`: %w", err)
	}

	addOk, failErr := result.Unwrap(res)
	if failErr != nil {
		return providercap.AddOk{}, fmt.Errorf("`provider/add` failed: %w", failErr)
	}

	return addOk, nil
}
