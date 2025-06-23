package main

import (
	"context"
	"fmt"

	"github.com/storacha/go-libstoracha/capabilities/upload"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/client"
)

// Error handling omitted for brevity.

func main() {
	ctx := context.Background()

	// space to list uploads from
	space, _ := did.Parse("did:key:z6MkwDuRThQcyWjqNsK54yKAmzfsiH6BTkASyiucThMtHt1y")

	// the account to log in as, which has access to the space
	account, _ := did.Parse("mailto:example.com:ucansam")

	// nil uses the default connection to the Storacha network
	// Without `client.WithPrincipal`, the client will generate a new signer.
	c, _ := client.NewClient(nil)

	// Kick off the login flow
	authOk, _ := c.RequestAccess(ctx, account.String())

	// Start polling to see if the user has authenticated yet
	resultChan := c.PollClaim(ctx, authOk)
	fmt.Println("Please click the link in your email to authenticate...")
	// Wait for the user to authenticate
	proofs, _ := result.Unwrap(<-resultChan)

	// Either add the proofs to the client to use them on any invocation...
	c.AddProofs(proofs...)

	listOk, _ := c.UploadList(
		context.Background(),
		space,
		upload.ListCaveats{},
		// ...Or use them for a single invocation
		proofs...,
	)

	for _, r := range listOk.Results {
		fmt.Printf("%s\n", r.Root)
	}
}
