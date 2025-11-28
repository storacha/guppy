package main

import (
	"context"
	"fmt"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/client"
)

// Error handling omitted for brevity.

func main() {
	ctx := context.Background()

	// the account to log in as, which has access to the space
	account, err := did.Parse("did:mailto:gmail.com:sambhavjain170944")
	if err != nil {
		panic(err) // handle error properly
	}

	c, err := client.NewClient()
	if err != nil {
		panic(err) // handle error properly
	}

	// Kick off the login flow
	authOk, err := c.RequestAccess(ctx, account.String())
	if err != nil {
		panic(err) // handle error properly
	}

	// Start polling to see if the user as authenticated yet
	resultChan := c.PollClaim(ctx, authOk)
	fmt.Println("Please click the link in your email to authenticate...")
	// Wait for the user to authenticate
	proofs, err := result.Unwrap(<-resultChan)
	if err != nil {
		panic(err) // handle error properly
	}

	// Add the proofs to the client
	c.AddProofs(proofs...)
}
