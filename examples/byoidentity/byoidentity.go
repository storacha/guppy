package main

import (
	"context"
	"fmt"
	"os"

	uploadcap "github.com/storacha/go-libstoracha/capabilities/upload"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/guppy/pkg/client"
	"github.com/storacha/guppy/pkg/delegation"
)

func main() {
	// private key to sign invocation UCAN with
	keybytes, _ := os.ReadFile("path/to/private.key")
	signer, _ := signer.FromRaw(keybytes)

	// UCAN proof that signer can list uploads in this space (a delegation chain)
	prfbytes, _ := os.ReadFile("path/to/proof.ucan")
	proof, _ := delegation.ExtractProof(prfbytes)

	// space to list uploads from
	space, _ := did.Parse("did:key:z6MkwDuRThQcyWjqNsK54yKAmzfsiH6BTkASyiucThMtHt1y")

	// nil uses the default connection to the Storacha network
	c, _ := client.NewClient(nil, nil, client.WithPrincipal(signer))

	c.AddProofs(proof)

	listOk, _ := c.UploadList(
		context.Background(),
		space,
		uploadcap.ListCaveats{},
	)

	for _, r := range listOk.Results {
		fmt.Printf("%s\n", r.Root)
	}
}
