// Helper tool to generate a valid test CID from a seed
package main

import (
	"encoding/hex"
	"fmt"
	"os"

	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <seed-number>\n", os.Args[0])
		os.Exit(1)
	}

	seed := os.Args[1]

	// Create a multihash from the seed (SHA2-256)
	// We'll just use the seed repeated to make 32 bytes
	hashBytes := make([]byte, 32)
	for i := 0; i < 32; i++ {
		hashBytes[i] = byte(i) ^ byte(seed[i%len(seed)])
	}

	mhash, err := mh.Encode(hashBytes, mh.SHA2_256)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating multihash: %v\n", err)
		os.Exit(1)
	}

	// Create CID v1 with DagProtobuf codec (used for UnixFS nodes)
	c := cid.NewCidV1(cid.DagProtobuf, mhash)

	// Output as hex bytes
	fmt.Print(hex.EncodeToString(c.Bytes()))
}
