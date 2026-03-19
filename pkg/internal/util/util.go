package util

import "github.com/ipfs/go-cid"

// PlaceholderCID is the smallest legal identity CID (CIDv1, raw codec, identity
// multihash of empty bytes). It's used as a root CID when the actual root CID
// isn't actually needed and might not be known yet.
var PlaceholderCID = cid.NewCidV1(cid.Raw, []byte{0x00, 0x00})
