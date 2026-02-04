package verification

import (
	"bytes"
	"fmt"

	"github.com/ipfs/go-cid"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-libstoracha/digestutil"
)

func verifyIntegrity(digest multihash.Multihash, data []byte) error {
	digestInfo, err := multihash.Decode(digest)
	if err != nil {
		return fmt.Errorf("decoding multihash for block %q: %w", digestutil.Format(digest), err)
	}
	vdigest, err := multihash.Sum(data, digestInfo.Code, digestInfo.Length)
	if err != nil {
		return fmt.Errorf("computing multihash for block %q: %w", digestutil.Format(digest), err)
	}
	if !bytes.Equal(vdigest, digest) {
		return fmt.Errorf("integrity check failed: expected digest %q, got %q", digestutil.Format(digest), digestutil.Format(vdigest))
	}
	return nil
}

// extractLinks decodes a block with the given codec and extracts all unique
// links present in the block.
func extractLinks(codec uint64, b []byte) ([]cid.Cid, error) {
	var decoder ipld.Decoder
	switch codec {
	case cid.Raw:
		return []cid.Cid{}, nil
	case cid.DagProtobuf:
		decoder = dagpb.Decode
	case cid.DagCBOR:
		decoder = dagcbor.Decode
	case cid.DagJSON:
		decoder = dagjson.Decode
	default:
		return nil, fmt.Errorf("unsupported codec: %d", codec)
	}
	n, err := ipld.Decode(b, decoder)
	if err != nil {
		return nil, fmt.Errorf("decoding dag-pb block: %w", err)
	}
	links, err := traversal.SelectLinks(n)
	if err != nil {
		return nil, fmt.Errorf("selecting links: %w", err)
	}
	values := map[cid.Cid]struct{}{}
	keys := []cid.Cid{}
	for _, l := range links {
		c := toCID(l)
		if _, ok := values[c]; ok {
			continue
		}
		keys = append(keys, c)
		values[c] = struct{}{}
	}
	return keys, nil
}

// toCID converts an IPLD link to a CID instance. If the link is not already a
// [cidlink.Link], it attempts to parse the CID from the link's string
// representation. It will panic if parsing fails.
func toCID(l ipld.Link) cid.Cid {
	cl, ok := l.(cidlink.Link)
	if ok {
		return cl.Cid
	}
	c, err := cid.Parse(l.String())
	if err != nil {
		panic(fmt.Sprintf("parsing link CID %q: %v", l.String(), err))
	}
	return c
}
