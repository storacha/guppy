package directory

import (
	"fmt"
	"math/bits"

	"github.com/ipfs/go-bitfield"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode/data"
	"github.com/ipfs/go-unixfsnode/data/builder"
	dagpb "github.com/ipld/go-codec-dagpb"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/guppy/pkg/preparation/dag/model"
	"github.com/storacha/guppy/pkg/preparation/dag/unixfs"
)

type shard struct {
	// metadata about the shard
	hasher  uint64
	size    int
	sizeLg2 int
	width   int
	depth   int

	children map[int]entry
}

// a shard entry is either another shard, or a direct link.
type entry struct {
	*shard
	*hamtLink
}

// a hamtLink is a member of the hamt - the file/directory pointed to, but
// stored with it's hashed key used for addressing.
type hamtLink struct {
	hash hashBits
	model.LinkParams
}

// hashBits is a helper for pulling out sections of a hash
type hashBits []byte

func mkmask(n int) byte {
	return (1 << uint(n)) - 1
}

// Slice returns the 'width' bits of the hashBits value as an integer, or an
// error if there aren't enough bits.
func (hb hashBits) Slice(offset, width int) (int, error) {
	if offset+width > len(hb)*8 {
		return 0, fmt.Errorf("sharded directory too deep")
	}
	return hb.slice(offset, width), nil
}

func (hb hashBits) slice(offset, width int) int {
	curbi := offset / 8
	leftb := 8 - (offset % 8)

	curb := hb[curbi]
	if width == leftb {
		out := int(mkmask(width) & curb)
		return out
	} else if width < leftb {
		a := curb & mkmask(leftb)     // mask out the high bits we don't want
		b := a & ^mkmask(leftb-width) // mask out the low bits we don't want
		c := b >> uint(leftb-width)   // shift whats left down
		return int(c)
	} else {
		out := int(mkmask(leftb) & curb)
		out <<= uint(width - leftb)
		out += hb.slice(offset+leftb, width-leftb)
		return out
	}
}

func logtwo(v int) (int, error) {
	if v <= 0 {
		return 0, fmt.Errorf("hamt size should be a power of two")
	}
	lg2 := bits.TrailingZeros(uint(v))
	if 1<<uint(lg2) != v {
		return 0, fmt.Errorf("hamt size should be a power of two")
	}
	return lg2, nil
}

// BuildUnixFSShardedDirectory will build a hamt of unixfs hamt shards encoing a directory with more entries
// than is typically allowed to fit in a standard IPFS single-block unixFS directory.
// This code is adapted from https://github.com/ipfs/go-unixfsnode/blob/main/data/builder/dirshard.go
// but is tailored to the dag walking process we need for database storage
func BuildUnixFSShardedDirectory(size int, hasher uint64, linkParams []model.LinkParams, visitor VisitUnixFSNodeVisitor) (cid.Cid, error) {
	h, err := multihash.GetHasher(hasher)
	if err != nil {
		return cid.Undef, err
	}
	hamtEntries := make([]hamtLink, 0, len(linkParams))
	for _, lp := range linkParams {
		h.Reset()
		h.Write([]byte(lp.Name))
		sum := h.Sum(nil)
		hamtEntries = append(hamtEntries, hamtLink{
			sum,
			lp,
		})
	}

	sizeLg2, err := logtwo(size)
	if err != nil {
		return cid.Undef, err
	}

	sharder := shard{
		hasher:  hasher,
		size:    size,
		sizeLg2: sizeLg2,
		width:   len(fmt.Sprintf("%X", size-1)),
		depth:   0,

		children: make(map[int]entry),
	}

	for _, entry := range hamtEntries {
		err := sharder.add(entry)
		if err != nil {
			return cid.Undef, err
		}
	}

	c, _, err := sharder.serialize(visitor)
	if err != nil {
		return cid.Undef, err
	}
	return c, nil
}

func (s *shard) add(lnk hamtLink) error {
	// get the bucket for lnk
	bucket, err := lnk.hash.Slice(s.depth*s.sizeLg2, s.sizeLg2)
	if err != nil {
		return err
	}

	current, ok := s.children[bucket]
	if !ok {
		// no bucket, make one with this entry
		s.children[bucket] = entry{nil, &lnk}
		return nil
	} else if current.shard != nil {
		// existing shard, add this link to the shard
		return current.shard.add(lnk)
	}
	// make a shard for current and lnk
	newShard := entry{
		&shard{
			hasher:   s.hasher,
			size:     s.size,
			sizeLg2:  s.sizeLg2,
			width:    s.width,
			depth:    s.depth + 1,
			children: make(map[int]entry),
		},
		nil,
	}
	// add existing link from this bucket to the new shard
	if err := newShard.add(*current.hamtLink); err != nil {
		return err
	}
	// replace bucket with shard
	s.children[bucket] = newShard
	// add new link to the new shard
	return newShard.add(lnk)
}

func (s *shard) formatLinkName(name string, idx int) string {
	return fmt.Sprintf("%0*X%s", s.width, idx, name)
}

// bitmap calculates the bitmap of which links in the shard are set.
func (s *shard) bitmap() ([]byte, error) {
	bm, err := bitfield.NewBitfield(s.size)
	if err != nil {
		return nil, err
	}
	for i := 0; i < s.size; i++ {
		if _, ok := s.children[i]; ok {
			bm.SetBit(i)
		}
	}
	return bm.Bytes(), nil
}

// serialize stores the concrete representation of this shard in the link system and
// returns a link to it.
func (s *shard) serialize(visitor VisitUnixFSNodeVisitor) (cid.Cid, uint64, error) {
	bm, err := s.bitmap()
	if err != nil {
		return cid.Undef, 0, err
	}
	ufsNode, err := builder.BuildUnixFS(func(b *builder.Builder) {
		builder.DataType(b, data.Data_HAMTShard)
		builder.HashType(b, s.hasher)
		builder.Data(b, bm)
		builder.Fanout(b, uint64(s.size))
	})
	if err != nil {
		return cid.Undef, 0, err
	}
	ufsData := data.EncodeUnixFSData(ufsNode)

	// sorting happens in codec-dagpb
	var totalSize uint64
	var links []dagpb.PBLink
	for idx, e := range s.children {
		var lnk dagpb.PBLink
		if e.shard != nil {
			c, sz, err := e.shard.serialize(visitor)
			if err != nil {
				return cid.Undef, 0, err
			}
			totalSize += sz
			fullName := s.formatLinkName("", idx)
			lnk, err = builder.BuildUnixFSDirectoryEntry(fullName, int64(sz), cidlink.Link{Cid: c})

		} else {
			fullName := s.formatLinkName(e.Name, idx)
			sz := e.TSize
			totalSize += sz
			lnk, err = builder.BuildUnixFSDirectoryEntry(fullName, int64(sz), cidlink.Link{Cid: e.Hash})
		}
		if err != nil {
			return cid.Undef, 0, err
		}
		links = append(links, lnk)
	}
	nd, err := unixfs.BuildNode(ufsData, links)
	if err != nil {
		return cid.Undef, 0, fmt.Errorf("building unixfs node: %w", err)
	}
	c, data, err := unixfs.WritePBNode(nd)
	if err != nil {
		return cid.Undef, 0, fmt.Errorf("writing pb node: %w", err)
	}
	if err := visitor.VisitUnixFSNode(c, uint64(len(data)), ufsData, links, data); err != nil {
		return cid.Undef, 0, fmt.Errorf("visiting unixfs node: %w", err)
	}
	return c, totalSize + uint64(len(data)), nil
}
