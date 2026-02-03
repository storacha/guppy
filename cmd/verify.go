package cmd

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"iter"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/dustin/go-humanize"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	dagpb "github.com/ipld/go-codec-dagpb"
	prime "github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/multiformats/go-multihash"
	"github.com/spf13/cobra"
	"github.com/storacha/go-libstoracha/blobindex"
	"github.com/storacha/go-libstoracha/bytemap"
	"github.com/storacha/go-libstoracha/capabilities/assert"
	contentcap "github.com/storacha/go-libstoracha/capabilities/space/content"
	"github.com/storacha/go-libstoracha/digestutil"
	"github.com/storacha/go-ucanto/core/dag/blockstore"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/go-ucanto/validator"
	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/agentstore"
	"github.com/storacha/guppy/pkg/config"
	indexing_service "github.com/storacha/indexing-service/pkg/client"
	"github.com/storacha/indexing-service/pkg/types"
)

var (
	hotIndexerID, _  = did.Parse("did:web:indexer.storacha.network")
	hotIndexerURL, _ = url.Parse("https://indexer.storacha.network")
)

var verifyCmd = &cobra.Command{
	Use:   "verify <root-cid>",
	Short: "Verify a DAG",
	Long:  `Verify the integrity and correctness of a Directed Acyclic Graph (DAG).`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		logging.SetLogLevel("cmd", "INFO")

		cfg, err := config.Load[config.Config]()
		if err != nil {
			return fmt.Errorf("loading config: %w", err)
		}

		root, err := cid.Parse(args[0])
		if err != nil {
			return fmt.Errorf("parsing root CID: %w", err)
		}

		guppy := cmdutil.MustGetClient(cfg.Repo.Dir)
		allProofs, err := guppy.Proofs(agentstore.CapabilityQuery{Can: contentcap.RetrieveAbility})
		if err != nil {
			return err
		}

		authdSpaces := map[did.DID]struct{}{}
		for _, proof := range allProofs {
			if r, ok := cmdutil.ProofResource(proof, contentcap.RetrieveAbility); ok {
				spaceDID, err := did.Parse(r)
				if err == nil {
					authdSpaces[spaceDID] = struct{}{}
				}
			}
		}

		idxr, err := indexing_service.New(hotIndexerID, *hotIndexerURL)
		cobra.CheckErr(err)

		authorizeIndexer := func() (delegation.Delegation, error) {
			queries := make([]agentstore.CapabilityQuery, 0, len(authdSpaces))
			for space := range authdSpaces {
				queries = append(queries, agentstore.CapabilityQuery{
					Can:  contentcap.RetrieveAbility,
					With: space.String(),
				})
			}

			var pfs []delegation.Proof
			dlgs, err := guppy.Proofs(queries...)
			if err != nil {
				return nil, err
			}
			for _, del := range dlgs {
				pfs = append(pfs, delegation.FromDelegation(del))
			}

			caps := make([]ucan.Capability[ucan.NoCaveats], 0, len(authdSpaces))
			for space := range authdSpaces {
				caps = append(caps, ucan.NewCapability(contentcap.RetrieveAbility, space.String(), ucan.NoCaveats{}))
			}

			opts := []delegation.Option{
				delegation.WithProof(pfs...),
				delegation.WithExpiration(int(time.Now().Add(30 * time.Second).Unix())),
			}

			return delegation.Delegate(guppy.Issuer(), hotIndexerID, caps, opts...)
		}

		getProofs := func() ([]delegation.Proof, error) {
			queries := make([]agentstore.CapabilityQuery, 0, len(authdSpaces))
			for space := range authdSpaces {
				queries = append(queries, agentstore.CapabilityQuery{
					Can:  contentcap.RetrieveAbility,
					With: space.String(),
				})
			}
			var pfs []delegation.Proof
			dlgs, err := guppy.Proofs(queries...)
			if err != nil {
				return nil, err
			}
			for _, del := range dlgs {
				pfs = append(pfs, delegation.FromDelegation(del))
			}
			return pfs, nil
		}

		blocks := bytemap.NewByteMap[multihash.Multihash, struct{}](1)
		blocks.Set(root.Hash(), struct{}{})

		p := tea.NewProgram(verifyModel{
			root:    root,
			blocks:  blocks,
			vblocks: bytemap.NewByteMap[multihash.Multihash, struct{}](0),
		})

		go func() {
			for msg, err := range verifyDAG(cmd.Context(), root, idxr, getProofs, authorizeIndexer) {
				if err != nil {
					p.Quit()
					cobra.CheckErr(err)
				}
				p.Send(msg)
			}
		}()

		_, err = p.Run()
		if err != nil {
			return err
		}

		cmd.Println("Verification complete.")
		return nil
	},
}

func init() {
	rootCmd.AddCommand(verifyCmd)
}

type verifyModel struct {
	root    cid.Cid
	blocks  bytemap.ByteMap[multihash.Multihash, struct{}]
	vblocks bytemap.ByteMap[multihash.Multihash, struct{}]
	size    uint64
}

func (m verifyModel) Init() tea.Cmd {
	return nil
}

func (m verifyModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		return m, tea.Quit
	case blockVerifiedMsg:
		if !m.vblocks.Has(msg.stat.Digest) {
			m.vblocks.Set(msg.stat.Digest, struct{}{})
			m.size += msg.stat.Size
		}
		for _, link := range msg.stat.Links {
			m.blocks.Set(link.Hash(), struct{}{})
		}
		return m, nil
	default:
		return m, nil
	}
}

var heading = lipgloss.NewStyle().Bold(true)
var faint = lipgloss.NewStyle().Faint(true)

func (m verifyModel) View() string {
	var sb strings.Builder
	sb.WriteString("\n")
	sb.WriteString(heading.Render("Root"))
	sb.WriteString("\n  ")
	sb.WriteString(m.root.String())
	sb.WriteString("\n")

	sb.WriteString(heading.Render("Blocks "))
	sb.WriteString(faint.Render("(verified / known)"))
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("  %s / %s\n", humanize.Comma(int64(m.vblocks.Size())), humanize.Comma(int64(m.blocks.Size()))))

	sb.WriteString(heading.Render("Size"))
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("  %s\n", humanize.IBytes(m.size)))

	sb.WriteString("\n")
	return sb.String()
}

var _ tea.Model = (*verifyModel)(nil)

type blockVerifiedMsg struct {
	stat blockStat
}

type proofGetterFunc func() ([]delegation.Proof, error)
type authorizeIndexerRetrievalFunc func() (delegation.Delegation, error)

func verifyDAG(ctx context.Context, root cid.Cid, indexerClient *indexing_service.Client, getProofs proofGetterFunc, authorizeIndexer authorizeIndexerRetrievalFunc) iter.Seq2[tea.Msg, error] {
	indexer := &indexer{
		client:    indexerClient,
		authorize: nil,
		indexCache: &indexCache{
			indexes:    map[cid.Cid]blobindex.ShardedDagIndex{},
			sliceIndex: bytemap.NewByteMap[multihash.Multihash, cid.Cid](0),
		},
		locationCache: &locationCache{
			locations:       map[cid.Cid]delegation.Delegation{},
			locationCaveats: map[cid.Cid]assert.LocationCaveats{},
			shardLocation:   bytemap.NewByteMap[multihash.Multihash, cid.Cid](0),
		},
	}

	return func(yield func(tea.Msg, error) bool) {
		queue := [][]cid.Cid{{root}}
		for len(queue) > 0 {
			chunk := queue[0]
			queue = queue[1:]
			for stat, err := range statBlocks(ctx, chunk, indexer) {
				if err != nil {
					yield(nil, fmt.Errorf("retrieving block stats: %w", err))
					return
				}
				if len(stat.Links) > 0 {
					queue = append(queue, stat.Links)
				}
				digestInfo, err := multihash.Decode(stat.Digest)
				if err != nil {
					yield(nil, fmt.Errorf("decoding multihash for block %q: %w", digestutil.Format(stat.Digest), err))
					return
				}
				vdigest, err := multihash.Sum(stat.Bytes, digestInfo.Code, digestInfo.Length)
				if err != nil {
					yield(nil, fmt.Errorf("computing multihash for block %q: %w", digestutil.Format(stat.Digest), err))
					return
				}
				if !bytes.Equal(vdigest, stat.Digest) {
					yield(nil, fmt.Errorf("integrity check failed: expected digest %q, got %q", digestutil.Format(stat.Digest), digestutil.Format(vdigest)))
					return
				}
				if !yield(blockVerifiedMsg{stat: stat}, nil) {
					return
				}
			}
		}
		yield(tea.QuitMsg{}, nil)
	}
}

type indexCache struct {
	indexes    map[cid.Cid]blobindex.ShardedDagIndex
	sliceIndex bytemap.ByteMap[multihash.Multihash, cid.Cid]
	mutex      sync.RWMutex
}

func (c *indexCache) Add(root cid.Cid, index blobindex.ShardedDagIndex) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.indexes[root] = index
	for _, slices := range index.Shards().Iterator() {
		for slice := range slices.Iterator() {
			c.sliceIndex.Set(slice, root)
		}
	}
}

func (c *indexCache) IndexForSlice(slice multihash.Multihash) (blobindex.ShardedDagIndex, bool) {
	root := c.sliceIndex.Get(slice)
	idx, ok := c.indexes[root]
	return idx, ok
}

type locationCache struct {
	locations       map[cid.Cid]delegation.Delegation
	locationCaveats map[cid.Cid]assert.LocationCaveats
	shardLocation   bytemap.ByteMap[multihash.Multihash, cid.Cid]
	mutex           sync.RWMutex
}

func (c *locationCache) Add(location delegation.Delegation) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	match, err := assert.Location.Match(validator.NewSource(location.Capabilities()[0], location))
	if err != nil {
		log.Warnw("adding location to cache", "error", err)
		return
	}

	root := location.Link().(cidlink.Link).Cid
	nb := match.Value().Nb()

	c.locations[root] = location
	c.locationCaveats[root] = nb
	c.shardLocation.Set(nb.Content.Hash(), root)
}

func (c *locationCache) LocationForShard(shard multihash.Multihash) (assert.LocationCaveats, delegation.Delegation, bool) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	root := c.shardLocation.Get(shard)
	loc, ok := c.locations[root]
	if !ok {
		return assert.LocationCaveats{}, nil, false
	}
	caveats, ok := c.locationCaveats[root]
	if !ok {
		return assert.LocationCaveats{}, nil, false
	}
	return caveats, loc, true
}

type indexer struct {
	client        *indexing_service.Client
	authorize     authorizeIndexerRetrievalFunc
	indexCache    *indexCache
	locationCache *locationCache
}

// do a query and cache the results
func (i *indexer) doQuery(ctx context.Context, digest multihash.Multihash) error {
	q := types.Query{Hashes: []multihash.Multihash{digest}}
	if i.authorize != nil {
		auth, err := i.authorize()
		if err != nil {
			return fmt.Errorf("authorizing indexer retrieval: %w", err)
		}

		spaces := []did.DID{}
		for _, c := range auth.Capabilities() {
			if c.Can() == contentcap.RetrieveAbility {
				s, err := did.Parse(c.With())
				if err != nil {
					return fmt.Errorf("parsing space DID from authorized capability: %w", err)
				}
				spaces = append(spaces, s)
			}
		}

		q.Match = types.Match{Subject: spaces}
		q.Delegations = []delegation.Delegation{auth}
	}

	result, err := i.client.QueryClaims(ctx, q)
	if err != nil {
		return fmt.Errorf("querying indexer for slice %s: %w", digestutil.Format(digest), err)
	}

	bs, err := blockstore.NewBlockStore(blockstore.WithBlocksIterator(result.Blocks()))
	if err != nil {
		return fmt.Errorf("creating blockstore from query result: %w", err)
	}

	for _, root := range result.Indexes() {
		b, ok, err := bs.Get(root)
		if err != nil {
			log.Warnw("getting index block", "root", root, "error", err)
			continue
		}
		if !ok {
			log.Warnf("index block %s not found in result blocks", root)
			continue
		}
		index, err := blobindex.Extract(bytes.NewReader(b.Bytes()))
		if err != nil {
			log.Warnw("extracting blob index", "error", err)
			continue
		}
		i.indexCache.Add(root.(cidlink.Link).Cid, index)
	}

	for _, root := range result.Claims() {
		dlg, err := delegation.NewDelegationView(root, bs)
		if err != nil {
			log.Warnw("extracting claim", "error", err)
			continue
		}
		if len(dlg.Capabilities()) < 1 || dlg.Capabilities()[0].Can() != assert.LocationAbility {
			continue
		}
		i.locationCache.Add(dlg)
	}

	return nil
}

func (i *indexer) FindShard(ctx context.Context, slice multihash.Multihash) (multihash.Multihash, blobindex.Position, error) {
	index, ok := i.indexCache.IndexForSlice(slice)
	if !ok {
		err := i.doQuery(ctx, slice)
		if err != nil {
			return nil, blobindex.Position{}, err
		}
		idx, ok := i.indexCache.IndexForSlice(slice)
		if !ok {
			return nil, blobindex.Position{}, fmt.Errorf("index for slice not found: %s", digestutil.Format(slice))
		}
		index = idx
	}

	for shard, slices := range index.Shards().Iterator() {
		for s, pos := range slices.Iterator() {
			if bytes.Equal(s, slice) {
				return shard, pos, nil
			}
		}
	}
	return nil, blobindex.Position{}, fmt.Errorf("slice not found in index: %s", digestutil.Format(slice))
}

func (i *indexer) FindLocation(ctx context.Context, shard multihash.Multihash) (assert.LocationCaveats, delegation.Delegation, error) {
	caveats, loc, ok := i.locationCache.LocationForShard(shard)
	if !ok {
		err := i.doQuery(ctx, shard)
		if err != nil {
			return assert.LocationCaveats{}, nil, err
		}
		caveats, loc, ok = i.locationCache.LocationForShard(shard)
		if !ok {
			return assert.LocationCaveats{}, nil, fmt.Errorf("location for shard %q not found", digestutil.Format(shard))
		}
	}
	return caveats, loc, nil
}

type blockStat struct {
	Codec  uint64
	Size   uint64
	Digest multihash.Multihash
	Bytes  []byte
	Links  []cid.Cid
}

func statBlocks(ctx context.Context, links []cid.Cid, indexer *indexer) iter.Seq2[blockStat, error] {
	return func(yield func(blockStat, error) bool) {
		for _, link := range links {
			slice := link.Hash()
			shard, pos, err := indexer.FindShard(ctx, slice)
			if err != nil {
				yield(blockStat{}, fmt.Errorf("finding shard for %q: %w", link, err))
				return
			}

			nb, _, err := indexer.FindLocation(ctx, shard)
			if err != nil {
				yield(blockStat{}, fmt.Errorf("finding location for shard %q: %w", shard, err))
				return
			}

			var reqErr error
			for _, l := range nb.Location {
				// TODO: replace with UCAN authorized retrieval
				req, err := http.NewRequestWithContext(ctx, "GET", l.String(), nil)
				if err != nil {
					reqErr = fmt.Errorf("creating request for block %q: %w", link, err)
					continue
				}

				req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", pos.Offset, pos.Offset+pos.Length-1))
				res, err := http.DefaultClient.Do(req)
				if err != nil {
					reqErr = fmt.Errorf("performing request for block %q: %w", link, err)
					continue
				}

				body, err := io.ReadAll(res.Body)
				if err != nil {
					res.Body.Close()
					reqErr = fmt.Errorf("reading response body for block %q: %w", link, err)
					continue
				}
				res.Body.Close()

				codec := link.Prefix().Codec
				links, err := extractLinks(codec, body)
				if err != nil {
					reqErr = fmt.Errorf("extracting links for block %q: %w", link, err)
					continue
				}

				if !yield(blockStat{
					Codec:  codec,
					Size:   pos.Length,
					Digest: slice,
					Bytes:  body,
					Links:  links,
				}, nil) {
					return
				}
				reqErr = nil
				break
			}
			if reqErr != nil {
				if !yield(blockStat{}, reqErr) {
					return
				}
			}
		}
	}
}

func extractLinks(codec uint64, b []byte) ([]cid.Cid, error) {
	var decoder prime.Decoder
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
	n, err := prime.Decode(b, decoder)
	if err != nil {
		return nil, fmt.Errorf("decoding dag-pb block: %w", err)
	}
	links, err := traversal.SelectLinks(n)
	if err != nil {
		return nil, fmt.Errorf("selecting links: %w", err)
	}
	dudupes := bytemap.NewByteMap[multihash.Multihash, cid.Cid](0)
	for _, l := range links {
		cl, ok := l.(cidlink.Link)
		if ok {
			dudupes.Set(cl.Hash(), cl.Cid)
			continue
		}
		c, err := cid.Parse(l.String())
		if err != nil {
			return nil, fmt.Errorf("parsing link CID %q: %w", l.String(), err)
		}
		dudupes.Set(c.Hash(), c)
	}
	return slices.Collect(dudupes.Values()), nil
}
