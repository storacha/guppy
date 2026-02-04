package cmd

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"iter"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v5"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/dustin/go-humanize"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
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
	"github.com/storacha/go-ucanto/client/retrieval"
	"github.com/storacha/go-ucanto/core/dag/blockstore"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/go-ucanto/validator"
	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/agentstore"
	"github.com/storacha/guppy/pkg/config"
	indexing_service "github.com/storacha/indexing-service/pkg/client"
	"github.com/storacha/indexing-service/pkg/types"
)

var (
	forgeIndexerID, _  = did.Parse("did:web:indexer.forge.storacha.network")
	forgeIndexerURL, _ = url.Parse("https://indexer.forge.storacha.network")
	// hotIndexerID, _  = did.Parse("did:web:indexer.storacha.network")
	// hotIndexerURL, _ = url.Parse("https://indexer.storacha.network")
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

		indexerClient, err := indexing_service.New(forgeIndexerID, *forgeIndexerURL)
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

			return delegation.Delegate(guppy.Issuer(), forgeIndexerID, caps, opts...)
		}

		indexer := newIndexer(indexerClient, authorizeIndexer)

		getProofs := func(space did.DID) ([]delegation.Proof, error) {
			var pfs []delegation.Proof
			dlgs, err := guppy.Proofs(agentstore.CapabilityQuery{
				Can:  contentcap.RetrieveAbility,
				With: space.String(),
			})
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

		p := tea.NewProgram(newVerifyModel(root))

		go func() {
			for msg, err := range verifyDAG(cmd.Context(), guppy.Issuer(), getProofs, indexer, root) {
				if err != nil {
					p.Quit()
					cobra.CheckErr(err)
				}
				p.Send(msg)
			}
		}()

		_, err = p.Run()
		return err
	},
}

func init() {
	rootCmd.AddCommand(verifyCmd)
}

type verifyModel struct {
	root         cid.Cid
	blocks       bytemap.ByteMap[multihash.Multihash, struct{}] // not validated
	vblocks      bytemap.ByteMap[multihash.Multihash, struct{}] // validated
	shards       bytemap.ByteMap[multihash.Multihash, uint64]   // shard digest -> verified blocks
	size         uint64
	origins      map[did.DID]string // node DID -> hostname
	originBlocks map[did.DID]uint64 // node DID -> verified blocks
}

func newVerifyModel(root cid.Cid) verifyModel {
	blocks := bytemap.NewByteMap[multihash.Multihash, struct{}](1)
	blocks.Set(root.Hash(), struct{}{})
	return verifyModel{
		root:         root,
		blocks:       blocks,
		vblocks:      bytemap.NewByteMap[multihash.Multihash, struct{}](0),
		shards:       bytemap.NewByteMap[multihash.Multihash, uint64](0),
		origins:      map[did.DID]string{},
		originBlocks: map[did.DID]uint64{},
	}
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
			m.blocks.Delete(msg.stat.Digest)
			m.size += msg.stat.Size
		}
		for _, link := range msg.stat.Links {
			if !m.vblocks.Has(link.Hash()) {
				m.blocks.Set(link.Hash(), struct{}{})
			}
		}
		shardBlockCount := m.shards.Get(msg.stat.Origin.Shard) + 1
		m.shards.Set(msg.stat.Origin.Shard, shardBlockCount)
		m.origins[msg.stat.Origin.Node] = msg.stat.Origin.URL.Hostname()
		m.originBlocks[msg.stat.Origin.Node] = m.originBlocks[msg.stat.Origin.Node] + 1
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

	if m.shards.Size() > 0 {
		sb.WriteString(heading.Render("Shards "))
		sb.WriteString(faint.Render("(blocks verified)"))
		sb.WriteString("\n")
		shards := make([]struct {
			digest string
			count  string
		}, 0, m.shards.Size())
		for shard, count := range m.shards.Iterator() {
			shards = append(shards, struct {
				digest string
				count  string
			}{
				digest: digestutil.Format(shard),
				count:  humanize.Comma(int64(count)),
			})
		}
		slices.SortFunc(shards, func(a, b struct {
			digest string
			count  string
		}) int {
			return strings.Compare(a.digest, b.digest)
		})
		max := 5
		for i := range max {
			if i >= len(shards) {
				break
			}
			shard := shards[i]
			sb.WriteString("  ")
			sb.WriteString(shard.digest)
			sb.WriteString(" (")
			sb.WriteString(shard.count)
			sb.WriteString(")\n")
		}
		if len(shards) > max {
			sb.WriteString("  ...")
			sb.WriteString(humanize.Comma(int64(len(shards) - max)))
			sb.WriteString(" more\n")
		}
	}

	if len(m.origins) > 0 {
		sb.WriteString(heading.Render("Origins"))
		sb.WriteString("\n")
		origins := make([]struct {
			node   string
			host   string
			blocks string
		}, 0, m.shards.Size())
		for node, host := range m.origins {
			origins = append(origins, struct {
				node   string
				host   string
				blocks string
			}{
				node:   node.String(),
				host:   host,
				blocks: humanize.Comma(int64(m.originBlocks[node])),
			})
		}
		slices.SortFunc(origins, func(a, b struct {
			node   string
			host   string
			blocks string
		}) int {
			return strings.Compare(a.node, b.node)
		})
		max := 5
		for i := range max {
			if i >= len(origins) {
				break
			}
			origin := origins[i]
			sb.WriteString("  ")
			sb.WriteString(origin.node)
			sb.WriteString(faint.Render(" @ "))
			sb.WriteString(faint.Render(origin.host))
			sb.WriteString(" (")
			sb.WriteString(origin.blocks)
			sb.WriteString(")\n")
		}
		if len(origins) > max {
			sb.WriteString("  ...")
			sb.WriteString(humanize.Comma(int64(len(origins) - max)))
			sb.WriteString(" more\n")
		}
	}

	sb.WriteString(heading.Render("Blocks "))
	sb.WriteString(faint.Render("verified / known"))
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("  %s / %s\n", humanize.Comma(int64(m.vblocks.Size())), humanize.Comma(int64(m.vblocks.Size()+m.blocks.Size()))))

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

type proofGetterFunc func(space did.DID) ([]delegation.Proof, error)
type authorizeIndexerRetrievalFunc func() (delegation.Delegation, error)

func verifyDAG(
	ctx context.Context,
	id principal.Signer,
	getProofs proofGetterFunc,
	indexer *indexer,
	root cid.Cid,
) iter.Seq2[tea.Msg, error] {

	// list of already verified blocks to avoid redundant work
	verified := bytemap.NewByteMap[multihash.Multihash, struct{}](0)

	return func(yield func(tea.Msg, error) bool) {
		queue := [][]cid.Cid{{root}}
		for len(queue) > 0 {
			chunk := queue[0]
			queue = queue[1:]

			// filter blocks we may have verified since adding the chunk to the queue
			unverified := []cid.Cid{}
			for _, link := range chunk {
				if !verified.Has(link.Hash()) {
					unverified = append(unverified, link)
				}
			}
			if len(unverified) == 0 {
				continue
			}

			for stat, err := range statBlocks(ctx, id, getProofs, indexer, unverified) {
				if err != nil {
					yield(nil, fmt.Errorf("retrieving block stats: %w", err))
					return
				}
				if len(stat.Links) > 0 {
					queue = append(queue, stat.Links)
				}
				if !yield(blockVerifiedMsg{stat: stat}, nil) {
					return
				}
				// mark as verified
				verified.Set(stat.Digest, struct{}{})
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

type location struct {
	commitment delegation.Delegation
	caveats    assert.LocationCaveats
}

type locationCache struct {
	// shard digest -> commitment CID -> location info
	locations bytemap.ByteMap[multihash.Multihash, map[cid.Cid]location]
	mutex     sync.RWMutex
}

func (c *locationCache) Add(commitment delegation.Delegation) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	match, err := assert.Location.Match(validator.NewSource(commitment.Capabilities()[0], commitment))
	if err != nil {
		log.Warnw("adding location to cache", "error", err)
		return
	}

	root := toCID(commitment.Link())
	nb := match.Value().Nb()

	shardLocations := c.locations.Get(nb.Content.Hash())
	if shardLocations == nil {
		shardLocations = map[cid.Cid]location{}
		c.locations.Set(nb.Content.Hash(), shardLocations)
	}
	shardLocations[root] = location{commitment: commitment, caveats: nb}
}

func (c *locationCache) LocationsForShard(shard multihash.Multihash) []location {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return slices.Collect(maps.Values(c.locations.Get(shard)))
}

type indexer struct {
	client        *indexing_service.Client
	authorize     authorizeIndexerRetrievalFunc
	indexCache    *indexCache
	locationCache *locationCache
}

func newIndexer(client *indexing_service.Client, authorize authorizeIndexerRetrievalFunc) *indexer {
	return &indexer{
		client:    client,
		authorize: authorize,
		indexCache: &indexCache{
			indexes:    map[cid.Cid]blobindex.ShardedDagIndex{},
			sliceIndex: bytemap.NewByteMap[multihash.Multihash, cid.Cid](0),
		},
		locationCache: &locationCache{
			locations: bytemap.NewByteMap[multihash.Multihash, map[cid.Cid]location](0),
		},
	}
}

// do a query and cache the results
func (i *indexer) doQuery(ctx context.Context, digest multihash.Multihash) error {
	result, err := backoff.Retry(ctx, func() (types.QueryResult, error) {
		q := types.Query{Hashes: []multihash.Multihash{digest}}
		if i.authorize != nil {
			auth, err := i.authorize()
			if err != nil {
				return nil, fmt.Errorf("authorizing indexer retrieval: %w", err)
			}

			spaces := []did.DID{}
			for _, c := range auth.Capabilities() {
				if c.Can() == contentcap.RetrieveAbility {
					s, err := did.Parse(c.With())
					if err != nil {
						return nil, fmt.Errorf("parsing space DID from authorized capability: %w", err)
					}
					spaces = append(spaces, s)
				}
			}

			q.Match = types.Match{Subject: spaces}
			q.Delegations = []delegation.Delegation{auth}
		}

		return i.client.QueryClaims(ctx, q)
	}, backoff.WithMaxTries(3))
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
		i.indexCache.Add(toCID(root), index)
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

// FindLocations finds at least 1 location for the given shard.
func (i *indexer) FindLocations(ctx context.Context, shard multihash.Multihash) ([]location, error) {
	locations := i.locationCache.LocationsForShard(shard)
	if len(locations) == 0 {
		err := i.doQuery(ctx, shard)
		if err != nil {
			return nil, err
		}
		locations = i.locationCache.LocationsForShard(shard)
		if len(locations) == 0 {
			return nil, fmt.Errorf("location for shard %q not found", digestutil.Format(shard))
		}
	}
	return locations, nil
}

type blockStat struct {
	Codec  uint64
	Size   uint64
	Digest multihash.Multihash
	// Links are the CIDs linked from this block, deduped.
	Links  []cid.Cid
	Origin origin
}

type origin struct {
	Node     did.DID             // node that provided the data
	URL      url.URL             // url of the shard the data came from
	Shard    multihash.Multihash // hash of the shard
	Position blobindex.Position  // byte range within the shard
}

// statBlocks retrieves block statistics for the given CIDs using the provided
// indexer. It also performs integrity checks on the retrieved blocks.
func statBlocks(ctx context.Context, id principal.Signer, getProofs proofGetterFunc, indexer *indexer, links []cid.Cid) iter.Seq2[blockStat, error] {
	return func(yield func(blockStat, error) bool) {
		for _, link := range links {
			slice := link.Hash()
			shard, pos, err := indexer.FindShard(ctx, slice)
			if err != nil {
				yield(blockStat{}, fmt.Errorf("finding shard for %q: %w", link, err))
				return
			}

			locations, err := indexer.FindLocations(ctx, shard)
			if err != nil {
				yield(blockStat{}, fmt.Errorf("finding location for shard %q: %w", shard, err))
				return
			}

			var fetchErr error
			for _, loc := range locations {
				var body []byte
				var err error
				if getProofs != nil {
					body, err = authorizedFetch(ctx, id, getProofs, loc, pos)
				} else {
					body, err = fetch(ctx, loc, pos)
				}
				if err != nil {
					fetchErr = err
					continue
				}

				err = verifyIntegrity(slice, body)
				if err != nil {
					fetchErr = err
					continue
				}

				codec := link.Prefix().Codec
				links, err := extractLinks(codec, body)
				if err != nil {
					fetchErr = fmt.Errorf("extracting links for block %q: %w", link, err)
					continue
				}

				if !yield(blockStat{
					Codec:  codec,
					Size:   pos.Length,
					Digest: slice,
					Links:  links,
					Origin: origin{
						Node:     loc.commitment.Issuer().DID(),
						URL:      loc.caveats.Location[0],
						Shard:    shard,
						Position: pos,
					},
				}, nil) {
					return
				}
				fetchErr = nil
				break
			}
			if fetchErr != nil {
				if !yield(blockStat{}, fetchErr) {
					return
				}
			}
		}
	}
}

func authorizedFetch(ctx context.Context, id principal.Signer, getProofs proofGetterFunc, shardLocation location, slicePosition blobindex.Position) ([]byte, error) {
	if shardLocation.caveats.Space == did.Undef {
		return nil, fmt.Errorf("missing space DID in location commitment for shard: %s", digestutil.Format(shardLocation.caveats.Content.Hash()))
	}

	var reqErr error
	for _, url := range shardLocation.caveats.Location {
		conn, err := retrieval.NewConnection(shardLocation.commitment.Issuer(), &url)
		if err != nil {
			return nil, fmt.Errorf("creating retrieval connection to %q: %w", url.String(), err)
		}

		body, err := backoff.Retry(ctx, func() ([]byte, error) {
			proofs, err := getProofs(shardLocation.caveats.Space)
			if err != nil {
				return nil, fmt.Errorf("getting proofs for retrieval to %q: %w", url.String(), err)
			}

			inv, err := contentcap.Retrieve.Invoke(
				id,
				shardLocation.commitment.Issuer(),
				shardLocation.caveats.Space.String(),
				contentcap.RetrieveCaveats{
					Blob: contentcap.BlobDigest{
						Digest: shardLocation.caveats.Content.Hash(),
					},
					Range: contentcap.Range{
						Start: slicePosition.Offset,
						End:   slicePosition.Offset + slicePosition.Length - 1,
					},
				},
				delegation.WithProof(proofs...),
			)
			if err != nil {
				return nil, fmt.Errorf("invoking retrieve capability for retrieval to %q: %w", url.String(), err)
			}

			_, hres, err := retrieval.Execute(ctx, inv, conn)
			if err != nil {
				return nil, fmt.Errorf("executing retrieve invocation for retrieval to %q: %w", url.String(), err)
			}
			defer hres.Body().Close()

			body, err := io.ReadAll(hres.Body())
			if err != nil {
				return nil, fmt.Errorf("reading response body for request to %q: %w", url.String(), err)
			}
			return body, nil
		}, backoff.WithMaxTries(3))
		if err != nil {
			reqErr = err
			continue
		}

		return body, nil
	}
	return nil, reqErr
}

func fetch(ctx context.Context, shardLocation location, slicePosition blobindex.Position) ([]byte, error) {
	var reqErr error
	for _, url := range shardLocation.caveats.Location {
		req, err := http.NewRequestWithContext(ctx, "GET", url.String(), nil)
		if err != nil {
			reqErr = fmt.Errorf("creating request for block %q: %w", url.String(), err)
			continue
		}
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", slicePosition.Offset, slicePosition.Offset+slicePosition.Length-1))

		res, err := http.DefaultClient.Do(req)
		if err != nil {
			reqErr = fmt.Errorf("performing request for block %q: %w", url.String(), err)
			continue
		}
		defer res.Body.Close()

		body, err := io.ReadAll(res.Body)
		if err != nil {
			reqErr = fmt.Errorf("reading response body for block %q: %w", url.String(), err)
			continue
		}
		return body, nil
	}
	return nil, reqErr
}

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
