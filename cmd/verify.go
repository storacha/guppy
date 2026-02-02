package cmd

import (
	"bytes"
	"context"
	"fmt"
	"iter"
	"net/url"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/dustin/go-humanize"
	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multihash"
	"github.com/spf13/cobra"
	"github.com/storacha/go-libstoracha/blobindex"
	"github.com/storacha/go-libstoracha/bytemap"
	contentcap "github.com/storacha/go-libstoracha/capabilities/space/content"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/pkg/agentstore"
	"github.com/storacha/guppy/pkg/config"
	indexer "github.com/storacha/indexing-service/pkg/client"
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

		idxr, err := indexer.New(hotIndexerID, *hotIndexerURL)
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

		p := tea.NewProgram(verifyModel{
			root:    root,
			shards:  bytemap.NewByteMap[multihash.Multihash, struct{}](0),
			blocks:  bytemap.NewByteMap[multihash.Multihash, struct{}](0),
			indexes: map[cid.Cid]blobindex.ShardedDagIndex{},
		})

		go func() {
			for msg, err := range verifyDAG(cmd.Context(), root, idxr, getProofs, authorizeIndexer) {
				cobra.CheckErr(err)
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
	root    cid.Cid
	shards  bytemap.ByteMap[multihash.Multihash, struct{}]
	indexes map[cid.Cid]blobindex.ShardedDagIndex
	blocks  bytemap.ByteMap[multihash.Multihash, struct{}]
	vblocks uint64
	size    uint64
	vsize   uint64
}

func (m verifyModel) Init() tea.Cmd {
	return nil
}

func (m verifyModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		return m, tea.Quit
	case queryResultMsg:
		for root, idx := range msg.indexes {
			if _, ok := m.indexes[root]; ok {
				continue
			}
			m.indexes[root] = idx
			for shard, slices := range idx.Shards().Iterator() {
				m.shards.Set(shard, struct{}{})
				for slice, pos := range slices.Iterator() {
					if m.shards.Has(slice) {
						continue
					}
					m.blocks.Set(slice, struct{}{})
					m.size += pos.Length
				}
			}
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

	sb.WriteString(heading.Render("Indexes"))
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("  %s\n", humanize.Comma(int64(len(m.indexes)))))

	sb.WriteString(heading.Render("Shards"))
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("  %s\n", humanize.Comma(int64(m.shards.Size()))))

	sb.WriteString(heading.Render("Blocks "))
	sb.WriteString(faint.Render("(verified / known)"))
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("  %s / %s\n", humanize.Comma(int64(m.vblocks)), humanize.Comma(int64(m.blocks.Size()))))

	sb.WriteString(heading.Render("Size "))
	sb.WriteString(faint.Render("(verified / known)"))
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("  %s / %s\n", humanize.IBytes(m.vsize), humanize.IBytes(m.size)))

	sb.WriteString("\n")
	return sb.String()
}

var _ tea.Model = (*verifyModel)(nil)

type queryResultMsg struct {
	indexes map[cid.Cid]blobindex.ShardedDagIndex
}

type proofGetterFunc func() ([]delegation.Proof, error)
type authorizeIndexerRetrievalFunc func() (delegation.Delegation, error)

func verifyDAG(ctx context.Context, root cid.Cid, indexer *indexer.Client, getProofs proofGetterFunc, authorizeIndexer authorizeIndexerRetrievalFunc) iter.Seq2[tea.Msg, error] {
	return func(yield func(tea.Msg, error) bool) {
		idxAuth, err := authorizeIndexer()
		if err != nil {
			yield(nil, fmt.Errorf("authorizing retrieval: %w", err))
			return
		}

		result, err := indexer.QueryClaims(ctx, types.Query{
			Hashes:      []multihash.Multihash{root.Hash()},
			Delegations: []delegation.Delegation{idxAuth},
		})
		if err != nil {
			yield(nil, fmt.Errorf("querying indexer for root CID %s: %w", root.String(), err))
			return
		}
		msg, err := toQueryResultMessage(result)
		if err != nil {
			yield(nil, fmt.Errorf("processing query result for root %q: %w", root.String(), err))
			return
		}
		if !yield(msg, nil) {
			return
		}

		yield(tea.QuitMsg{}, nil)
	}
}

func toQueryResultMessage(result types.QueryResult) (queryResultMsg, error) {
	blocks := map[ipld.Link]ipld.Block{}
	for b, err := range result.Blocks() {
		if err != nil {
			return queryResultMsg{}, err
		}
		blocks[b.Link()] = b
	}
	idxlinks := result.Indexes()
	m := queryResultMsg{
		indexes: make(map[cid.Cid]blobindex.ShardedDagIndex, len(idxlinks)),
	}
	for _, root := range idxlinks {
		index, err := blobindex.Extract(bytes.NewReader(blocks[root].Bytes()))
		if err != nil {
			return queryResultMsg{}, err
		}
		m.indexes[root.(cidlink.Link).Cid] = index
	}
	return m, nil
}
