package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"iter"
	"net/http"
	"net/url"
	"time"

	"github.com/multiformats/go-multihash"
	blobcap "github.com/storacha/go-libstoracha/capabilities/blob"
	httpcap "github.com/storacha/go-libstoracha/capabilities/http"
	spaceblobcap "github.com/storacha/go-libstoracha/capabilities/space/blob"
	captypes "github.com/storacha/go-libstoracha/capabilities/types"
	ucancap "github.com/storacha/go-libstoracha/capabilities/ucan"
	w3sblobcap "github.com/storacha/go-libstoracha/capabilities/web3.storage/blob"
	uclient "github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/core/dag/blockstore"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/invocation/ran"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/core/message"
	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/core/result/failure"
	fdm "github.com/storacha/go-ucanto/core/result/failure/datamodel"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	ucanhttp "github.com/storacha/go-ucanto/transport/http"
	"github.com/storacha/go-ucanto/ucan"
)

// SpaceBlobAdd adds a blob to the service. The issuer needs proof of
// `space/blob/add` delegated capability.
//
// Required delegated capability proofs: `space/blob/add`
//
// The `issuer` is the signing authority that is issuing the UCAN invocation.
//
// The `space` is the resource the invocation applies to. It is typically the
// DID of a space.
//
// Returns the multihash of the added blob and the location commitment that contains details about where the
// blob can be located, or an error if something went wrong.
func SpaceBlobAdd(ctx context.Context, content io.Reader, issuer principal.Signer, space did.DID, receiptsURL *url.URL, options ...Option) (multihash.Multihash, delegation.Delegation, error) {
	cfg, err := NewConfig(options...)
	if err != nil {
		return nil, nil, err
	}

	contentBytes, err := io.ReadAll(content)
	if err != nil {
		return nil, nil, fmt.Errorf("reading content: %w", err)
	}

	contentHash, err := multihash.Sum(contentBytes, multihash.SHA2_256, -1)
	if err != nil {
		return nil, nil, fmt.Errorf("computing content multihash: %w", err)
	}

	caveats := spaceblobcap.AddCaveats{
		Blob: captypes.Blob{
			Digest: contentHash,
			Size:   uint64(len(contentBytes)),
		},
	}

	inv, err := spaceblobcap.Add.Invoke(issuer, cfg.conn.ID(), space.String(), caveats, convertToInvocationOptions(cfg)...)
	if err != nil {
		return nil, nil, fmt.Errorf("generating invocation: %w", err)
	}

	resp, err := uclient.Execute([]invocation.Invocation{inv}, cfg.conn)
	if err != nil {
		return nil, nil, fmt.Errorf("sending invocation: %w", err)
	}

	rcptlnk, ok := resp.Get(inv.Link())
	if !ok {
		return nil, nil, fmt.Errorf("receipt not found: %s", inv.Link())
	}

	reader, err := receipt.NewReceiptReaderFromTypes[spaceblobcap.AddOk, fdm.FailureModel](spaceblobcap.AddOkType(), fdm.FailureType(), captypes.Converters...)
	if err != nil {
		return nil, nil, fmt.Errorf("generating receipt reader: %w", err)
	}

	rcpt, err := reader.Read(rcptlnk, resp.Blocks())
	if err != nil {
		return nil, nil, fmt.Errorf("reading receipt: %w", err)
	}

	_, failErr := result.Unwrap(result.MapError(rcpt.Out(), failure.FromFailureModel))
	if failErr != nil {
		return nil, nil, fmt.Errorf("blob add failed: %w", failErr)
	}

	var allocateTask, putTask, acceptTask invocation.Invocation
	legacyAccept := false
	var concludeFxs []invocation.Invocation
	for _, task := range rcpt.Fx().Fork() {
		inv, ok := task.Invocation()
		if ok {
			switch inv.Capabilities()[0].Can() {
			case blobcap.AllocateAbility:
				allocateTask = inv
			case w3sblobcap.AllocateAbility:
				if allocateTask == nil {
					allocateTask = inv
				}
			case ucancap.ConcludeAbility:
				concludeFxs = append(concludeFxs, inv)
			case httpcap.PutAbility:
				putTask = inv
			case blobcap.AcceptAbility:
				acceptTask = inv
			case w3sblobcap.AcceptAbility:
				if acceptTask == nil {
					acceptTask = inv
					legacyAccept = true
				}
			default:
				fmt.Printf("[WARN] ignoring unexpected task: %s\n", inv.Capabilities()[0].Can())
			}
		}
	}

	if allocateTask == nil || len(concludeFxs) == 0 || putTask == nil || acceptTask == nil {
		return nil, nil, fmt.Errorf("mandatory tasks not received in space/blob/add receipt")
	}

	var allocateRcpt receipt.Receipt[blobcap.AllocateOk, fdm.FailureModel]
	var legacyAllocateRcpt receipt.Receipt[w3sblobcap.AllocateOk, fdm.FailureModel]
	var putRcpt receipt.AnyReceipt
	var acceptRcpt receipt.Receipt[blobcap.AcceptOk, fdm.FailureModel]
	var legacyAcceptRcpt receipt.Receipt[w3sblobcap.AcceptOk, fdm.FailureModel]
	for _, concludeFx := range concludeFxs {
		concludeRcpt, err := getConcludeReceipt(concludeFx)
		if err != nil {
			return nil, nil, fmt.Errorf("reading ucan/conclude receipt: %w", err)
		}

		switch concludeRcpt.Ran().Link() {
		case allocateTask.Link():
			switch allocateTask.Capabilities()[0].Can() {
			case blobcap.AllocateAbility:
				allocateRcpt, err = receipt.Rebind[blobcap.AllocateOk, fdm.FailureModel](concludeRcpt, blobcap.AllocateOkType(), fdm.FailureType(), captypes.Converters...)
				if err != nil {
					return nil, nil, fmt.Errorf("bad allocate receipt in conclude fx: %w", err)
				}
			case w3sblobcap.AllocateAbility:
				legacyAllocateRcpt, err = receipt.Rebind[w3sblobcap.AllocateOk, fdm.FailureModel](concludeRcpt, w3sblobcap.AllocateOkType(), fdm.FailureType(), captypes.Converters...)
				if err != nil {
					return nil, nil, fmt.Errorf("bad allocate receipt in conclude fx: %w", err)
				}
			default:
				return nil, nil, fmt.Errorf("unexpected capability in allocate task: %s", allocateTask.Capabilities()[0].Can())
			}
		case putTask.Link():
			putRcpt = concludeRcpt
		case acceptTask.Link():
			switch allocateTask.Capabilities()[0].Can() {
			case blobcap.AcceptAbility:
				acceptRcpt, err = receipt.Rebind[blobcap.AcceptOk, fdm.FailureModel](concludeRcpt, blobcap.AcceptOkType(), fdm.FailureType(), captypes.Converters...)
				if err != nil {
					return nil, nil, fmt.Errorf("bad accept receipt in conclude fx: %w", err)
				}
			case w3sblobcap.AcceptAbility:
				legacyAcceptRcpt, err = receipt.Rebind[w3sblobcap.AcceptOk, fdm.FailureModel](concludeRcpt, w3sblobcap.AcceptOkType(), fdm.FailureType(), captypes.Converters...)
				if err != nil {
					return nil, nil, fmt.Errorf("bad accept receipt in conclude fx: %w", err)
				}
			default:
				return nil, nil, fmt.Errorf("unexpected capability in accept task: %s", acceptTask.Capabilities()[0].Can())
			}
		default:
			fmt.Printf("[WARN] ignoring receipt for unexpected task: %s\n", concludeRcpt.Ran().Link())
		}
	}

	if allocateRcpt == nil && legacyAllocateRcpt == nil {
		return nil, nil, fmt.Errorf("mandatory receipts not received in space/blob/add receipt")
	}

	var url *url.URL
	var headers http.Header
	if allocateRcpt != nil {
		allocateOk, err := result.Unwrap(result.MapError(allocateRcpt.Out(), failure.FromFailureModel))
		if err != nil {
			return nil, nil, fmt.Errorf("blob allocation failed: %w", err)
		}

		address := allocateOk.Address
		if address != nil {
			url = &address.URL
			headers = address.Headers
		}
	} else {
		allocateOk, err := result.Unwrap(result.MapError(legacyAllocateRcpt.Out(), failure.FromFailureModel))
		if err != nil {
			return nil, nil, fmt.Errorf("blob allocation failed: %w", err)
		}

		address := allocateOk.Address
		if address != nil {
			url = &address.URL
			headers = address.Headers
		}
	}

	if url != nil && headers != nil {
		if err := putBlob(ctx, url, headers, contentBytes); err != nil {
			return nil, nil, fmt.Errorf("putting blob: %w", err)
		}
	}

	// invoke `ucan/conclude` with `http/put` receipt
	if putRcpt == nil {
		if err := sendPutReceipt(putTask, issuer, cfg.conn.ID(), cfg.conn); err != nil {
			return nil, nil, fmt.Errorf("sending put receipt: %w", err)
		}
	} else {
		putOk, _ := result.Unwrap(putRcpt.Out())
		if putOk == nil {
			if err := sendPutReceipt(putTask, issuer, cfg.conn.ID(), cfg.conn); err != nil {
				return nil, nil, fmt.Errorf("sending put receipt: %w", err)
			}
		}
	}

	// ensure the blob has been accepted
	var anyAcceptRcpt receipt.AnyReceipt
	var site ucan.Link
	var rcptBlocks iter.Seq2[ipld.Block, error]
	if acceptRcpt == nil && legacyAcceptRcpt == nil {
		anyAcceptRcpt, err = pollAccept(ctx, acceptTask.Link(), cfg.conn, receiptsURL)
		if err != nil {
			return nil, nil, fmt.Errorf("polling accept: %w", err)
		}
	} else if acceptRcpt != nil {
		acceptOk, failErr := result.Unwrap(result.MapError(acceptRcpt.Out(), failure.FromFailureModel))
		if failErr != nil {
			anyAcceptRcpt, err = pollAccept(ctx, acceptTask.Link(), cfg.conn, receiptsURL)
			if err != nil {
				return nil, nil, fmt.Errorf("polling accept: %w", err)
			}
		} else {
			site = acceptOk.Site
			rcptBlocks = acceptRcpt.Blocks()
		}
	} else if legacyAcceptRcpt != nil {
		acceptOk, failErr := result.Unwrap(result.MapError(legacyAcceptRcpt.Out(), failure.FromFailureModel))
		if failErr != nil {
			anyAcceptRcpt, err = pollAccept(ctx, acceptTask.Link(), cfg.conn, receiptsURL)
			if err != nil {
				return nil, nil, fmt.Errorf("polling accept: %w", err)
			}
		} else {
			site = acceptOk.Site
			rcptBlocks = legacyAcceptRcpt.Blocks()
		}
	}

	if site == nil {
		if !legacyAccept {
			acceptRcpt, err = receipt.Rebind[blobcap.AcceptOk, fdm.FailureModel](anyAcceptRcpt, blobcap.AcceptOkType(), fdm.FailureType(), captypes.Converters...)
			if err != nil {
				return nil, nil, fmt.Errorf("fetching accept receipt: %w", err)
			}

			acceptOk, err := result.Unwrap(result.MapError(acceptRcpt.Out(), failure.FromFailureModel))
			if err != nil {
				return nil, nil, fmt.Errorf("blob/accept failed: %w", err)
			}

			site = acceptOk.Site
			rcptBlocks = acceptRcpt.Blocks()
		} else {
			legacyAcceptRcpt, err = receipt.Rebind[w3sblobcap.AcceptOk, fdm.FailureModel](anyAcceptRcpt, w3sblobcap.AcceptOkType(), fdm.FailureType(), captypes.Converters...)
			if err != nil {
				return nil, nil, fmt.Errorf("fetching legacy accept receipt: %w", err)
			}

			acceptOk, err := result.Unwrap(result.MapError(legacyAcceptRcpt.Out(), failure.FromFailureModel))
			if err != nil {
				return nil, nil, fmt.Errorf("web3.storage/blob/accept failed: %w", err)
			}

			site = acceptOk.Site
			rcptBlocks = legacyAcceptRcpt.Blocks()
		}
	}

	locationBlocks, err := blockstore.NewBlockStore(blockstore.WithBlocksIterator(rcptBlocks))
	if err != nil {
		return nil, nil, fmt.Errorf("reading location commitment blocks: %w", err)
	}

	location, err := delegation.NewDelegationView(site, locationBlocks)
	if err != nil {
		return nil, nil, fmt.Errorf("creating location delegation: %w", err)
	}

	return contentHash, location, nil
}

func getConcludeReceipt(concludeFx invocation.Invocation) (receipt.AnyReceipt, error) {
	concludeNb, fail := ucancap.ConcludeCaveatsReader.Read(concludeFx.Capabilities()[0].Nb())
	if fail != nil {
		return nil, fmt.Errorf("invalid conclude receipt: %w", fail)
	}

	reader := receipt.NewAnyReceiptReader(captypes.Converters...)
	rcpt, err := reader.Read(concludeNb.Receipt, concludeFx.Blocks())
	if err != nil {
		return nil, fmt.Errorf("reading receipt: %w", err)
	}

	return rcpt, nil
}

func putBlob(ctx context.Context, url *url.URL, headers http.Header, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url.String(), bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("creating upload request: %w", err)
	}

	for k, v := range headers {
		req.Header.Set(k, v[0])
	}

	// TODO: custom HTTP client with timeout
	// TODO: retries
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("uploading blob: %w", err)
	}
	io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("uploading blob: %s", resp.Status)
	}

	return nil
}

func sendPutReceipt(putTask invocation.Invocation, issuer ucan.Signer, audience ucan.Principal, conn uclient.Connection) error {
	if len(putTask.Facts()) != 1 {
		return fmt.Errorf("invalid put facts, wanted 1 fact but got %d", len(putTask.Facts()))
	}

	if _, ok := putTask.Facts()[0]["keys"]; !ok {
		return fmt.Errorf("invalid put facts, missing 'keys' field")
	}

	putKeysNode, ok := putTask.Facts()[0]["keys"].(ipld.Node)
	if !ok {
		return fmt.Errorf("invalid put facts, 'keys' field is not a node")
	}

	// TODO: define a schema and use bindnode.Rebind rather than doing this manually
	var id did.DID
	keys := map[string][]byte{}
	it := putKeysNode.MapIterator()
	for !it.Done() {
		k, v, err := it.Next()
		if err != nil {
			return fmt.Errorf("invalid put facts: %w", err)
		}

		kStr, err := k.AsString()
		if err != nil {
			return fmt.Errorf("invalid put facts: %w", err)
		}

		switch kStr {
		case "id":
			// v is a did string
			vStr, err := v.AsString()
			if err != nil {
				return fmt.Errorf("invalid put facts: %w", err)
			}

			id, err = did.Parse(vStr)
			if err != nil {
				return fmt.Errorf("invalid put facts: %w", err)
			}
		case "keys":
			// v is a did to key map
			it2 := v.MapIterator()
			for !it2.Done() {
				k2, v2, err := it2.Next()
				if err != nil {
					return fmt.Errorf("invalid put facts: %w", err)
				}

				k2Str, err := k2.AsString()
				if err != nil {
					return fmt.Errorf("invalid put facts: %w", err)
				}

				v2Bytes, err := v2.AsBytes()
				if err != nil {
					return fmt.Errorf("invalid put facts: %w", err)
				}

				keys[k2Str] = v2Bytes
			}
		}
	}

	derivedKey, ok := keys[id.String()]
	if !ok {
		return fmt.Errorf("invalid put facts: missing key for %s", id.String())
	}

	derivedSigner, err := signer.Decode(derivedKey)
	if err != nil {
		return fmt.Errorf("deriving signer: %w", err)
	}

	putRcpt, err := receipt.Issue(derivedSigner, result.Ok[httpcap.PutOk, ipld.Builder](httpcap.PutOk{}), ran.FromInvocation(putTask))
	if err != nil {
		return fmt.Errorf("generating receipt: %w", err)
	}

	// var concludeFacts []ucan.FactBuilder
	// for rcptBlock, err := range putRcpt.Blocks() {
	// 	if err != nil {
	// 		return nil, nil, fmt.Errorf("getting receipt block: %w", err)
	// 	}

	// 	concludeFacts = append(concludeFacts, rcptBlock.Link())
	// }

	httpPutConcludeInvocation, err := ucancap.Conclude.Invoke(
		issuer,
		audience,
		issuer.DID().String(),
		ucancap.ConcludeCaveats{
			Receipt: putRcpt.Root().Link(),
		},
		// delegation.WithFacts(concludeFacts),
		delegation.WithNoExpiration(),
	)
	if err != nil {
		return fmt.Errorf("generating invocation: %w", err)
	}

	// attach the receipt to the conclude invocation
	for rcptBlock, err := range putRcpt.Blocks() {
		if err != nil {
			return fmt.Errorf("getting receipt block: %w", err)
		}

		httpPutConcludeInvocation.Attach(rcptBlock)
	}

	resp, err := uclient.Execute([]invocation.Invocation{httpPutConcludeInvocation}, conn)
	if err != nil {
		return fmt.Errorf("executing conclude invocation: %w", err)
	}

	rcptlnk, ok := resp.Get(httpPutConcludeInvocation.Link())
	if !ok {
		return fmt.Errorf("receipt not found: %s", httpPutConcludeInvocation.Link())
	}

	reader, err := receipt.NewReceiptReaderFromTypes[ucancap.ConcludeOk, fdm.FailureModel](ucancap.ConcludeOkType(), fdm.FailureType(), captypes.Converters...)
	if err != nil {
		return fmt.Errorf("generating receipt reader: %w", err)
	}

	rcpt, err := reader.Read(rcptlnk, resp.Blocks())
	if err != nil {
		return fmt.Errorf("reading receipt: %w", err)
	}

	_, err = result.Unwrap(result.MapError(rcpt.Out(), failure.FromFailureModel))
	if err != nil {
		return fmt.Errorf("ucan/conclude failed: %w", err)
	}

	return nil
}

func pollAccept(ctx context.Context, acceptTaskLink ucan.Link, conn uclient.Connection, receiptsURL *url.URL) (receipt.AnyReceipt, error) {
	receiptURL := receiptsURL.JoinPath(acceptTaskLink.String())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, receiptURL.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("creating get request: %w", err)
	}

	// TODO: custom HTTP client with timeout
	client := &http.Client{}

	var msg message.AgentMessage
	for retry := 0; retry < 5 && msg == nil; retry++ {
		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("polling receipts endpoint: %w", err)
		}
		defer resp.Body.Close()

		respBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("reading response body: %w", err)
		}

		switch resp.StatusCode {
		case http.StatusOK:
			msg, err = conn.Codec().Decode(ucanhttp.NewHTTPResponse(resp.StatusCode, bytes.NewReader(respBytes), resp.Header))
			if err != nil {
				return nil, fmt.Errorf("decoding message: %w", err)
			}

		case http.StatusNotFound:
			time.Sleep(1 * time.Second)

		default:
			return nil, fmt.Errorf("polling receipts endpoint: %s", resp.Status)
		}
	}

	if msg == nil {
		return nil, fmt.Errorf("accept receipt not found: %s", acceptTaskLink)
	}

	rcptlnk, ok := msg.Get(acceptTaskLink)
	if !ok {
		return nil, fmt.Errorf("accept receipt not found: %s", acceptTaskLink)
	}

	reader := receipt.NewAnyReceiptReader(captypes.Converters...)

	return reader.Read(rcptlnk, msg.Blocks())
}
