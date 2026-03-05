package client

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/ipld/go-ipld-prime"
	uclient "github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/ucan"

	"github.com/storacha/guppy/pkg/agentstore"
	"github.com/storacha/guppy/pkg/client/nodevalue"
)

const UsageReportCan = "usage/report"

type UsagePeriod struct {
	From int64 `json:"from"`
	To   int64 `json:"to"`
}

type UsageCaveat struct {
	Period UsagePeriod `json:"period"`
}

func (u UsageCaveat) ToIPLD() (ipld.Node, error) {
	return nodevalue.FromAny(u)
}

type UsageReport struct {
	Provider string
	Size     uint64
}

func (c *Client) UsageReport(ctx context.Context, space did.DID, from, to time.Time) (map[string]UsageReport, error) {
	caveat := UsageCaveat{
		Period: UsagePeriod{
			From: from.Unix(),
			To:   to.Unix(),
		},
	}

	cap := ucan.NewCapability(UsageReportCan, space.String(), caveat)

	delegations, err := c.Proofs(
		agentstore.CapabilityQuery{Can: UsageReportCan, With: space.String()},
		agentstore.CapabilityQuery{Can: "*", With: space.String()},
	)
	if err != nil {
		return nil, fmt.Errorf("finding proofs: %w", err)
	}

	if len(delegations) == 0 {
		return nil, fmt.Errorf("no authorizations found for space %s", space)
	}

	var proofs []delegation.Proof
	for _, d := range delegations {
		proofs = append(proofs, delegation.FromDelegation(d))
	}

	inv, err := invocation.Invoke(
		c.Issuer(),
		c.Connection().ID(),
		cap,
		delegation.WithProof(proofs...),
	)
	if err != nil {
		return nil, fmt.Errorf("creating invocation: %w", err)
	}

	resp, err := uclient.Execute(ctx, []invocation.Invocation{inv}, c.Connection())
	if err != nil {
		return nil, fmt.Errorf("executing invocation: %w", err)
	}

	rcptLink, ok := resp.Get(inv.Link())
	if !ok {
		return nil, fmt.Errorf("receipt not found")
	}

	anyRcpt, err := receipt.NewAnyReceiptReader().Read(rcptLink, resp.Blocks())
	if err != nil {
		return nil, fmt.Errorf("reading receipt: %w", err)
	}

	okNode, errNode := result.Unwrap(anyRcpt.Out())

	if errNode != nil {
		val, _ := nodevalue.NodeValue(errNode)
		if errMap, ok := val.(map[string]any); ok {
			if msg, ok := errMap["message"].(string); ok && msg != "" {
				return nil, fmt.Errorf("server error: %s", msg)
			}
		}
		return nil, fmt.Errorf("server error: %v", val)
	}

	val, err := nodevalue.NodeValue(okNode)
	if err != nil {
		return nil, fmt.Errorf("decoding result: %w", err)
	}

	reports := make(map[string]UsageReport)
	if m, ok := val.(map[string]any); ok {
		if innerOk, hasOk := m["ok"].(map[string]any); hasOk {
			m = innerOk
		}
		for providerDID, data := range m {
			if pdMap, ok := data.(map[string]any); ok {
				report := UsageReport{Provider: providerDID}

				if sizeMap, ok := pdMap["size"].(map[string]any); ok {
					report.Size = parseUint64(sizeMap["final"])
				} else if sizeVal, ok := pdMap["size"]; ok {
					report.Size = parseUint64(sizeVal)
				}

				reports[providerDID] = report
			}
		}
	}

	return reports, nil
}

func parseUint64(v any) uint64 {
	switch n := v.(type) {
	case int:
		return uint64(n)
	case int32:
		return uint64(n)
	case int64:
		return uint64(n)
	case uint:
		return uint64(n)
	case uint32:
		return uint64(n)
	case uint64:
		return n
	case float32:
		return uint64(n)
	case float64:
		return uint64(n)
	default:
		s := fmt.Sprintf("%v", v)
		if val, err := strconv.ParseUint(s, 10, 64); err == nil {
			return val
		}
	}
	return 0
}
