package receipt

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"net/http"
	"net/url"
	"time"

	"github.com/storacha/go-libstoracha/capabilities/types"
	ucancap "github.com/storacha/go-libstoracha/capabilities/ucan"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/core/message"
	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/go-ucanto/transport"
	"github.com/storacha/go-ucanto/transport/car"
	ucanhttp "github.com/storacha/go-ucanto/transport/http"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/go-ucanto/validator"
)

var ErrNotFound = errors.New("receipt not found")

var (
	PollInterval = time.Second
	PollRetries  = 10
)

type Client struct {
	endpoint *url.URL
	client   *http.Client
	codec    transport.ResponseDecoder
}

type Option func(c *Client)

func WithCodec(codec transport.ResponseDecoder) Option {
	return func(c *Client) {
		c.codec = codec
	}
}

func WithHTTPClient(client *http.Client) Option {
	return func(c *Client) {
		c.client = client
	}
}

func New(endpoint *url.URL, options ...Option) *Client {
	c := Client{
		endpoint: endpoint,
		codec:    car.NewOutboundCodec(),
	}
	for _, o := range options {
		o(&c)
	}
	if c.client == nil {
		c.client = http.DefaultClient
	}
	return &c
}

// Fetch a receipt from the receipt API. Returns [ErrNotFound] if the API
// responds with [http.StatusNotFound].
func (c *Client) Fetch(ctx context.Context, task ucan.Link) (receipt.AnyReceipt, error) {
	receiptURL := c.endpoint.JoinPath(task.String())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, receiptURL.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("creating get request: %w", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("doing receipts request: %w", err)
	}
	defer resp.Body.Close()

	var msg message.AgentMessage
	switch resp.StatusCode {
	case http.StatusOK:
		msg, err = c.codec.Decode(ucanhttp.NewResponse(resp.StatusCode, resp.Body, resp.Header))
		if err != nil {
			return nil, fmt.Errorf("decoding message: %w", err)
		}
	case http.StatusNotFound:
		return nil, ErrNotFound
	default:
		return nil, fmt.Errorf("unexpected status: %s", resp.Status)
	}

	rcptlnk, ok := msg.Get(task)
	if !ok {
		// This could be an agent message that contains a ucan/conclude invocation
		// that contains the receipt.
		for _, root := range msg.Invocations() {
			inv, ok, err := msg.Invocation(root)
			if err != nil || !ok || len(inv.Capabilities()) != 1 {
				continue
			}
			rcpt, err := extractConcludeReceipt(task, inv, msg.Blocks())
			if err != nil {
				continue
			}
			return rcpt, nil
		}
		// This could be an agent message that contains a receipt for a
		// ucan/conclude invocation that contains the receipt.
		for _, root := range msg.Receipts() {
			concludeRcpt, ok, err := msg.Receipt(root)
			if err != nil || !ok {
				continue
			}
			inv, ok := concludeRcpt.Ran().Invocation()
			if !ok {
				continue
			}
			rcpt, err := extractConcludeReceipt(task, inv, msg.Blocks())
			if err != nil {
				continue
			}
			return rcpt, nil
		}
		return nil, errors.New("receipt not found in agent message")
	}

	reader := receipt.NewAnyReceiptReader(types.Converters...)
	return reader.Read(rcptlnk, msg.Blocks())
}

// extractConcludeReceipt extracts the receipt for the passed task from the
// passed invocation, if it is an invocation of `ucan/conclude`.
func extractConcludeReceipt(task ipld.Link, inv invocation.Invocation, blocks iter.Seq2[ipld.Block, error]) (receipt.AnyReceipt, error) {
	var err error
	cap := inv.Capabilities()[0]
	match, err := ucancap.Conclude.Match(validator.NewSource(cap, inv))
	if err != nil {
		return nil, err
	}
	reader := receipt.NewAnyReceiptReader(types.Converters...)
	rcpt, err := reader.Read(match.Value().Nb().Receipt, blocks)
	if err != nil {
		return nil, err
	}
	if rcpt.Ran().Link().String() != task.String() {
		return nil, fmt.Errorf("receipt is not for task: %s", task)
	}
	return rcpt, nil
}

type pollConfig struct {
	interval *time.Duration
	retries  *int
}

type PollOption func(opt *pollConfig)

// WithInterval configures the time to wait between poll requests. The default
// is [PollInterval].
func WithInterval(interval time.Duration) PollOption {
	return func(opt *pollConfig) {
		opt.interval = &interval
	}
}

// WithRetries configures the maximum number of times that Poll will attempt to
// fetch a receipt. The default is [PollRetries] requests. Set it to -1 to poll
// until a non-404 response is encountered.
func WithRetries(n int) PollOption {
	return func(opt *pollConfig) {
		opt.retries = &n
	}
}

// Poll attempts to fetch a receipt from the endpoint until a non-404 response
// is encountered or until the configured maximum retries are made.
func (c *Client) Poll(ctx context.Context, task ucan.Link, options ...PollOption) (receipt.AnyReceipt, error) {
	conf := pollConfig{}
	for _, o := range options {
		o(&conf)
	}
	if conf.interval == nil {
		conf.interval = &PollInterval
	}
	if conf.retries == nil {
		conf.retries = &PollRetries
	}

	attempts := 0
	for {
		rcpt, err := c.Fetch(ctx, task)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return nil, err
		}
		if err == nil {
			return rcpt, nil
		}

		attempts++
		if *conf.retries > -1 && (attempts-1) >= *conf.retries {
			return nil, fmt.Errorf("receipt for %s was not found after %d attempts", task, attempts)
		}

		// wait for the configured interval, or the context to be canceled
		sleep, cancel := context.WithTimeout(ctx, *conf.interval)
		<-sleep.Done()
		cancel()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
}
