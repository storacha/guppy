package testutil

import (
	"fmt"
	"net/http"
	"net/url"
)

// FailoverTransport is a RoundTripper that fails for a specific URL once
type FailoverTransport struct {
	FailURL url.URL
	Next    http.RoundTripper
	Calls   int
}

func (t *FailoverTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.Calls++
	if req.URL.Host == t.FailURL.Host && req.URL.Path == t.FailURL.Path {
		return nil, fmt.Errorf("simulated connection failure for %s", req.URL.String())
	}
	return t.Next.RoundTrip(req)
}
