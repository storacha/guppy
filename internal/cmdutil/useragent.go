package cmdutil

import (
	"fmt"
	"net/http"

	"github.com/storacha/guppy/pkg/build"
)

type userAgentTransport struct {
	userAgent string
	base      http.RoundTripper
}

func (t *userAgentTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Set("User-Agent", t.userAgent)
	return t.base.RoundTrip(req)
}

func newUserAgentTransport(base http.RoundTripper) http.RoundTripper {
	return &userAgentTransport{
		userAgent: fmt.Sprintf("guppy/%s", build.Version),
		base:      base,
	}
}
