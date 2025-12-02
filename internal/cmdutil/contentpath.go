package cmdutil

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/ipfs/go-cid"
)

// ContentPath parses a content path string and returns the CID and subpath. A
// content path can take several forms:
//
//   - /ipfs/<cid>[/<subpath>]
//   - ipfs://<cid>[/<subpath>]
//   - <cid>[/<subpath>]
//
// The subpath is returned with no leading `/`. If no subpath is specified in
// the input, an empty string is returned for it.
func ContentPath(pathStr string) (cid.Cid, string, error) {
	switch {
	case strings.HasPrefix(pathStr, "/"):
		cidAndSubpath, ok := strings.CutPrefix(pathStr, "/ipfs/")
		if !ok {
			return cid.Undef, "", fmt.Errorf("invalid path, only /ipfs/ is supported: %q", pathStr)
		}
		cidStr, subpath, _ := strings.Cut(cidAndSubpath, "/")
		pathCID, err := cid.Parse(cidStr)
		if err != nil {
			return cid.Undef, "", fmt.Errorf("parsing CID %q from IPFS path: %w", cidStr, err)
		}
		return pathCID, subpath, nil

	case strings.Contains(pathStr, "://"):
		pathURL, err := url.Parse(pathStr)
		if err != nil {
			return cid.Undef, "", fmt.Errorf("parsing URL %q: %w", pathStr, err)
		}
		if pathURL.Scheme != "ipfs" {
			return cid.Undef, "", fmt.Errorf("invalid URI, only ipfs:// is supported: %q", pathStr)
		}
		pathCID, err := cid.Parse(pathURL.Host)
		if err != nil {
			return cid.Undef, "", fmt.Errorf("parsing CID %q from IPFS URL: %w", pathURL.Host, err)
		}
		subpath, _ := strings.CutPrefix(pathURL.Path, "/")
		return pathCID, subpath, nil

	default:
		cidStr, subpath, _ := strings.Cut(pathStr, "/")
		pathCID, err := cid.Parse(cidStr)
		if err != nil {
			return cid.Undef, "", fmt.Errorf("parsing CID %q: %w", pathStr, err)
		}
		return pathCID, subpath, nil
	}
}
