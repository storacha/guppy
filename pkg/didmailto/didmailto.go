package didmailto

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/storacha/go-ucanto/did"
)

// FromEmail converts an email address to a `did:mailto:` DID.
func FromEmail(email string) (did.DID, error) {
	parts := strings.Split(email, "@")
	if len(parts) != 2 {
		return did.DID{}, fmt.Errorf("invalid email address %q", email)
	}
	return did.Parse("did:mailto:" + parts[1] + ":" + url.QueryEscape(parts[0]))
}
