package gateway

import (
	"fmt"
	"strings"

	"github.com/labstack/gommon/color"
	"github.com/storacha/go-ucanto/did"
)

func banner(version string, port int, id did.DID, spaces []did.DID, hosts []string) string {
	var sb strings.Builder
	sb.WriteString("Server ")
	sb.WriteString(color.Grey(id.String()))

	if len(spaces) > 1 {
		sb.WriteString("\nSpaces")
		for i, space := range spaces {
			if i == 0 {
				sb.WriteString(" ")
			} else {
				sb.WriteString("       ")
			}
			sb.WriteString(color.Grey(space.String()))
			if i < len(spaces)-1 {
				sb.WriteString("\n")
			}
		}
	} else {
		sb.WriteString("\nSpace  ")
		sb.WriteString(color.Grey(spaces[0].String()))
	}

	if len(hosts) > 0 {
		if len(hosts) > 1 {
			sb.WriteString("\nHosts")
			for i, domain := range hosts {
				if i == 0 {
					sb.WriteString("  ")
				} else {
					sb.WriteString("       ")
				}
				sb.WriteString(color.Grey(domain))
				if i < len(hosts)-1 {
					sb.WriteString("\n")
				}
			}
		} else {
			sb.WriteString("\nHost   ")
			sb.WriteString(color.Grey(hosts[0]))
		}
	}

	return fmt.Sprintf(
		`
%s ▄▖           
  ▌ ▌▌▛▌▌▌▌▀▌▌▌
  ▙▌▙▌▙▌▚▚▘█▌▙▌
      ▌      ▄▌ %s

High performance IPFS Gateway
%s
------------------------------
%s
------------------------------
⇨ HTTP server started on %s`,
		color.Cyan("⬢"),
		color.Red(version),
		color.Blue("https://storacha.network"),
		sb.String(),
		color.Green(fmt.Sprintf("http://localhost:%d", port)),
	)
}
