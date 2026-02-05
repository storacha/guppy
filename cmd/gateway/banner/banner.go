package banner

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/storacha/go-ucanto/did"
)

const gupway = `▄▖           
▌ ▌▌▛▌▌▌▌▀▌▌▌
▙▌▙▌▙▌▚▚▘█▌▙▌
    ▌      ▄▌`

var (
	// Storacha branding color palette
	yellow1 = lipgloss.Color("#FFC83F")
	red1    = lipgloss.Color("#E91315")
	blue1   = lipgloss.Color("#0176CE")
	blue2   = lipgloss.Color("#BDE0FF")

	// Mixed equal parts yellow1 and red1 in LAB space
	yellow1Red1 = lipgloss.Color("#f88128")

	// Banner styles
	hexStyle       = lipgloss.NewStyle().Foreground(lipgloss.Color("6"))
	gupwayStyle    = lipgloss.NewStyle().Padding(0, 1)
	versionStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("9"))
	networkStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("4"))
	didStyle       = lipgloss.NewStyle().Foreground(lipgloss.Color("8"))
	serverURLStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("2"))

	// Portal styles
	outerRingStyle  = lipgloss.NewStyle().Foreground(yellow1)
	secondRingStyle = lipgloss.NewStyle().Foreground(yellow1Red1)
	centerRingStyle = lipgloss.NewStyle().Foreground(red1).Faint(true)
	fishStyle       = lipgloss.NewStyle().Foreground(blue1).Bold(true)
	bubblesStyle    = lipgloss.NewStyle().Foreground(blue2).Faint(true)
)

func Banner(version string, port int, id did.DID, spaces []did.DID, hosts []string) string {
	right := lipgloss.JoinVertical(lipgloss.Left,
		portal(),
		versionStyle.Render(version),
	)

	left := lipgloss.JoinHorizontal(lipgloss.Top,
		hexStyle.Render("⬢"),
		gupwayStyle.Render(gupway),
	)

	bannerTop := lipgloss.JoinHorizontal(lipgloss.Bottom, left, right)

	return lipgloss.JoinVertical(lipgloss.Left,
		bannerTop,
		networkStyle.Render("https://storacha.network"),
		"------------------------------",
		serverAndSpaces(id, spaces, hosts),
		"------------------------------",
		fmt.Sprintf("⇨ HTTP server started on %s", serverURLStyle.Render(fmt.Sprintf("http://localhost:%d", port))),
	)
}

func portal() string {
	palette := []lipgloss.Style{
		outerRingStyle,
		secondRingStyle,
		centerRingStyle,
		fishStyle,
		bubblesStyle,
	}

	var portal = "" +
		" ⌢ 𝚘 \n" +
		"⎛◠⎞ ◦ \n" +
		"⎜⬯▸⬬º \n" +
		"⎝◡⎠"

	bitmap := []int{
		0, 1, 0, 5, 0,
		1, 2, 1, 0, 5, 0,
		1, 3, 4, 4, 5, 0,
		1, 2, 1, 0, 0,
	}

	return styleBitmap(portal, bitmap, palette)
}

func serverAndSpaces(id did.DID, spaces []did.DID, hosts []string) string {
	var sb strings.Builder
	sb.WriteString("Server ")
	sb.WriteString(didStyle.Render(id.String()))

	if len(spaces) > 1 {
		sb.WriteString("\nSpaces")
		for i, space := range spaces {
			if i == 0 {
				sb.WriteString(" ")
			} else {
				sb.WriteString("       ")
			}
			sb.WriteString(didStyle.Render(space.String()))
			if i < len(spaces)-1 {
				sb.WriteString("\n")
			}
		}
	} else {
		sb.WriteString("\nSpace  ")
		sb.WriteString(didStyle.Render(spaces[0].String()))
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
				sb.WriteString(didStyle.Render(domain))
				if i < len(hosts)-1 {
					sb.WriteString("\n")
				}
			}
		} else {
			sb.WriteString("\nHost   ")
			sb.WriteString(didStyle.Render(hosts[0]))
		}
	}
	return sb.String()
}

// Styles the string s according to the style numbers in bitmap taken as indices
// into palette. A style number of 0 means no style. Consecutive characters with
// the same non-zero style number are grouped into a single range for
// efficiency.
func styleBitmap(s string, bitmap []int, palette []lipgloss.Style) string {
	var ranges []lipgloss.Range
	for i, styleNum := range bitmap {
		if styleNum > 0 {
			if i > 0 && styleNum == bitmap[i-1] {
				ranges[len(ranges)-1].End++
			} else {
				ranges = append(ranges, lipgloss.NewRange(i, i+1, palette[styleNum-1]))
			}
		}
	}

	return lipgloss.StyleRanges(s, ranges...)
}
