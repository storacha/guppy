package cmd

import (
	"fmt"
	"os/exec"
	"runtime"

	"github.com/mitchellh/go-wordwrap"
	"github.com/spf13/cobra"
)

var openCmd = &cobra.Command{
	Use:   "open <cid>",
	Short: "Open CID on https://storacha.link",
	Long: wordwrap.WrapString(
		"Opens the given CID in the default browser using https://storacha.link.",
		80),
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		cid := args[0]
		url := fmt.Sprintf("https://storacha.link/ipfs/%s", cid)

		fmt.Printf("Opening %s\n", url)

		var err error
		switch runtime.GOOS {
		case "linux":
			err = exec.Command("xdg-open", url).Start()
		case "windows":
			err = exec.Command("rundll32", "url.dll,FileProtocolHandler", url).Start()
		case "darwin":
			err = exec.Command("open", url).Start()
		default:
			err = fmt.Errorf("unsupported platform")
		}

		return err
	},
}

func init() {
	rootCmd.AddCommand(openCmd)
}
