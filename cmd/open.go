package cmd

import (
	"fmt"
	"os/exec"
	"runtime"

	"github.com/ipfs/go-cid"
	"github.com/spf13/cobra"
)

var openFlags struct {
	printOnly bool
}

var openCmd = &cobra.Command{
	Use:   "open <cid>",
	Short: "Open a CID on https://storacha.link",
	Long:  "Opens the given CID on https://storacha.link in your default browser.",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		cstr := args[0]
		if _, err := cid.Decode(cstr); err != nil {
			return fmt.Errorf("invalid CID: %w", err)
		}

		url := fmt.Sprintf("https://storacha.link/%s", cstr)

		if openFlags.printOnly {
			fmt.Fprintln(cmd.OutOrStdout(), url)
			return nil
		}

		var openCmd *exec.Cmd
		switch runtime.GOOS {
		case "darwin":
			openCmd = exec.Command("open", url)
		case "windows":
			openCmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
		default:
			openCmd = exec.Command("xdg-open", url)
		}

		openCmd.Stdout = cmd.OutOrStdout()
		openCmd.Stderr = cmd.OutOrStderr()

		if err := openCmd.Run(); err != nil {
			return fmt.Errorf("failed to open browser: %w", err)
		}

		return nil
	},
}

func init() {
	rootCmd.AddCommand(openCmd)
	openCmd.Flags().BoolVar(&openFlags.printOnly, "print", false, "Print the storacha.link URL instead of opening it in a browser")
}
