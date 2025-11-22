package space

import (
	"fmt"

	"github.com/mitchellh/go-wordwrap"
	"github.com/spf13/cobra"

	"github.com/storacha/guppy/pkg/config"
	"github.com/storacha/guppy/pkg/repo"
)

func init() {
	registerCmd.Flags().StringVar(&spaceFlags.spaceName, "space", "", "Space name or DID to register (defaults to current space)")
	registerCmd.Flags().StringVarP(&spaceFlags.output, "output", "o", "", "Output path for delegation file")
}

var registerCmd = &cobra.Command{
	Use:   "register <email>",
	Short: "Register a locally-created space with Storacha",
	Long: wordwrap.WrapString(
		"Creates a UCAN delegation from your local space to your email address, "+
			"allowing you to import the space into the Storacha console. After running "+
			"this command, you'll need to import the generated delegation file via the "+
			"Storacha web console.",
		80),
	Example: fmt.Sprint(
		"  guppy space register alice@example.com\n" +
			"  guppy space register alice@example.com --space myproject",
	),
	Args: cobra.ExactArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		email := args[0]

		cfg, err := config.Load()
		if err != nil {
			return fmt.Errorf("loading config: %w", err)
		}

		r, err := repo.Open(cfg.Repo)
		if err != nil {
			return fmt.Errorf("opening repo: %w", err)
		}

		spaceStore, err := r.SpaceStore()
		if err != nil {
			return fmt.Errorf("opening space store: %w", err)
		}

		// Get space to register
		var space repo.Space
		if spaceFlags.spaceName != "" {
			space, err = spaceStore.Get(spaceFlags.spaceName)
			if err != nil {
				return err
			}
		} else {
			space, err = spaceStore.Current()
			if err != nil {
				return fmt.Errorf("no current space - specify with --space or use 'guppy space use': %w", err)
			}
		}

		if space.Registered {
			cmd.Printf("Space '%s' is already registered\n", space.Name)
			return nil
		}

		cmd.Printf("Creating delegation for space '%s' to email %s...\n", space.Name, email)

		// Create delegation from space to email
		del, err := spaceStore.RegisterSpace(space, email)
		if err != nil {
			return fmt.Errorf("failed to create delegation: %w", err)
		}

		// Save delegation to file
		outputPath := spaceFlags.output
		if outputPath == "" {
			outputPath = fmt.Sprintf("%s-delegation.ucan", space.Name)
		}

		if err := saveDelegationToFile(del, outputPath); err != nil {
			return fmt.Errorf("failed to save delegation: %w", err)
		}

		// Success message with clear instructions
		cmd.Printf("\n Delegation created successfully!\n")
		cmd.Printf(" Saved to: %s\n\n", outputPath)
		cmd.Printf("Next steps to import your space:\n")
		cmd.Printf("  1. Visit: https://console.storacha.network\n")
		cmd.Printf("  2. Click the 'IMPORT' button (top right)\n")
		cmd.Printf("  3. Upload the delegation file: %s\n", outputPath)
		cmd.Printf("  4. Your space '%s' will appear in the console!\n\n", space.Name)
		cmd.Printf("Space DID: %s\n", space.DID)

		return nil
	},
}
