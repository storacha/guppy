package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mitchellh/go-wordwrap"
	"github.com/multiformats/go-multibase"
	"github.com/spf13/cobra"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/guppy/pkg/didmailto"
)

type Space struct {
	DID         string    `json:"did"`
	Name        string    `json:"name"`
	Created     time.Time `json:"created"`
	Registered  bool      `json:"registered,omitempty"`
	Description string    `json:"description,omitempty"`
	PrivateKey  string    `json:"privateKey,omitempty"`
}

type SpaceConfig struct {
	Current string           `json:"current,omitempty"`
	Spaces  map[string]Space `json:"spaces"`
}

var spacePrimeFlags struct {
	description string
	spaceName   string
	output      string
	json        bool
}

var spacePrimeCmd = &cobra.Command{
	Use:   "space-prime",
	Short: "Manage spaces (being migrated to 'space' command)",
	Long: wordwrap.WrapString(
		"Create and manage Storacha spaces locally. Spaces are isolated storage "+
			"contexts that can be shared with other users via delegations.",
		80),
}

var spacePrimeCreateCmd = &cobra.Command{
	Use:   "create <name>",
	Short: "Create a new space",
	Long: wordwrap.WrapString(
		"Create a new space with the given name. The space will be generated with "+
			"its own cryptographic identity and saved locally.",
		80),
	Args: cobra.ExactArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		spaceName := args[0]

		// Check if space already exists
		if existing, _ := findSpaceByNameOrDID(spaceName); existing != nil {
			return fmt.Errorf("space with name '%s' already exists", spaceName)
		}

		// Generate new space
		space, err := generateSpace()
		if err != nil {
			return fmt.Errorf("failed to generate space: %w", err)
		}

		space.Name = spaceName
		space.Description = spacePrimeFlags.description

		// Save to config
		if err := addSpaceToConfig(space); err != nil {
			return fmt.Errorf("failed to save space: %w", err)
		}

		// Output
		if spacePrimeFlags.json {
			data, err := json.MarshalIndent(sanitizeSpace(space), "", "  ")
			if err != nil {
				return fmt.Errorf("failed to marshal JSON: %w", err)
			}
			fmt.Println(string(data))
		} else {
			fmt.Printf("Created space: %s\n", spaceName)
			fmt.Printf("DID: %s\n", space.DID)
			fmt.Printf("Set as current space\n")
		}

		return nil
	},
}

var spacePrimeLsCmd = &cobra.Command{
	Use:     "ls",
	Aliases: []string{"list"},
	Short:   "List all spaces",
	Long:    "List all spaces that have been created locally",

	RunE: func(cmd *cobra.Command, args []string) error {
		config, err := loadSpaceConfig()
		if err != nil {
			return err
		}

		if len(config.Spaces) == 0 {
			fmt.Println("No spaces found. Create one with 'guppy space create <name>'")
			return nil
		}

		if spacePrimeFlags.json {
			safeSpaces := make(map[string]interface{})
			for did, space := range config.Spaces {
				safeSpaces[did] = sanitizeSpace(&space)
			}
			data, err := json.MarshalIndent(safeSpaces, "", "  ")
			if err != nil {
				return fmt.Errorf("failed to marshal JSON: %w", err)
			}
			fmt.Println(string(data))
			return nil
		}

		// Table output
		fmt.Printf("%-20s %-12s %-10s %s\n", "NAME", "REGISTERED", "CURRENT", "DID")
		fmt.Println(strings.Repeat("-", 80))

		for did, space := range config.Spaces {
			current := ""
			if config.Current == did {
				current = "✓"
			}

			registered := "No"
			if space.Registered {
				registered = "Yes"
			}

			fmt.Printf("%-20s %-12s %-10s %s\n", space.Name, registered, current, space.DID)
		}

		return nil
	},
}

var spacePrimeUseCmd = &cobra.Command{
	Use:   "use <name|did>",
	Short: "Set the current space",
	Long:  "Set the specified space as the current active space",
	Args:  cobra.ExactArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		nameOrDID := args[0]

		space, err := findSpaceByNameOrDID(nameOrDID)
		if err != nil {
			return err
		}

		config, err := loadSpaceConfig()
		if err != nil {
			return err
		}

		config.Current = space.DID

		if err := saveSpaceConfig(config); err != nil {
			return fmt.Errorf("failed to save space config: %w", err)
		}

		if spacePrimeFlags.json {
			data, err := json.MarshalIndent(sanitizeSpace(space), "", "  ")
			if err != nil {
				return fmt.Errorf("failed to marshal JSON: %w", err)
			}
			fmt.Println(string(data))
		} else {
			fmt.Printf("Set current space to: %s\n", space.Name)
			fmt.Printf("DID: %s\n", space.DID)
		}

		return nil
	},
}

var spacePrimeInfoCmd = &cobra.Command{
	Use:   "info [name|did]",
	Short: "Show space information",
	Long:  "Display detailed information about a space. If no argument is provided, shows the current space.",
	Args:  cobra.MaximumNArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		var space *Space
		var err error

		if len(args) > 0 {
			space, err = findSpaceByNameOrDID(args[0])
		} else {
			space, err = getCurrentSpace()
		}
		if err != nil {
			return err
		}

		if spacePrimeFlags.json {
			data, err := json.MarshalIndent(sanitizeSpace(space), "", "  ")
			if err != nil {
				return fmt.Errorf("failed to marshal JSON: %w", err)
			}
			fmt.Println(string(data))
		} else {
			fmt.Printf("Space: %s\n", space.Name)
			fmt.Printf("DID: %s\n", space.DID)
			fmt.Printf("Created: %s\n", space.Created.Format(time.RFC3339))
			fmt.Printf("Registered: %t\n", space.Registered)
			if space.Description != "" {
				fmt.Printf("Description: %s\n", space.Description)
			}

			config, _ := loadSpaceConfig()
			if config != nil && config.Current == space.DID {
				fmt.Printf("Status: Current space\n")
			}
		}

		return nil
	},
}

var spacePrimeRegisterCmd = &cobra.Command{
	Use:   "register <email>",
	Short: "Register a locally-created space with Storacha",
	Long: wordwrap.WrapString(
		"Creates a UCAN delegation from your local space to your email address, "+
			"allowing you to import the space into the Storacha console. After running "+
			"this command, you'll need to import the generated delegation file via the "+
			"Storacha web console.",
		80),
	Example: fmt.Sprintf(
		"  %s space register alice@example.com\n"+
			"  %s space register alice@example.com --space myproject",
		rootCmd.Name(), rootCmd.Name()),
	Args: cobra.ExactArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		email := args[0]

		// Get space to register
		var space *Space
		var err error
		if spacePrimeFlags.spaceName != "" {
			space, err = findSpaceByNameOrDID(spacePrimeFlags.spaceName)
			if err != nil {
				return fmt.Errorf("space not found: %w", err)
			}
		} else {
			space, err = getCurrentSpace()
			if err != nil {
				return fmt.Errorf("no current space - specify with --space or use 'guppy space use': %w", err)
			}
		}

		if space.Registered {
			fmt.Printf("Space '%s' is already registered\n", space.Name)
			return nil
		}

		fmt.Printf("Creating delegation for space '%s' to email %s...\n", space.Name, email)

		// Create delegation from space to email
		del, err := createSpaceToEmailDelegation(space, email)
		if err != nil {
			return fmt.Errorf("failed to create delegation: %w", err)
		}

		// Save delegation to file
		outputPath := spacePrimeFlags.output
		if outputPath == "" {
			outputPath = fmt.Sprintf("%s-delegation.ucan", space.Name)
		}

		if err := saveDelegationToFile(del, outputPath); err != nil {
			return fmt.Errorf("failed to save delegation: %w", err)
		}

		// Mark space as registered locally
		space.Registered = true
		config, err := loadSpaceConfig()
		if err != nil {
			return fmt.Errorf("failed to load space config: %w", err)
		}
		config.Spaces[space.DID] = *space
		if err := saveSpaceConfig(config); err != nil {
			return fmt.Errorf("failed to save space config: %w", err)
		}

		// Success message with clear instructions
		fmt.Printf("\n Delegation created successfully!\n")
		fmt.Printf(" Saved to: %s\n\n", outputPath)
		fmt.Printf("Next steps to import your space:\n")
		fmt.Printf("  1. Visit: https://console.storacha.network\n")
		fmt.Printf("  2. Click the 'IMPORT' button (top right)\n")
		fmt.Printf("  3. Upload the delegation file: %s\n", outputPath)
		fmt.Printf("  4. Your space '%s' will appear in the console!\n\n", space.Name)
		fmt.Printf("Space DID: %s\n", space.DID)

		return nil
	},
}

// Helper functions

func generateSpace() (*Space, error) {
	s, err := signer.Generate()
	if err != nil {
		return nil, err
	}

	privKeyStr, err := multibase.Encode(multibase.Base64pad, s.Encode())
	if err != nil {
		return nil, err
	}

	return &Space{
		DID:        s.DID().String(),
		Created:    time.Now(),
		PrivateKey: privKeyStr,
	}, nil
}

func getSpaceConfigPath() (string, error) {
	homedir, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	// Use same directory as existing config
	storachaDir := filepath.Join(homedir, ".storacha")
	return filepath.Join(storachaDir, "spaces.json"), nil
}

func loadSpaceConfig() (*SpaceConfig, error) {
	spacePath, err := getSpaceConfigPath()
	if err != nil {
		return nil, err
	}

	var config SpaceConfig
	if data, err := os.ReadFile(spacePath); err == nil {
		if err := json.Unmarshal(data, &config); err != nil {
			return nil, fmt.Errorf("failed to parse space config: %w", err)
		}
	}

	if config.Spaces == nil {
		config.Spaces = make(map[string]Space)
	}

	return &config, nil
}

func saveSpaceConfig(config *SpaceConfig) error {
	spacePath, err := getSpaceConfigPath()
	if err != nil {
		return err
	}

	datadir := filepath.Dir(spacePath)
	if err := os.MkdirAll(datadir, 0700); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal space config: %w", err)
	}

	return os.WriteFile(spacePath, data, 0600)
}

func addSpaceToConfig(space *Space) error {
	config, err := loadSpaceConfig()
	if err != nil {
		return err
	}

	config.Spaces[space.DID] = *space

	if config.Current == "" {
		config.Current = space.DID
	}

	return saveSpaceConfig(config)
}

func getCurrentSpace() (*Space, error) {
	config, err := loadSpaceConfig()
	if err != nil {
		return nil, err
	}

	if config.Current == "" {
		return nil, fmt.Errorf("no current space set - run 'guppy space create' or 'guppy space use'")
	}

	space, exists := config.Spaces[config.Current]
	if !exists {
		return nil, fmt.Errorf("current space '%s' not found", config.Current)
	}

	return &space, nil
}

func findSpaceByNameOrDID(nameOrDID string) (*Space, error) {
	config, err := loadSpaceConfig()
	if err != nil {
		return nil, err
	}

	// Try DID first
	if space, exists := config.Spaces[nameOrDID]; exists {
		return &space, nil
	}

	// Try name (case-insensitive)
	for _, space := range config.Spaces {
		if strings.EqualFold(space.Name, nameOrDID) {
			return &space, nil
		}
	}

	return nil, fmt.Errorf("space '%s' not found", nameOrDID)
}

func createSpaceToEmailDelegation(space *Space, email string) (delegation.Delegation, error) {
	emailDID, err := didmailto.FromEmail(email)
	if err != nil {
		return nil, fmt.Errorf("invalid email address: %w", err)
	}

	spaceSigner, err := signer.Parse(space.PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to parse space private key: %w", err)
	}

	// Grant all necessary capabilities for the space
	capabilities := []ucan.Capability[ucan.NoCaveats]{
		ucan.NewCapability("space/*", space.DID, ucan.NoCaveats{}),
		ucan.NewCapability("upload/*", space.DID, ucan.NoCaveats{}),
		ucan.NewCapability("store/*", space.DID, ucan.NoCaveats{}),
		ucan.NewCapability("index/*", space.DID, ucan.NoCaveats{}),
		ucan.NewCapability("filecoin/*", space.DID, ucan.NoCaveats{}),
		ucan.NewCapability("blob/*", space.DID, ucan.NoCaveats{}),
	}

	return delegation.Delegate(
		spaceSigner,
		emailDID,
		capabilities,
		delegation.WithNoExpiration(),
	)
}

// sanitizeSpace removes sensitive data for JSON output
func sanitizeSpace(space *Space) interface{} {
	return struct {
		DID         string    `json:"did"`
		Name        string    `json:"name"`
		Created     time.Time `json:"created"`
		Registered  bool      `json:"registered,omitempty"`
		Description string    `json:"description,omitempty"`
	}{
		DID:         space.DID,
		Name:        space.Name,
		Created:     space.Created,
		Registered:  space.Registered,
		Description: space.Description,
	}
}

func saveDelegationToFile(del delegation.Delegation, filepath string) error {
	archive := del.Archive()
	data, err := io.ReadAll(archive)
	if err != nil {
		return fmt.Errorf("failed to read delegation archive: %w", err)
	}

	if err := os.WriteFile(filepath, data, 0600); err != nil {
		return fmt.Errorf("failed to write delegation file: %w", err)
	}

	return nil
}

func init() {
	rootCmd.AddCommand(spacePrimeCmd)

	spacePrimeCmd.AddCommand(spacePrimeCreateCmd)
	spacePrimeCmd.AddCommand(spacePrimeLsCmd)
	spacePrimeCmd.AddCommand(spacePrimeUseCmd)
	spacePrimeCmd.AddCommand(spacePrimeInfoCmd)
	spacePrimeCmd.AddCommand(spacePrimeRegisterCmd)

	// Flags
	spacePrimeCreateCmd.Flags().StringVar(&spacePrimeFlags.description, "description", "", "Description for the space")
	spacePrimeCreateCmd.Flags().BoolVar(&spacePrimeFlags.json, "json", false, "Output in JSON format")

	spacePrimeLsCmd.Flags().BoolVar(&spacePrimeFlags.json, "json", false, "Output in JSON format")
	spacePrimeUseCmd.Flags().BoolVar(&spacePrimeFlags.json, "json", false, "Output in JSON format")
	spacePrimeInfoCmd.Flags().BoolVar(&spacePrimeFlags.json, "json", false, "Output in JSON format")

	spacePrimeRegisterCmd.Flags().StringVar(&spacePrimeFlags.spaceName, "space", "", "Space name or DID to register (defaults to current space)")
	spacePrimeRegisterCmd.Flags().StringVarP(&spacePrimeFlags.output, "output", "o", "", "Output path for delegation file")
}
