package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/briandowns/spinner"
	logging "github.com/ipfs/go-log/v2"
	"github.com/multiformats/go-multibase"
	uploadcap "github.com/storacha/go-libstoracha/capabilities/upload"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/storacha/guppy/internal/upload"
	"github.com/storacha/guppy/pkg/didmailto"
	"github.com/urfave/cli/v2"
)

type KeyPair struct {
	DID        string `json:"did"`
	PrivateKey string `json:"key"`
}

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

func generateKey() (*KeyPair, error) {
	s, err := signer.Generate()
	if err != nil {
		return nil, fmt.Errorf("failed to create signer: %w", err)
	}

	privKeyStr, err := multibase.Encode(multibase.Base64pad, s.Encode())
	if err != nil {
		return nil, fmt.Errorf("failed to encode private key: %w", err)
	}

	return &KeyPair{
		DID:        s.DID().String(),
		PrivateKey: privKeyStr,
	}, nil
}

func getKeysConfigPath() (string, error) {
	homedir, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	datadir := filepath.Join(homedir, ".guppy")
	return filepath.Join(datadir, "keys.json"), nil
}

func saveKeyToConfig(kp *KeyPair, name string) error {
	keysPath, err := getKeysConfigPath()
	if err != nil {
		return err
	}

	datadir := filepath.Dir(keysPath)
	if err := os.MkdirAll(datadir, 0700); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	var config struct {
		Default string             `json:"default,omitempty"`
		Keys    map[string]KeyPair `json:"keys"`
	}

	if data, err := os.ReadFile(keysPath); err == nil {
		json.Unmarshal(data, &config)
	}

	if config.Keys == nil {
		config.Keys = make(map[string]KeyPair)
	}

	config.Keys[name] = *kp

	if config.Default == "" {
		config.Default = name
	}

	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	return os.WriteFile(keysPath, data, 0600)
}

func loadDefaultKey() (*KeyPair, error) {
	keysPath, err := getKeysConfigPath()
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(keysPath)
	if err != nil {
		return nil, fmt.Errorf("no keys found - run 'guppy key create' first: %w", err)
	}

	var config struct {
		Default string             `json:"default,omitempty"`
		Keys    map[string]KeyPair `json:"keys"`
	}

	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse keys config: %w", err)
	}

	if config.Default == "" {
		return nil, fmt.Errorf("no default key set")
	}

	kp, exists := config.Keys[config.Default]
	if !exists {
		return nil, fmt.Errorf("default key '%s' not found", config.Default)
	}

	return &kp, nil
}

func generateSpace() (*Space, error) {
	s, err := signer.Generate()
	if err != nil {
		return nil, fmt.Errorf("failed to create space signer: %w", err)
	}

	privKeyStr, err := multibase.Encode(multibase.Base64pad, s.Encode())
	if err != nil {
		return nil, fmt.Errorf("failed to encode private key: %w", err)
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
	datadir := filepath.Join(homedir, ".guppy")
	return filepath.Join(datadir, "spaces.json"), nil
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

	if space, exists := config.Spaces[nameOrDID]; exists {
		return &space, nil
	}

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

	capabilities := []ucan.Capability[ucan.NoCaveats]{
		ucan.NewCapability("space/*", space.DID, ucan.NoCaveats{}),
		ucan.NewCapability("upload/*", space.DID, ucan.NoCaveats{}),
		ucan.NewCapability("store/*", space.DID, ucan.NoCaveats{}),
		ucan.NewCapability("index/*", space.DID, ucan.NoCaveats{}),
		ucan.NewCapability("filecoin/*", space.DID, ucan.NoCaveats{}),
	}

	return delegation.Delegate(
		spaceSigner,
		emailDID,
		capabilities,
		delegation.WithNoExpiration(),
	)
}

func saveDelegationToFile(del delegation.Delegation, filepath string) error {
	// Get delegation archive (CAR format)
	archive := del.Archive()

	// Read archive data
	data, err := io.ReadAll(archive)
	if err != nil {
		return fmt.Errorf("failed to read delegation archive: %w", err)
	}

	// Write to file
	if err := os.WriteFile(filepath, data, 0600); err != nil {
		return fmt.Errorf("failed to write delegation file: %w", err)
	}

	return nil
}

func keyCreate(cCtx *cli.Context) error {
	kp, err := generateKey()
	if err != nil {
		return fmt.Errorf("failed to generate key: %w", err)
	}

	keyName := cCtx.String("name")
	if keyName == "" {
		keyName = "default"
	}

	if err := saveKeyToConfig(kp, keyName); err != nil {
		return fmt.Errorf("failed to save key: %w", err)
	}

	if cCtx.Bool("json") {
		data, err := json.MarshalIndent(kp, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal JSON: %w", err)
		}
		fmt.Println(string(data))
	} else {
		fmt.Printf("# %s\n", kp.DID)
		fmt.Println(kp.PrivateKey)
	}

	if output := cCtx.String("output"); output != "" {
		data, err := json.MarshalIndent(kp, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal JSON: %w", err)
		}
		if err := os.WriteFile(output, data, 0600); err != nil {
			return fmt.Errorf("failed to save key to file: %w", err)
		}
		fmt.Fprintf(os.Stderr, "Key saved to: %s\n", output)
	}

	return nil
}

func keyWhoami(cCtx *cli.Context) error {
	kp, err := loadDefaultKey()
	if err != nil {
		return fmt.Errorf("failed to load key: %w", err)
	}

	fmt.Printf("DID: %s\n", kp.DID)
	return nil
}

func spaceCreate(cCtx *cli.Context) error {
	spaceName := cCtx.Args().First()
	if spaceName == "" {
		return fmt.Errorf("space name is required")
	}

	if existing, _ := findSpaceByNameOrDID(spaceName); existing != nil {
		return fmt.Errorf("space with name '%s' already exists", spaceName)
	}

	space, err := generateSpace()
	if err != nil {
		return fmt.Errorf("failed to generate space: %w", err)
	}

	space.Name = spaceName
	space.Description = cCtx.String("description")

	if err := addSpaceToConfig(space); err != nil {
		return fmt.Errorf("failed to save space: %w", err)
	}

	if cCtx.Bool("json") {
		data, err := json.MarshalIndent(space, "", "  ")
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
}

func spaceLs(cCtx *cli.Context) error {
	config, err := loadSpaceConfig()
	if err != nil {
		return err
	}

	if len(config.Spaces) == 0 {
		fmt.Println("No spaces found. Create one with 'guppy space create <n>'")
		return nil
	}

	if cCtx.Bool("json") {
		// Fix: Create sanitized spaces without private keys
		safeSpaces := make(map[string]interface{})
		for did, space := range config.Spaces {
			safeSpaces[did] = struct {
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
		data, err := json.MarshalIndent(safeSpaces, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal JSON: %w", err)
		}
		fmt.Println(string(data))
		return nil
	}

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
}

func spaceUse(cCtx *cli.Context) error {
	nameOrDID := cCtx.Args().First()
	if nameOrDID == "" {
		return fmt.Errorf("space name or DID is required")
	}

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

	if cCtx.Bool("json") {
		// Create a sanitized version without private key
		safeSpace := struct {
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
		data, err := json.MarshalIndent(safeSpace, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal JSON: %w", err)
		}
		fmt.Println(string(data))
	} else {
		fmt.Printf("Set current space to: %s\n", space.Name)
		fmt.Printf("DID: %s\n", space.DID)
	}

	return nil
}

func spaceInfo(cCtx *cli.Context) error {
	var space *Space
	var err error

	nameOrDID := cCtx.Args().First()
	if nameOrDID != "" {
		space, err = findSpaceByNameOrDID(nameOrDID)
		if err != nil {
			return err
		}
	} else {
		// Use current space if no argument provided
		space, err = getCurrentSpace()
		if err != nil {
			return err
		}
	}

	if cCtx.Bool("json") {
		safeSpace := struct {
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
		data, err := json.MarshalIndent(safeSpace, "", "  ")
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
}

func spaceWhoami(cCtx *cli.Context) error {
	space, err := getCurrentSpace()
	if err != nil {
		return err
	}

	if cCtx.Bool("json") {
		safeSpace := struct {
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
		data, err := json.MarshalIndent(safeSpace, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal JSON: %w", err)
		}
		fmt.Println(string(data))
	} else {
		fmt.Printf("%s (%s)\n", space.Name, space.DID)
	}

	return nil
}

func spaceRegister(cCtx *cli.Context) error {
	email := cCtx.Args().First()
	if email == "" {
		return fmt.Errorf("email address is required")
	}

	var space *Space
	var err error

	if spaceName := cCtx.String("space"); spaceName != "" {
		space, err = findSpaceByNameOrDID(spaceName)
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

	delegation, err := createSpaceToEmailDelegation(space, email)
	if err != nil {
		return fmt.Errorf("failed to create delegation: %w", err)
	}

	// Option 2: Manual registration (current approach)
	outputPath := cCtx.String("output")
	if outputPath == "" {
		outputPath = fmt.Sprintf("%s-delegation.ucan", space.Name)
	}

	// Save delegation to file
	if err := saveDelegationToFile(delegation, outputPath); err != nil {
		return fmt.Errorf("failed to save delegation: %w", err)
	}

	// Mark space as registered locally (user will verify via email)
	space.Registered = true
	config, err := loadSpaceConfig()
	if err != nil {
		return fmt.Errorf("failed to load space config: %w", err)
	}
	config.Spaces[space.DID] = *space

	if err := saveSpaceConfig(config); err != nil {
		return fmt.Errorf("failed to save space config: %w", err)
	}

	fmt.Printf("Space delegation created successfully!\n")
	fmt.Printf("Delegation saved to: %s\n", outputPath)
	fmt.Printf("\n Next steps:\n")
	fmt.Printf("1. Visit https://console.storacha.network\n")
	fmt.Printf("2. Upload the delegation file: %s\n", outputPath)
	fmt.Printf("3. Check your email (%s) for verification\n", email)
	fmt.Printf("4. Click the verification link\n")
	fmt.Printf("5. Your space will be ready for uploads!\n")
	return nil
}

func spaceDelegate(cCtx *cli.Context) error {
	email := cCtx.Args().First()
	if email == "" {
		return fmt.Errorf("email address is required")
	}

	var space *Space
	var err error

	if spaceName := cCtx.String("space"); spaceName != "" {
		space, err = findSpaceByNameOrDID(spaceName)
		if err != nil {
			return fmt.Errorf("space not found: %w", err)
		}
	} else {
		space, err = getCurrentSpace()
		if err != nil {
			return fmt.Errorf("no current space - specify with --space or use 'guppy space use': %w", err)
		}
	}

	delegation, err := createSpaceToEmailDelegation(space, email)
	if err != nil {
		return fmt.Errorf("failed to create delegation: %w", err)
	}

	if cCtx.Bool("json") {
		result := struct {
			Space      string `json:"space"`
			Email      string `json:"email"`
			Delegation string `json:"delegation"`
		}{
			Space:      space.Name,
			Email:      email,
			Delegation: delegation.Link().String(),
		}
		data, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal JSON: %w", err)
		}
		fmt.Println(string(data))
		return nil
	}

	outputPath := cCtx.String("output")
	if outputPath == "" {
		outputPath = fmt.Sprintf("%s-to-%s.ucan", space.Name, email)
	}

	if err := saveDelegationToFile(delegation, outputPath); err != nil {
		return fmt.Errorf("failed to save delegation: %w", err)
	}

	fmt.Printf("Delegation created from space '%s' to email %s\n", space.Name, email)
	fmt.Printf("Saved to: %s\n", outputPath)

	return nil
}

// Key command definitions
var keyCommands = []*cli.Command{
	{
		Name:  "key",
		Usage: "Manage cryptographic keys",
		Subcommands: []*cli.Command{
			{
				Name:  "create",
				Usage: "Generate a new cryptographic key pair",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "output",
						Aliases: []string{"o"},
						Usage:   "Output file path for the key",
					},
					&cli.BoolFlag{
						Name:  "json",
						Usage: "Output in JSON format",
					},
					&cli.StringFlag{
						Name:  "name",
						Usage: "Name for the key (default: 'default')",
						Value: "default",
					},
				},
				Action: keyCreate,
			},
			{
				Name:   "whoami",
				Usage:  "Show the current key's DID",
				Action: keyWhoami,
			},
		},
	},
}

var spaceCommandsWithRegistration = []*cli.Command{
	{
		Name:  "space",
		Usage: "Manage spaces",
		Subcommands: []*cli.Command{
			{
				Name:      "create",
				Usage:     "Create a new space",
				UsageText: "space create <name>",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "description",
						Usage: "Description for the space",
					},
					&cli.BoolFlag{
						Name:  "json",
						Usage: "Output in JSON format",
					},
				},
				Action: spaceCreate,
			},
			{
				Name:    "ls",
				Aliases: []string{"list"},
				Usage:   "List spaces",
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name:  "json",
						Usage: "Output in JSON format",
					},
				},
				Action: spaceLs,
			},
			{
				Name:      "use",
				Usage:     "Set the current space",
				UsageText: "space use <name|did>",
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name:  "json",
						Usage: "Output in JSON format",
					},
				},
				Action: spaceUse,
			},
			{
				Name:      "info",
				Usage:     "Show space information",
				UsageText: "space info [name|did]",
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name:  "json",
						Usage: "Output in JSON format",
					},
				},
				Action: spaceInfo,
			},
			{
				Name:  "whoami",
				Usage: "Show the current space",
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name:  "json",
						Usage: "Output in JSON format",
					},
				},
				Action: spaceWhoami,
			},
			{
				Name:      "register",
				Usage:     "Register a space with the Storacha service",
				UsageText: "space register <email>",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "space",
						Usage: "Space name or DID to register (defaults to current space)",
					},
					&cli.StringFlag{
						Name:    "output",
						Aliases: []string{"o"},
						Usage:   "Output path for delegation file",
					},
				},
				Action: spaceRegister,
			},
			{
				Name:      "delegate",
				Usage:     "Create a delegation from space to email (without registration)",
				UsageText: "space delegate <email>",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "space",
						Usage: "Space name or DID to delegate from (defaults to current space)",
					},
					&cli.StringFlag{
						Name:    "output",
						Aliases: []string{"o"},
						Usage:   "Output path for delegation file",
					},
					&cli.BoolFlag{
						Name:  "json",
						Usage: "Output delegation info in JSON format",
					},
				},
				Action: spaceDelegate,
			},
		},
	},
}

var log = logging.Logger("guppy/main")

var commands = append([]*cli.Command{
	{
		Name:   "whoami",
		Usage:  "Print information about the current agent.",
		Action: whoami,
	},
	{
		Name:      "login",
		Usage:     "Authenticate this agent with your email address to gain access to all capabilities that have been delegated to it.",
		UsageText: "login <email>",
		Action:    login,
	},
	{
		Name:      "reset",
		Usage:     "Remove all proofs/delegations from the store but retain the agent DID.",
		UsageText: "reset",
		Action:    reset,
	},
	{
		Name:    "up",
		Aliases: []string{"upload"},
		Usage:   "Store a file(s) to the service and register an upload.",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "space",
				Value: "",
				Usage: "DID of space to upload to.",
			},
			&cli.StringFlag{
				Name:  "proof",
				Value: "",
				Usage: "Path to file containing UCAN proof(s) for the operation.",
			},
			&cli.StringFlag{
				Name:    "car",
				Aliases: []string{"c"},
				Value:   "",
				Usage:   "Path to CAR file to upload.",
			},
			&cli.BoolFlag{
				Name:    "hidden",
				Aliases: []string{"H"},
				Value:   false,
				Usage:   "Include paths that start with \".\".",
			},
			&cli.BoolFlag{
				Name:    "json",
				Aliases: []string{"j"},
				Value:   false,
				Usage:   "Format as newline delimited JSON",
			},
			&cli.BoolFlag{
				Name:    "verbose",
				Aliases: []string{"v"},
				Value:   false,
				Usage:   "Output more details.",
			},
			&cli.BoolFlag{
				Name:  "wrap",
				Value: true,
				Usage: "Wrap single input file in a directory. Has no effect on directory or CAR uploads. Pass --no-wrap to disable.",
			},
			&cli.IntFlag{
				Name:  "shard-size",
				Value: 0,
				Usage: "Shard uploads into CAR files of approximately this size in bytes.",
			},
		},
		Action: upload.Upload,
	},
	{
		Name:    "ls",
		Aliases: []string{"list"},
		Usage:   "List uploads in the current space.",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "space",
				Value: "",
				Usage: "DID of space to list uploads from.",
			},
			&cli.StringFlag{
				Name:  "proof",
				Value: "",
				Usage: "Path to file containing UCAN proof(s) for the operation.",
			},
			&cli.BoolFlag{
				Name:  "shards",
				Value: false,
				Usage: "Display shard CID(s) for each upload root.",
			},
		},
		Action: ls,
	},
}, append(keyCommands, spaceCommandsWithRegistration...)...)

func main() {
	app := &cli.App{
		Name:     "guppy",
		Usage:    "interact with the Storacha Network",
		Commands: commands,
	}

	// set up a context that is canceled when a command is interrupted
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// set up a signal handler to cancel the context
	go func() {
		interrupt := make(chan os.Signal, 1)
		signal.Notify(interrupt, syscall.SIGTERM, syscall.SIGINT)

		select {
		case <-interrupt:
			fmt.Println()
			log.Info("received interrupt signal")
			cancel()
		case <-ctx.Done():
		}

		// Allow any further SIGTERM or SIGINT to kill process
		signal.Stop(interrupt)
	}()

	if err := app.RunContext(ctx, os.Args); err != nil {
		log.Fatal(err)
	}
}

func whoami(cCtx *cli.Context) error {
	c := cmdutil.MustGetClient()
	fmt.Println(c.DID())
	return nil
}

func login(cCtx *cli.Context) error {
	email := cCtx.Args().First()
	if email == "" {
		return fmt.Errorf("email address is required")
	}

	accountDid, err := didmailto.FromEmail(email)
	if err != nil {
		return fmt.Errorf("invalid email address: %w", err)
	}

	c := cmdutil.MustGetClient()

	authOk, err := c.RequestAccess(cCtx.Context, accountDid.String())
	if err != nil {
		return fmt.Errorf("requesting access: %w", err)
	}

	resultChan := c.PollClaim(cCtx.Context, authOk)

	s := spinner.New(spinner.CharSets[14], 100*time.Millisecond) // Spinner: ⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏
	s.Suffix = fmt.Sprintf(" 🔗 please click the link sent to %s to authorize this agent", email)
	s.Start()
	claimedDels, err := result.Unwrap(<-resultChan)
	s.Stop()

	if cCtx.Context.Err() != nil {
		return fmt.Errorf("login canceled: %w", cCtx.Context.Err())
	}

	if err != nil {
		return fmt.Errorf("claiming access: %w", err)
	}

	fmt.Println("Successfully logged in!", claimedDels)
	c.AddProofs(claimedDels...)

	return nil
}

func reset(cCtx *cli.Context) error {
	c := cmdutil.MustGetClient()
	return c.Reset()
}

func ls(cCtx *cli.Context) error {
	space := cmdutil.MustParseDID(cCtx.String("space"))

	proofs := []delegation.Delegation{}
	if cCtx.String("proof") != "" {
		proof := cmdutil.MustGetProof(cCtx.String("proof"))
		proofs = append(proofs, proof)
	}

	c := cmdutil.MustGetClient(proofs...)

	listOk, err := c.UploadList(
		cCtx.Context,
		space,
		uploadcap.ListCaveats{})
	if err != nil {
		return err
	}

	for _, r := range listOk.Results {
		fmt.Printf("%s\n", r.Root)
		if cCtx.Bool("shards") {
			for _, s := range r.Shards {
				fmt.Printf("\t%s\n", s)
			}
		}
	}

	return nil
}
