package config

import (
	"fmt"
	"strings"

	"github.com/urfave/cli/v2"
)

// ConfigCommand returns the configuration management command
func ConfigCommand() *cli.Command {
	return &cli.Command{
		Name:  "config",
		Usage: "Manage guppy configuration",
		Subcommands: []*cli.Command{
			{
				Name:   "init",
				Usage:  "Initialize configuration file",
				Action: initConfig,
			},
			{
				Name:      "set",
				Usage:     "Set a configuration value",
				ArgsUsage: "<key> <value>",
				Action:    setConfig,
			},
			{
				Name:      "get",
				Usage:     "Get a configuration value",
				ArgsUsage: "<key>",
				Action:    getConfig,
			},
			{
				Name:   "list",
				Usage:  "List all configuration values",
				Action: listConfig,
			},
			{
				Name:   "profile",
				Usage:  "Manage configuration profiles",
				Subcommands: []*cli.Command{
					{
						Name:      "create",
						Usage:     "Create a new profile",
						ArgsUsage: "<name>",
						Action:    createProfile,
					},
					{
						Name:      "delete",
						Usage:     "Delete a profile",
						ArgsUsage: "<name>",
						Action:    deleteProfile,
					},
					{
						Name:   "list",
						Usage:  "List all profiles",
						Action: listProfiles,
					},
				},
			},
		},
	}
}

func initConfig(cCtx *cli.Context) error {
	_, err := createDefaultConfig()
	if err != nil {
		return fmt.Errorf("failed to initialize config: %w", err)
	}
	fmt.Println("Configuration file initialized successfully")
	return nil
}

func setConfig(cCtx *cli.Context) error {
	if cCtx.NArg() != 2 {
		return fmt.Errorf("expected key and value arguments")
	}

	key := cCtx.Args().Get(0)
	value := cCtx.Args().Get(1)

	mgr, err := NewManager()
	if err != nil {
		return err
	}

	if err := mgr.SetValue(key, value); err != nil {
		return err
	}

	if err := mgr.Save(); err != nil {
		return err
	}

	fmt.Printf("Set %s = %s\n", key, value)
	return nil
}

func getConfig(cCtx *cli.Context) error {
	if cCtx.NArg() != 1 {
		return fmt.Errorf("expected key argument")
	}

	key := cCtx.Args().First()

	mgr, err := NewManager()
	if err != nil {
		return err
	}

	value, err := mgr.GetValue(key)
	if err != nil {
		return err
	}

	fmt.Printf("%v\n", value)
	return nil
}

func listConfig(cCtx *cli.Context) error {
	mgr, err := NewManager()
	if err != nil {
		return err
	}

	cfg := mgr.Get()
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	fmt.Println(string(data))
	return nil
}

func createProfile(cCtx *cli.Context) error {
	if cCtx.NArg() != 1 {
		return fmt.Errorf("expected profile name argument")
	}

	name := cCtx.Args().First()

	mgr, err := NewManager()
	if err != nil {
		return err
	}

	if mgr.config.Profiles == nil {
		mgr.config.Profiles = make(map[string]*Profile)
	}

	if _, exists := mgr.config.Profiles[name]; exists {
		return fmt.Errorf("profile %s already exists", name)
	}

	mgr.config.Profiles[name] = &Profile{
		Verbose:         false,
		JSONOutput:      false,
		WrapSingleFiles: true,
	}

	if err := mgr.Save(); err != nil {
		return err
	}

	fmt.Printf("Created profile %s\n", name)
	return nil
}

func deleteProfile(cCtx *cli.Context) error {
	if cCtx.NArg() != 1 {
		return fmt.Errorf("expected profile name argument")
	}

	name := cCtx.Args().First()

	mgr, err := NewManager()
	if err != nil {
		return err
	}

	if _, exists := mgr.config.Profiles[name]; !exists {
		return fmt.Errorf("profile %s does not exist", name)
	}

	delete(mgr.config.Profiles, name)

	if err := mgr.Save(); err != nil {
		return err
	}

	fmt.Printf("Deleted profile %s\n", name)
	return nil
}

func listProfiles(cCtx *cli.Context) error {
	mgr, err := NewManager()
	if err != nil {
		return err
	}

	if len(mgr.config.Profiles) == 0 {
		fmt.Println("No profiles configured")
		return nil
	}

	for name, profile := range mgr.config.Profiles {
		fmt.Printf("Profile: %s\n", name)
		data, err := yaml.Marshal(profile)
		if err != nil {
			return fmt.Errorf("failed to marshal profile: %w", err)
		}
		fmt.Printf("%s\n", strings.ReplaceAll(string(data), "\n", "\n  "))
	}

	return nil
}
