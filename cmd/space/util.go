package space

import (
	"fmt"
	"io"
	"os"

	"github.com/storacha/go-ucanto/core/delegation"
)

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
