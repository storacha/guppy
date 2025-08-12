package model

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/storacha/guppy/pkg/preparation/types"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

// ShardIndex contains all blocks for a specific shard
type ShardIndex struct {
	ShardCID string           `json:"shard_cid"`
	Blocks   []ShardBlockJSON `json:"blocks"`
}

// Remove the duplicate ShardBlock definition since it's now in shard_block.go

// IndexManifest represents the complete index for an upload
type IndexManifest struct {
	uploadID  id.UploadID
	createdAt time.Time
	shards    []ShardIndex
}

// NewIndexManifest creates a new IndexManifest
func NewIndexManifest(uploadID id.UploadID) (*IndexManifest, error) {
	manifest := &IndexManifest{
		uploadID:  uploadID,
		createdAt: time.Now().UTC().Truncate(time.Second),
		shards:    make([]ShardIndex, 0),
	}
	if err := validateIndexManifest(manifest); err != nil {
		return nil, fmt.Errorf("failed to create IndexManifest: %w", err)
	}
	return manifest, nil
}

func validateIndexManifest(m *IndexManifest) error {
	if m.uploadID == id.Nil {
		return types.ErrEmpty{Field: "uploadID"}
	}
	return nil
}

// Accessors
func (im *IndexManifest) UploadID() id.UploadID {
	return im.uploadID
}

func (im *IndexManifest) CreatedAt() time.Time {
	return im.createdAt
}

func (im *IndexManifest) Shards() []ShardIndex {
	return im.shards
}

// AddShard adds a shard index to the manifest
func (im *IndexManifest) AddShard(shardIndex ShardIndex) {
	im.shards = append(im.shards, shardIndex)
}

// Serializable version for JSON storage
type indexManifestJSON struct {
	UploadID  string       `json:"upload_id"`
	CreatedAt time.Time    `json:"created_at"`
	Shards    []ShardIndex `json:"shards"`
}

// ToJSON serializes the manifest to JSON
func (im *IndexManifest) ToJSON() ([]byte, error) {
	jsonData := indexManifestJSON{
		UploadID:  im.uploadID.String(),
		CreatedAt: im.createdAt,
		Shards:    im.shards,
	}
	return json.Marshal(jsonData)
}

// FromJSON deserializes manifest from JSON
func FromJSON(data []byte) (*IndexManifest, error) {
	var jsonData indexManifestJSON
	if err := json.Unmarshal(data, &jsonData); err != nil {
		return nil, err
	}

	// For now, we'll create a basic ID - in practice you'd need proper UUID parsing
	// This is simplified for the initial implementation
	uploadID := id.New() // TODO: Parse actual UUID from string

	manifest := &IndexManifest{
		uploadID:  uploadID,
		createdAt: jsonData.CreatedAt,
		shards:    jsonData.Shards,
	}

	return manifest, validateIndexManifest(manifest)
}
