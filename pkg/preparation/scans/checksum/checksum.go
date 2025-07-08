package checksum

import (
	"crypto/sha256"
	"encoding/binary"
	"io/fs"

	"github.com/storacha/guppy/pkg/preparation/scans/model"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

func FileChecksum(path string, info fs.FileInfo, sourceID id.SourceID) []byte {
	hasher := sha256.New()
	hasher.Write([]byte(path))
	modTimeBytes, _ := info.ModTime().MarshalBinary()
	hasher.Write(modTimeBytes)
	modeBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(modeBytes, uint32(info.Mode()))
	hasher.Write(modeBytes)
	sizeBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(sizeBytes, uint64(info.Size()))
	hasher.Write(sizeBytes)
	hasher.Write(sourceID[:])
	return hasher.Sum(nil)
}

func DirChecksum(path string, info fs.FileInfo, sourceID id.SourceID, children []model.FSEntry) []byte {
	hasher := sha256.New()
	hasher.Write([]byte(path))
	modTimeBytes, _ := info.ModTime().MarshalBinary()
	hasher.Write(modTimeBytes)
	modeBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(modeBytes, uint32(info.Mode()))
	hasher.Write(modeBytes)
	for _, child := range children {
		hasher.Write(child.Checksum())
	}
	hasher.Write(sourceID[:])
	return hasher.Sum(nil)
}
