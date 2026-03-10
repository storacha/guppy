package dags

import (
	"io"

	chunk "github.com/ipfs/boxo/chunker"
	"github.com/storacha/guppy/pkg/encryption"
)

type AES256CTRSplitter struct {
	splitter   chunk.Splitter
	privateKey []byte
}

func NewAES256CTRSplitter(sk []byte, r io.Reader, size int) *AES256CTRSplitter {
	return &AES256CTRSplitter{
		splitter:   chunk.NewSizeSplitter(r, int64(size)),
		privateKey: sk,
	}
}

func (a *AES256CTRSplitter) NextBytes() ([]byte, error) {
	b, err := a.splitter.NextBytes()
	if err != nil {
		return b, err
	}
	ciphertext, _, err := encryption.EncryptAES256CTR(a.privateKey, b)
	if err != nil {
		return nil, err
	}
	return ciphertext, nil
}

func (a *AES256CTRSplitter) Reader() io.Reader {
	return a.splitter.Reader()
}
