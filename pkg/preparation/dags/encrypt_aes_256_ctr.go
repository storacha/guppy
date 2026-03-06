package dags

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	chunk "github.com/ipfs/boxo/chunker"
)

func init() {
	chunk.Register("aes", func(r io.Reader, chunker string) (chunk.Splitter, error) {
		if !strings.HasPrefix(chunker, "aes-256-ctr-size-") {
			return nil, fmt.Errorf("unsupported chunker: %s", chunker)
		}
		sizeChunkerStr, _ := strings.CutPrefix(chunker, "aes-256-ctr-")
		parts := strings.Split(sizeChunkerStr, "-")
		if len(parts) != 2 {
			return nil, errors.New("incorrect chunker string format (expected aes-256-ctr-size-{size})")
		}
		size, err := strconv.Atoi(parts[1])
		if err != nil {
			return nil, err
		} else if size <= 0 {
			return nil, chunk.ErrSize
		} else if size > chunk.ChunkSizeLimit {
			return nil, chunk.ErrSizeMax
		}
		return NewAES256CTRSplitter(r, size), nil
	})
}

type AES256CTRSplitter struct {
	reader     io.Reader
	splitter   chunk.Splitter
	privateKey []byte
}

func NewAES256CTRSplitter(r io.Reader, size int) *AES256CTRSplitter {
	sk, err := os.ReadFile("./key.sk")
	if err != nil {
		panic(err)
	}
	return &AES256CTRSplitter{
		reader:     r,
		splitter:   chunk.NewSizeSplitter(r, int64(size)),
		privateKey: sk,
	}
}

func (a *AES256CTRSplitter) NextBytes() ([]byte, error) {
	b, err := a.splitter.NextBytes()
	if err != nil {
		return b, err
	}
	block, err := aes.NewCipher(a.privateKey)
	if err != nil {
		return nil, err
	}
	ciphertext := make([]byte, aes.BlockSize+len(b))
	// create and prepend the IV to the ciphertext
	_, err = rand.Reader.Read(ciphertext[:aes.BlockSize])
	if err != nil {
		return nil, err
	}
	stream := cipher.NewCTR(block, ciphertext[:aes.BlockSize])
	stream.XORKeyStream(ciphertext[aes.BlockSize:], b)
	return ciphertext, nil
}

func (a *AES256CTRSplitter) Reader() io.Reader {
	return a.reader
}
