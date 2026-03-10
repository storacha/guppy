package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"io"
)

type EncryptAES256CTROption func(*aes256ctroption) error

type aes256ctroption struct {
	iv []byte
}

func WithIV(iv []byte) EncryptAES256CTROption {
	return func(o *aes256ctroption) error {
		if len(iv) != aes.BlockSize {
			return fmt.Errorf("invalid IV length: expected %d bytes, got %d bytes", aes.BlockSize, len(iv))
		}
		o.iv = iv
		return nil
	}
}

func EncryptAES256CTR(key []byte, data []byte, options ...EncryptAES256CTROption) ([]byte, []byte, error) {
	cfg := aes256ctroption{}
	for _, option := range options {
		if err := option(&cfg); err != nil {
			return nil, nil, err
		}
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, nil, err
	}
	ciphertext := make([]byte, aes.BlockSize+len(data))
	var iv []byte
	if cfg.iv != nil {
		copy(ciphertext[:aes.BlockSize], cfg.iv)
		iv = cfg.iv
	} else {
		iv = ciphertext[:aes.BlockSize]
		// create and prepend the IV to the ciphertext
		if _, err := io.ReadFull(rand.Reader, iv); err != nil {
			return nil, iv, err
		}
	}

	stream := cipher.NewCTR(block, iv)
	stream.XORKeyStream(ciphertext[aes.BlockSize:], data)
	return ciphertext, iv, nil
}

func DecryptAES256CTR(key []byte, data []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	plaintext := make([]byte, len(data)-aes.BlockSize)
	iv := data[:aes.BlockSize]
	stream := cipher.NewCTR(block, iv)
	stream.XORKeyStream(plaintext, data[aes.BlockSize:])
	return plaintext, nil
}
