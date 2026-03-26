package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"fmt"
	"io"
)

// WrapWithLocalKey encrypts plaintext using AES-256-GCM with the given 32-byte KEK.
// Wire format: nonce(12) || ciphertext || tag(16).
func WrapWithLocalKey(kek, plaintext []byte) ([]byte, error) {
	if len(kek) != 32 {
		return nil, fmt.Errorf("invalid KEK length: expected 32 bytes, got %d", len(kek))
	}
	block, err := aes.NewCipher(kek)
	if err != nil {
		return nil, fmt.Errorf("creating AES cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("creating GCM: %w", err)
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("generating nonce: %w", err)
	}
	// gcm.Seal appends tag to ciphertext; result is ciphertext+tag(16)
	ciphertext := gcm.Seal(nil, nonce, plaintext, nil)
	return append(nonce, ciphertext...), nil
}

// UnwrapWithLocalKey decrypts an AES-256-GCM wrapped payload.
// Expects wire format: nonce(12) || ciphertext || tag(16).
func UnwrapWithLocalKey(kek, data []byte) ([]byte, error) {
	if len(kek) != 32 {
		return nil, fmt.Errorf("invalid KEK length: expected 32 bytes, got %d", len(kek))
	}
	block, err := aes.NewCipher(kek)
	if err != nil {
		return nil, fmt.Errorf("creating AES cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("creating GCM: %w", err)
	}
	nonceSize := gcm.NonceSize()
	if len(data) < nonceSize+gcm.Overhead() {
		return nil, fmt.Errorf("ciphertext too short: %d bytes", len(data))
	}
	nonce, ciphertext := data[:nonceSize], data[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("AES-GCM decryption failed: %w", err)
	}
	return plaintext, nil
}

// WrapWithRSA encrypts plaintext using RSA-OAEP with SHA-256.
// Used in KMS mode to wrap the serialized DEK payload with the space's public key.
func WrapWithRSA(pubKey *rsa.PublicKey, plaintext []byte) ([]byte, error) {
	ciphertext, err := rsa.EncryptOAEP(sha256.New(), rand.Reader, pubKey, plaintext, nil)
	if err != nil {
		return nil, fmt.Errorf("RSA-OAEP encryption failed: %w", err)
	}
	return ciphertext, nil
}

// Note: RSA unwrapping is intentionally absent — the KMS holds the private key exclusively.
// Decryption is delegated via the `space/encryption/key/decrypt` UCAN capability to the KMS gateway,
// which decrypts server-side and returns the plaintext DEK. The private key never leaves KMS.
