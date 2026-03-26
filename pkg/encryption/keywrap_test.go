package encryption_test

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"testing"

	"github.com/storacha/guppy/pkg/encryption"
	"github.com/stretchr/testify/require"
)

func TestWrapUnwrapWithLocalKey_RoundTrip(t *testing.T) {
	kek := make([]byte, 32)
	_, err := rand.Read(kek)
	require.NoError(t, err)

	plaintext := []byte("super-secret-dek-32-bytes-exactly")

	ciphertext, err := encryption.WrapWithLocalKey(kek, plaintext)
	require.NoError(t, err)
	require.NotEqual(t, plaintext, ciphertext)

	recovered, err := encryption.UnwrapWithLocalKey(kek, ciphertext)
	require.NoError(t, err)
	require.Equal(t, plaintext, recovered)
}

func TestWrapWithLocalKey_DifferentNonceEachCall(t *testing.T) {
	kek := make([]byte, 32)
	_, err := rand.Read(kek)
	require.NoError(t, err)

	plaintext := []byte("same-plaintext-every-time")

	ct1, err := encryption.WrapWithLocalKey(kek, plaintext)
	require.NoError(t, err)

	ct2, err := encryption.WrapWithLocalKey(kek, plaintext)
	require.NoError(t, err)

	require.NotEqual(t, ct1, ct2, "two wraps of the same plaintext must differ (random nonce)")

	// Both must still decrypt to the same plaintext
	r1, err := encryption.UnwrapWithLocalKey(kek, ct1)
	require.NoError(t, err)
	require.Equal(t, plaintext, r1)

	r2, err := encryption.UnwrapWithLocalKey(kek, ct2)
	require.NoError(t, err)
	require.Equal(t, plaintext, r2)
}

func TestUnwrapWithLocalKey_WrongKey(t *testing.T) {
	kek := make([]byte, 32)
	_, err := rand.Read(kek)
	require.NoError(t, err)

	wrongKEK := make([]byte, 32)
	_, err = rand.Read(wrongKEK)
	require.NoError(t, err)

	ciphertext, err := encryption.WrapWithLocalKey(kek, []byte("secret"))
	require.NoError(t, err)

	_, err = encryption.UnwrapWithLocalKey(wrongKEK, ciphertext)
	require.Error(t, err, "decryption with wrong key must fail")
}

func TestWrapWithRSA_NonDeterministic(t *testing.T) {
	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	plaintext := []byte("dek-payload-32-bytes-exactly!!!!") // 32 bytes

	ct1, err := encryption.WrapWithRSA(&privKey.PublicKey, plaintext)
	require.NoError(t, err)

	ct2, err := encryption.WrapWithRSA(&privKey.PublicKey, plaintext)
	require.NoError(t, err)

	require.NotEqual(t, ct1, ct2, "RSA-OAEP is randomised — two wraps of the same plaintext must differ")
}

func TestWrapWithRSA_BothOutputsDecryptCorrectly(t *testing.T) {
	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	plaintext := []byte("dek-payload-32-bytes-exactly!!!!")

	ct1, err := encryption.WrapWithRSA(&privKey.PublicKey, plaintext)
	require.NoError(t, err)

	ct2, err := encryption.WrapWithRSA(&privKey.PublicKey, plaintext)
	require.NoError(t, err)

	r1, err := rsa.DecryptOAEP(sha256.New(), rand.Reader, privKey, ct1, nil)
	require.NoError(t, err)
	require.Equal(t, plaintext, r1)

	r2, err := rsa.DecryptOAEP(sha256.New(), rand.Reader, privKey, ct2, nil)
	require.NoError(t, err)
	require.Equal(t, plaintext, r2)
}
