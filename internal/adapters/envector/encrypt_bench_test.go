//go:build integration

package envector

import (
	"os"
	"strconv"
	"testing"

	envector "github.com/CryptoLabInc/envector-go-sdk"
)

// BenchmarkEncrypt measures the client-side FHE encryption cost (Keys.Encrypt)
// in isolation. This is the one segment the US-1 bench instrumentation cannot
// see: it is N-independent and runs *before* the gRPC stream opens, so no
// interceptor (unary or stream) captures it (see docs/bench/
// us1-report-segment-coverage.md §3). Capture's `insert` bundles this cost with
// the RPC; rather than split it inside the live Insert call, we characterise the
// constant here.
//
// Server-free: encryption is a local cgo operation, so this needs only the
// EncKey on disk — no envector server, no Vault. Gated on ENVECTOR_TEST_KEY_PATH
// alone (NOT the endpoint), so it can run fully offline.
//
//	ENVECTOR_TEST_KEY_PATH=~/.rune/keys/<keyID> \
//	ENVECTOR_TEST_KEY_ID=<keyID> \
//	ENVECTOR_TEST_DIM=1024 \
//	go test -tags integration -run '^$' -bench BenchmarkEncrypt ./internal/adapters/envector/...
//
// Note: measures encryption under the SDK-default preset/eval_mode (PresetIP0 /
// EvalModeRMP). For a specific eval_mode (e.g. v1.4.3 mm32), add WithKeyEvalMode
// to the OpenKeysFromFile call below — the encryption cost differs by mode.
func BenchmarkEncrypt(b *testing.B) {
	keyPath := os.Getenv("ENVECTOR_TEST_KEY_PATH")
	if keyPath == "" {
		b.Skip("ENVECTOR_TEST_KEY_PATH not set; skipping encryption benchmark")
	}
	keyID := os.Getenv("ENVECTOR_TEST_KEY_ID")
	if keyID == "" {
		keyID = "test-key"
	}

	// Production embedding dim is 1024 (Qwen3-Embedding-0.6B). Override via env to
	// match your key material if it was generated for a different dim — the dim
	// is used for BOTH the key handle and the sample vector, so they always agree.
	dim := 1024
	if v := os.Getenv("ENVECTOR_TEST_DIM"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			dim = parsed
		}
	}

	keys, err := envector.OpenKeysFromFile(
		envector.WithKeyPath(keyPath),
		envector.WithKeyID(keyID),
		envector.WithKeyDim(dim),
		envector.WithKeyParts(envector.KeyPartEnc), // Encrypt needs KeyPartEnc
	)
	if err != nil {
		b.Fatalf("open EncKey from %s (id=%s, dim=%d): %v", keyPath, keyID, dim, err)
	}
	defer func() { _ = keys.Close() }()

	// One vector — matches capture single-insert (batch size 1). Deterministic,
	// non-trivial values so the encryptor isn't fed an all-zero edge case.
	vec := make([]float32, dim)
	for i := range vec {
		vec[i] = 0.01 * float32(i%100)
	}
	vectors := [][]float32{vec}

	// Warmup: the first Encrypt pays cgo/libevi lazy init (encryptor setup) that
	// would skew an untimed-baseline reading. Discard a couple before the timer.
	for i := 0; i < 2; i++ {
		if _, _, err := keys.Encrypt(vectors); err != nil {
			b.Fatalf("warmup encrypt: %v", err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := keys.Encrypt(vectors); err != nil {
			b.Fatalf("encrypt: %v", err)
		}
	}
}
