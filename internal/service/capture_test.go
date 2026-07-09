package service

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/CryptoLabInc/rune-mcp/internal/adapters/embedder"
	"github.com/CryptoLabInc/rune-mcp/internal/adapters/vault"
)

// TestBatch_RejectsContentlessItems guards the D14 fix: items with no usable
// extraction content are classified as "error" instead of being fabricated into
// a "[batch_capture]" placeholder record (which poisoned the corpus and caused
// cascading false near_duplicate). Rejection is enforced by Handle's shared D14
// guard (empty Text + !extraction.HasContent()), which fires before any adapter
// call, so a zero-value service (nil Embedder/Envector/Vault) is sufficient.
func TestBatch_RejectsContentlessItems(t *testing.T) {
	s := &CaptureService{}
	items := `[
		{"text": "real prose", "extracted": {"title": "wrapper-shape"}},
		{},
		{"foo": "bar"}
	]`

	res, err := s.Batch(context.Background(), BatchCaptureArgs{Items: items, Source: "test"})
	if err != nil {
		t.Fatalf("Batch returned error: %v", err)
	}
	if res.Total != 3 || res.Errors != 3 || res.Captured != 0 || res.Skipped != 0 {
		t.Fatalf("got total=%d errors=%d captured=%d skipped=%d; want total=3 errors=3 captured=0 skipped=0",
			res.Total, res.Errors, res.Captured, res.Skipped)
	}
	for i, r := range res.Results {
		if r.Status != "error" {
			t.Errorf("item %d: status=%q, want %q", i, r.Status, "error")
		}
		if r.Error == nil || !strings.Contains(*r.Error, "reusable_insight") {
			t.Errorf("item %d: error=%v, want message mentioning reusable_insight", i, r.Error)
		}
	}
}

func TestBatch_InvalidJSON(t *testing.T) {
	s := &CaptureService{}
	if _, err := s.Batch(context.Background(), BatchCaptureArgs{Items: "not json"}); err == nil {
		t.Fatal("expected error for invalid items JSON, got nil")
	}
}

// spyVault records the share groups of every Insert call; the remaining
// vault.Client methods are inert. Search returning zero hits keeps the novelty
// check on its non-fatal "novel" path so every item proceeds to insert.
type spyVault struct {
	insertShareGroups [][]string // one entry per Insert call, in call order
}

func (v *spyVault) GetAgentManifest(context.Context) (*vault.Bundle, error) { return nil, nil }
func (v *spyVault) Insert(_ context.Context, _ []float32, _ string, shareGroups []string) (string, error) {
	v.insertShareGroups = append(v.insertShareGroups, shareGroups)
	return "spy-id", nil
}
func (v *spyVault) Search(context.Context, []float32, int) ([]vault.Hit, error) { return nil, nil }
func (v *spyVault) GetPermissions(context.Context, string, bool) (*vault.Permissions, error) {
	return nil, nil
}
func (v *spyVault) HealthCheck(context.Context) (bool, error) { return true, nil }
func (v *spyVault) Endpoint() string                          { return "spy" }
func (v *spyVault) Close() error                              { return nil }

// fakeEmbedder returns one placeholder vector per text so Handle's Phase 6
// record/vector index alignment holds (stubEmbedder in lifecycle_test.go
// returns nil batches and cannot drive the insert path).
type fakeEmbedder struct{}

func (fakeEmbedder) EmbedSingle(context.Context, string) ([]float32, error) {
	return []float32{0}, nil
}
func (fakeEmbedder) EmbedBatch(_ context.Context, texts []string) ([][]float32, error) {
	vecs := make([][]float32, len(texts))
	for i := range vecs {
		vecs[i] = []float32{0}
	}
	return vecs, nil
}
func (fakeEmbedder) Info(context.Context) (embedder.InfoSnapshot, error) {
	return embedder.InfoSnapshot{}, nil
}
func (fakeEmbedder) Health(context.Context) (embedder.HealthSnapshot, error) {
	return embedder.HealthSnapshot{}, nil
}
func (fakeEmbedder) SocketPath() string { return "" }
func (fakeEmbedder) Close() error       { return nil }

// TestBatch_ShareGroupsReachEveryVaultInsert guards the M3 fix: the batch-level
// share_groups selection must be carried onto the vault Insert of EVERY item in
// the batch, mirroring single-capture semantics (rune-mcp passes it through
// unvalidated; the vault resolves + validates). Before the fix Batch() built
// per-item CaptureRequests without ShareGroups, so every batch item was
// inserted with nil share groups — the vault's broadcast default — with no way
// to narrow.
func TestBatch_ShareGroupsReachEveryVaultInsert(t *testing.T) {
	spy := &spyVault{}
	s := &CaptureService{
		Vault:    spy,
		Embedder: fakeEmbedder{},
		Now:      time.Now,
	}
	items := `[
		{"title": "alpha", "decision": "use a"},
		{"title": "beta", "decision": "use b"},
		{"title": "gamma", "decision": "use c"}
	]`
	groups := []string{"g-platform", "g-research"}

	res, err := s.Batch(context.Background(), BatchCaptureArgs{
		Items:       items,
		Source:      "test",
		ShareGroups: groups,
	})
	if err != nil {
		t.Fatalf("Batch returned error: %v", err)
	}
	if res.Captured != 3 || res.Errors != 0 {
		t.Fatalf("got captured=%d errors=%d; want captured=3 errors=0 (results=%+v)",
			res.Captured, res.Errors, res.Results)
	}
	if len(spy.insertShareGroups) != 3 {
		t.Fatalf("vault Insert calls: got %d, want 3 (one per batch item)", len(spy.insertShareGroups))
	}
	for i, got := range spy.insertShareGroups {
		if !slices.Equal(got, groups) {
			t.Errorf("insert %d: shareGroups=%v, want %v", i, got, groups)
		}
	}
}
