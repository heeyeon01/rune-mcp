package service

import (
	"bytes"
	"context"
	"testing"

	"github.com/CryptoLabInc/rune-mcp/internal/adapters/embedder"
	"github.com/CryptoLabInc/rune-mcp/internal/adapters/vault"
)

// resyncEmbedder simulates runed's centroid state: EmbedRoute fails with
// FAILED_PRECONDITION until a set is pushed, then routes against the pushed
// set's version.
type resyncEmbedder struct {
	stubEmbedder
	version    string // "" = no set loaded (cold runed)
	routeCalls int
	pushCalls  int
	pushErr    error
}

func (e *resyncEmbedder) EmbedRoute(context.Context, string) (embedder.Routed, error) {
	e.routeCalls++
	if e.version == "" {
		return embedder.Routed{}, &embedder.Error{Code: embedder.ErrEmbedderNoCentroids.Code, Message: "no centroid set"}
	}
	return embedder.Routed{Vector: []float32{1, 0}, ClusterID: 3, CentroidSetVersion: e.version}, nil
}

func (e *resyncEmbedder) SetCentroids(_ context.Context, version string, _ int, _ string, _ [][]float32) error {
	e.pushCalls++
	if e.pushErr != nil {
		return e.pushErr
	}
	e.version = version
	return nil
}

// resyncVault simulates the vault relay: Centroids serves engineVersion, and
// Insert rejects items routed against any other version with the C3 error.
type resyncVault struct {
	engineVersion       string
	centroidsErr        error
	insertCalls         int
	insertedIDs         []string
	insertedShareGroups [][]string // one entry per Insert, in call order
}

func (v *resyncVault) GetAgentManifest(context.Context) (*vault.Bundle, error) { return nil, nil }
func (v *resyncVault) Search(context.Context, []float32, int) ([]vault.Hit, error) {
	return nil, nil
}
func (v *resyncVault) HealthCheck(context.Context) (bool, error) { return true, nil }
func (v *resyncVault) Endpoint() string                          { return "fake" }
func (v *resyncVault) Close() error                              { return nil }
func (v *resyncVault) GetPermissions(context.Context, string, bool) (*vault.Permissions, error) {
	return nil, nil
}

func (v *resyncVault) Centroids(context.Context) (*vault.CentroidSet, error) {
	if v.centroidsErr != nil {
		return nil, v.centroidsErr
	}
	return &vault.CentroidSet{Version: v.engineVersion, Dim: 2, Vectors: [][]float32{{1, 0}, {0, 1}}}, nil
}

func (v *resyncVault) Insert(_ context.Context, item vault.InsertItem) (string, error) {
	v.insertCalls++
	v.insertedIDs = append(v.insertedIDs, item.ID)
	v.insertedShareGroups = append(v.insertedShareGroups, item.ShareGroups)
	if item.CentroidSetVersion != v.engineVersion {
		return "", &vault.Error{Code: vault.ErrVaultWrongCentroidVersion.Code, Message: "WRONG_CENTROID_VERSION: stale"}
	}
	return item.ID, nil
}

type noopEncryptor struct{}

func (noopEncryptor) EncryptFlat([]float32) ([]byte, error)      { return []byte{0xAA}, nil }
func (noopEncryptor) EncryptClustered([]float32) ([]byte, error) { return []byte{0xBB}, nil }

func newResyncService(e embedder.Client, v *resyncVault) *CaptureService {
	return &CaptureService{
		Vault:     v,
		Embedder:  e,
		Encryptor: noopEncryptor{},
		AgentID:   "agent-test",
		AgentDEK:  bytes.Repeat([]byte{7}, 32),
	}
}

// The capture-time group selection must ride onto the forwarded vault item
// (M3): EncryptSealInsert(…, shareGroups) → buildInsertItem → InsertItem.ShareGroups.
func TestEncryptSealInsert_ThreadsShareGroups(t *testing.T) {
	e := &resyncEmbedder{version: "v1"} // warm: route succeeds first try
	v := &resyncVault{engineVersion: "v1"}
	s := newResyncService(e, v)

	groups := []string{"g-platform", "g-research"}
	if _, err := s.EncryptSealInsert(context.Background(), "text", `{"k":"v"}`, groups); err != nil {
		t.Fatalf("EncryptSealInsert: %v", err)
	}
	if len(v.insertedShareGroups) != 1 {
		t.Fatalf("vault Insert calls: got %d, want 1", len(v.insertedShareGroups))
	}
	got := v.insertedShareGroups[0]
	if len(got) != len(groups) {
		t.Fatalf("share_groups not threaded to InsertItem: got %v, want %v", got, groups)
	}
	for i := range groups {
		if got[i] != groups[i] {
			t.Errorf("share_groups[%d] = %q, want %q", i, got[i], groups[i])
		}
	}
}

// C4 (§9.2): runed has no centroid set → resync once via the vault relay,
// then the retried route and the insert both succeed.
func TestEncryptSealInsert_C4_ColdRunedSelfHeals(t *testing.T) {
	e := &resyncEmbedder{version: ""} // cold
	v := &resyncVault{engineVersion: "v1"}
	s := newResyncService(e, v)

	id, err := s.EncryptSealInsert(context.Background(), "text", `{"k":"v"}`, nil)
	if err != nil {
		t.Fatalf("want self-heal, got error: %v", err)
	}
	if id == "" {
		t.Fatal("empty id")
	}
	if e.pushCalls != 1 || e.routeCalls != 2 {
		t.Fatalf("push=%d route=%d; want push=1 route=2 (one resync, one retry)", e.pushCalls, e.routeCalls)
	}
	if v.insertCalls != 1 {
		t.Fatalf("insertCalls=%d; want 1", v.insertCalls)
	}
}

// C4 with a failing resync must not loop: one push attempt, error surfaced.
func TestEncryptSealInsert_C4_ResyncFailureSurfaces(t *testing.T) {
	e := &resyncEmbedder{version: "", pushErr: context.DeadlineExceeded}
	v := &resyncVault{engineVersion: "v1"}
	s := newResyncService(e, v)

	if _, err := s.EncryptSealInsert(context.Background(), "text", `{}`, nil); err == nil {
		t.Fatal("want error when resync fails, got nil")
	}
	if e.pushCalls != 1 || e.routeCalls != 1 {
		t.Fatalf("push=%d route=%d; want exactly one attempt each (no loop)", e.pushCalls, e.routeCalls)
	}
}

// C3 (§9.2): the engine replaced its set after runed was routed against v1.
// The insert is rejected once, the service resyncs, rebuilds the item under
// the same id with the new version, and the retry succeeds.
func TestEncryptSealInsert_C3_VersionSwapSelfHeals(t *testing.T) {
	e := &resyncEmbedder{version: "v1"}     // runed still on v1
	v := &resyncVault{engineVersion: "v2"}  // engine swapped to v2
	s := newResyncService(e, v)

	id, err := s.EncryptSealInsert(context.Background(), "text", `{"k":"v"}`, nil)
	if err != nil {
		t.Fatalf("want self-heal, got error: %v", err)
	}
	if v.insertCalls != 2 {
		t.Fatalf("insertCalls=%d; want 2 (reject + retry)", v.insertCalls)
	}
	if v.insertedIDs[0] != v.insertedIDs[1] {
		t.Fatalf("retry changed id: %s → %s (breaks idempotency)", v.insertedIDs[0], v.insertedIDs[1])
	}
	if id != v.insertedIDs[0] {
		t.Fatalf("returned id %s != inserted id %s", id, v.insertedIDs[0])
	}
	if e.pushCalls != 1 || e.version != "v2" {
		t.Fatalf("push=%d version=%s; want one resync landing v2 on runed", e.pushCalls, e.version)
	}
}

// C3 must retry exactly once: if the retry is rejected again (e.g. the engine
// swapped sets a second time mid-retry), the error surfaces instead of looping.
func TestEncryptSealInsert_C3_SecondRejectionSurfaces(t *testing.T) {
	e := &sabotageEmbedder{resyncEmbedder: &resyncEmbedder{version: "v1"}}
	v := &resyncVault{engineVersion: "v2"}
	s := newResyncService(e, v)

	if _, err := s.EncryptSealInsert(context.Background(), "text", `{}`, nil); err == nil {
		t.Fatal("want error after single failed retry, got nil")
	}
	if v.insertCalls != 2 {
		t.Fatalf("insertCalls=%d; want exactly 2 (no retry loop)", v.insertCalls)
	}
	if e.pushCalls != 1 {
		t.Fatalf("pushCalls=%d; want exactly 1 resync", e.pushCalls)
	}
}

// sabotageEmbedder accepts the centroid push but keeps routing against v1,
// so the C3 retry is rejected again — exercising the retry-once bound.
type sabotageEmbedder struct {
	*resyncEmbedder
}

func (e *sabotageEmbedder) SetCentroids(ctx context.Context, version string, dim int, preset string, vecs [][]float32) error {
	if err := e.resyncEmbedder.SetCentroids(ctx, version, dim, preset, vecs); err != nil {
		return err
	}
	e.version = "v1" // routing stays stale despite the push
	return nil
}
