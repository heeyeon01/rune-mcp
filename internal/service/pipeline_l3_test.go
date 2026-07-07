package service_test

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/CryptoLabInc/rune-mcp/internal/adapters/embedder"
	"github.com/CryptoLabInc/rune-mcp/internal/adapters/vault"
)

// TestPipelineL3 exercises the full runespace pipeline through the mcp-side
// clients against a LIVE local stack (runed + runevault + runespace):
//
//	text → runed embed → vault.Insert → runespace   (capture)
//	text → runed embed → vault.Search → decrypted hit (recall)
//
// Gated on RUNEVAULT_ADDR (e.g. 127.0.0.1:50051). Uses the demo token from
// run/tokens.yaml and the already-running runed embedding socket.
//
//	RUNEVAULT_ADDR=127.0.0.1:50051 go test ./internal/service -run PipelineL3 -v
func TestPipelineL3(t *testing.T) {
	addr := os.Getenv("RUNEVAULT_ADDR")
	if addr == "" {
		t.Skip("set RUNEVAULT_ADDR to run the live L3 pipeline test")
	}
	token := os.Getenv("RUNEVAULT_TOKEN")
	if token == "" {
		token = "evt_0000000000000000000000000000test"
	}

	vc, err := vault.NewClient(addr, token, vault.ClientOpts{TLSDisable: true})
	if err != nil {
		t.Fatalf("vault.NewClient: %v", err)
	}
	defer vc.Close()

	emb, err := embedder.New(embedder.ResolveSocketPath(""), embedder.Opts{})
	if err != nil {
		t.Fatalf("embedder.New: %v", err)
	}
	defer emb.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// ── capture ──
	insight := "The team chose PostgreSQL over MongoDB for the ledger because ACID transactions were non-negotiable."
	vec, err := emb.EmbedSingle(ctx, insight)
	if err != nil {
		t.Fatalf("embed insight: %v", err)
	}
	t.Logf("embedded insight: dim=%d", len(vec))

	meta, _ := json.Marshal(map[string]any{
		"id":               "rec-db-choice",
		"title":            "Database choice",
		"reusable_insight": insight,
		"domain":           "architecture",
	})
	id, err := vc.Insert(ctx, vec, string(meta), nil)
	if err != nil {
		t.Fatalf("vault.Insert: %v", err)
	}
	t.Logf("capture ok: id=%s", id)

	// ── recall ──
	qvec, err := emb.EmbedSingle(ctx, "which database did we pick for the ledger and why?")
	if err != nil {
		t.Fatalf("embed query: %v", err)
	}
	hits, err := vc.Search(ctx, qvec, 5)
	if err != nil {
		t.Fatalf("vault.Search: %v", err)
	}
	t.Logf("recall returned %d hits", len(hits))
	for i, h := range hits {
		t.Logf("  hit[%d] id=%s score=%.4f meta=%s", i, h.ID, h.Score, h.Metadata)
	}
	if len(hits) == 0 {
		t.Fatal("recall returned 0 hits")
	}

	// The captured record should surface with its plaintext metadata intact.
	found := false
	for _, h := range hits {
		if strings.Contains(h.Metadata, "PostgreSQL") && strings.Contains(h.Metadata, "Database choice") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("captured record not found in recall results")
	}
}
