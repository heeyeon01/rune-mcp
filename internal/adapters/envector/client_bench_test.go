package envector

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	envector "github.com/CryptoLabInc/envector-go-sdk"
)

// These tests lock in the P0 fix: Score/Insert are streaming RPCs that the unary
// bench interceptor cannot see, so they are timed at the adapter level via
// bench.Observe (docs/bench/us1-report-segment-coverage.md §3). Before this seam
// existed, score/insert silently emitted zero bench lines and nothing caught it.
// fakeIndex (a sdkIndex) lets us assert the lines without a live envector server.

type fakeIndex struct {
	scoreErr  error
	insertErr error
}

func (f *fakeIndex) Score(_ context.Context, _ []float32) ([][]byte, error) {
	return [][]byte{{0x01}}, f.scoreErr
}

func (f *fakeIndex) Insert(_ context.Context, _ envector.InsertRequest) (*envector.InsertResult, error) {
	return &envector.InsertResult{}, f.insertErr
}

func (f *fakeIndex) GetMetadata(_ context.Context, _ []envector.MetadataRef, _ []string) ([]envector.Metadata, error) {
	return nil, nil
}

// captureBenchLogs swaps the default slog logger for a buffer-backed text handler
// (the path bench.Observe writes through) and restores it on cleanup.
func captureBenchLogs(t *testing.T) func() string {
	t.Helper()
	var sb strings.Builder
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&sb, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return sb.String
}

func TestScore_EmitsBenchLine(t *testing.T) {
	t.Setenv("RUNE_MCP_BENCH", "1")
	read := captureBenchLogs(t)

	c := &client{idx: &fakeIndex{}}
	if _, err := c.Score(context.Background(), []float32{0.1, 0.2}); err != nil {
		t.Fatalf("Score: %v", err)
	}

	out := read()
	for _, want := range []string{"msg=bench", "seg=envector", "op=" + opScore, "ok=true"} {
		if !strings.Contains(out, want) {
			t.Errorf("score bench line missing %q\nfull: %q", want, out)
		}
	}
}

func TestInsert_EmitsBenchLine(t *testing.T) {
	t.Setenv("RUNE_MCP_BENCH", "1")
	read := captureBenchLogs(t)

	c := &client{idx: &fakeIndex{}}
	req := InsertRequest{Vectors: [][]float32{{0.1, 0.2}}, Metadata: []string{"{}"}}
	if _, err := c.Insert(context.Background(), req); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	out := read()
	for _, want := range []string{"msg=bench", "seg=envector", "op=" + opInsert, "ok=true"} {
		if !strings.Contains(out, want) {
			t.Errorf("insert bench line missing %q\nfull: %q", want, out)
		}
	}
}

// Regression for the original defect: with the toggle off, the adapter must emit
// no bench line — same no-op guarantee the interceptor path has.
func TestScore_NoBenchLineWhenOff(t *testing.T) {
	t.Setenv("RUNE_MCP_BENCH", "") // explicitly off
	read := captureBenchLogs(t)

	c := &client{idx: &fakeIndex{}}
	if _, err := c.Score(context.Background(), []float32{0.1}); err != nil {
		t.Fatalf("Score: %v", err)
	}

	if out := read(); strings.Contains(out, "msg=bench") {
		t.Errorf("off must emit no bench line, got: %q", out)
	}
}

// A failed streaming call must still emit a line, marked ok=false — the harness
// separates success/failure latency.
func TestInsert_BenchLineMarksError(t *testing.T) {
	t.Setenv("RUNE_MCP_BENCH", "1")
	read := captureBenchLogs(t)

	c := &client{idx: &fakeIndex{insertErr: errors.New("boom")}}
	req := InsertRequest{Vectors: [][]float32{{0.1}}, Metadata: []string{"{}"}}
	_, _ = c.Insert(context.Background(), req) // error is expected and mapped

	if out := read(); !strings.Contains(out, "ok=false") {
		t.Errorf("failed insert must render ok=false, got: %q", out)
	}
}
