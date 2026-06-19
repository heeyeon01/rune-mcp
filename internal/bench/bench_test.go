package bench

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/CryptoLabInc/rune-mcp/internal/obs"
)

// captureLogs swaps the default slog logger for a buffer-backed text handler
// (the path Observe writes through) and restores it on cleanup. Returns a
// func that reads the accumulated output.
func captureLogs(t *testing.T) func() string {
	t.Helper()
	var sb strings.Builder
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&sb, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return sb.String
}

// Contract 1 — off = no-op. With RUNE_MCP_BENCH unset, Enabled() is false and
// Observe emits nothing. This is the safety net: a future refactor that wires
// a call site without its own guard still produces zero production log noise.
func TestObserve_OffIsNoOp(t *testing.T) {
	t.Setenv("RUNE_MCP_BENCH", "") // explicitly off
	read := captureLogs(t)

	Observe(context.Background(), "envector", "/envector/Score", time.Now(), nil)

	if got := read(); got != "" {
		t.Fatalf("off must emit nothing, got: %q", got)
	}
	if Enabled() {
		t.Fatal("Enabled() must be false when RUNE_MCP_BENCH is unset")
	}
}

// Contract 2 — on = parseable line. The harness greps msg=bench and parses
// the fields; this pins the exact field set the analysis depends on.
func TestObserve_EmitsParseableLine(t *testing.T) {
	t.Setenv("RUNE_MCP_BENCH", "1")
	t.Setenv("RUNE_BENCH_N", "100000")
	read := captureLogs(t)

	ctx := obs.WithRequestID(context.Background(), "ab12-req")
	Observe(ctx, "envector", "/envector.v1.Envector/Score", time.Now(), nil)

	out := read()
	for _, want := range []string{
		"msg=bench",
		"seg=envector",
		"op=/envector.v1.Envector/Score",
		"dur_us=",
		"ok=true",
		"n=100000",
		"req=ab12-req",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("line missing %q\nfull line: %q", want, out)
		}
	}
}

// Contract 2b — k tagging. The vault adapter tags ctx via WithK so the
// bimodal vault_topk curve (main search vs larger boost search) can be split
// by top-K downstream. A tagged ctx must render k=; the value is appended, so
// existing field consumers are unaffected.
func TestObserve_EmitsKWhenTagged(t *testing.T) {
	t.Setenv("RUNE_MCP_BENCH", "1")
	read := captureLogs(t)

	ctx := WithK(context.Background(), 20)
	Observe(ctx, "vault", "/rune.vault.v1.VaultService/DecryptScores", time.Now(), nil)

	if out := read(); !strings.Contains(out, " k=20") {
		t.Errorf("tagged ctx must render k=20, got: %q", out)
	}
}

// An untagged ctx (every non-vault segment) must NOT render k=, so the field
// stays exclusive to vault_topk and downstream defaults it to k=-1.
func TestObserve_NoKWhenUntagged(t *testing.T) {
	t.Setenv("RUNE_MCP_BENCH", "1")
	read := captureLogs(t)

	Observe(context.Background(), "embedder", "/runed.v1.RunedService/Embed", time.Now(), nil)

	if out := read(); strings.Contains(out, " k=") {
		t.Errorf("untagged ctx must not render k=, got: %q", out)
	}
}

// n defaults to -1 when the sweep var is unset, so an out-of-sweep bench line
// is still self-describing rather than silently dropping the x-axis value.
func TestObserve_NDefaultsToMinusOne(t *testing.T) {
	t.Setenv("RUNE_MCP_BENCH", "1")
	t.Setenv("RUNE_BENCH_N", "") // unset
	read := captureLogs(t)

	Observe(context.Background(), "tool", "recall", time.Now(), nil)

	if out := read(); !strings.Contains(out, "n=-1") {
		t.Errorf("unset N must render n=-1, got: %q", out)
	}
}

// A failed call must render ok=false — the harness separates success/failure
// latency, so the ok label has to track err faithfully.
func TestObserve_ErrIsNotOk(t *testing.T) {
	t.Setenv("RUNE_MCP_BENCH", "1")
	read := captureLogs(t)

	Observe(context.Background(), "vault", "/vault/DecryptScores", time.Now(), errors.New("boom"))

	if out := read(); !strings.Contains(out, "ok=false") {
		t.Errorf("err must render ok=false, got: %q", out)
	}
}

// Contract 3 — interceptor timing. UnaryInterceptor wraps invoker, times the
// real call, forwards both invoker's error and the method name. Uses a fake
// invoker so the test is fast, deterministic, and network-free.
func TestUnaryInterceptor_TimesAndForwards(t *testing.T) {
	t.Setenv("RUNE_MCP_BENCH", "1")
	read := captureLogs(t)

	const method = "/envector.v1.Envector/Score"
	wantErr := errors.New("rpc failed")
	var invokerCalled bool
	fakeInvoker := func(ctx context.Context, m string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		invokerCalled = true
		if m != method {
			t.Errorf("invoker got method %q, want %q", m, method)
		}
		time.Sleep(time.Millisecond) // ensure a measurable dur_us
		return wantErr
	}

	interceptor := UnaryInterceptor("envector")
	gotErr := interceptor(context.Background(), method, nil, nil, nil, fakeInvoker)

	if !invokerCalled {
		t.Fatal("interceptor must call the wrapped invoker")
	}
	if !errors.Is(gotErr, wantErr) {
		t.Fatalf("interceptor must forward invoker error, got: %v", gotErr)
	}
	out := read()
	if !strings.Contains(out, "seg=envector") || !strings.Contains(out, "op="+method) {
		t.Errorf("bench line must carry seg and method, got: %q", out)
	}
	if !strings.Contains(out, "ok=false") {
		t.Errorf("failed RPC must render ok=false, got: %q", out)
	}
}
