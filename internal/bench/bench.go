// Package bench — env-gated latency instrumentation for US-1 benchmarking.
//
// US-1 measures how recall/capture latency scales with N (pre-loaded vector
// rows). Only the envector Score segment is N-sensitive; the rest (embed,
// in-process, vault decrypt) is N-independent. This package emits one log
// line per measured segment so the benchmark harness can grep `msg=bench`
// and analyse mean/max/top5% per segment against N.
//
// Design: the production binary is also the bench binary — instrumentation
// is gated behind RUNE_MCP_BENCH=1 so a single build serves prod and bench.
// The time.Now() cost is a fixed, N-independent constant, so it does not
// bend the latency curve being measured.
//
// This is a leaf module: it imports only obs (logging + request id) and
// grpc (interceptor type). Service code (tools.go, boot.go) calls into it;
// it never calls back. That one-way dependency keeps the blast radius to
// this file behind the Enabled() guard.
//
// See planning: heeyeon-plan .../2026-06-18-us1-rune-mcp-bench-instrumentation-ko.md
package bench

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"time"

	"google.golang.org/grpc"

	"github.com/CryptoLabInc/rune-mcp/internal/obs"
)

// Enabled reports whether bench instrumentation is on (RUNE_MCP_BENCH=1).
// This is the single place that reads the toggle: call sites only ask
// Enabled(), they never read the env themselves. Read live (not cached) so
// tests can toggle it with t.Setenv.
func Enabled() bool {
	return os.Getenv("RUNE_MCP_BENCH") == "1"
}

// n returns the current sweep point N (RUNE_BENCH_N), or -1 when unset or
// unparseable. The harness sets it per sweep point; it is the x-axis value
// stamped on every bench line so the N-trend can be reconstructed.
func n() int {
	v := os.Getenv("RUNE_BENCH_N")
	if v == "" {
		return -1
	}
	parsed, err := strconv.Atoi(v)
	if err != nil {
		return -1
	}
	return parsed
}

// kContextKey types the optional top-K context value. Unexported so no other
// package can collide with the key.
type kContextKey struct{}

// WithK tags ctx with the top-K of an upcoming decrypt call so Observe can
// stamp k= on that segment's bench line. It exists because the generic
// UnaryInterceptor cannot see top-K — it is buried inside the request proto,
// and reaching in would force this leaf module to import vault's proto and
// special-case one RPC. Instead the vault adapter, which holds top-K as a
// plain int, tags the ctx; the interceptor's Observe reads it back blindly.
//
// Only vault_topk carries a meaningful k; every other segment leaves ctx
// untagged and renders no k= (parsed downstream as k=-1, "not applicable").
func WithK(ctx context.Context, k int) context.Context {
	return context.WithValue(ctx, kContextKey{}, k)
}

func kFromContext(ctx context.Context) (int, bool) {
	k, ok := ctx.Value(kContextKey{}).(int)
	return k, ok
}

// Observe emits one bench line for a single segment. It is called *after*
// the work finishes, so the caller must pass the start time it captured
// before the work began (Observe can only read "now" = the end). err is
// likewise known only at the end; nil means ok=true.
//
// Output (slog default handler renders key=value, redaction leaves numbers
// and method names untouched):
//
//	msg=bench seg=envector op=/ES2E.ES2EService/inner_product dur_us=12345 ok=true n=100000 req=ab12-...
//
// When ctx is tagged via WithK (vault decrypt only), a trailing k= is added so
// the N-sensitive vault_topk curve can be split by top-K downstream.
//
// Guarded by Enabled() so the module is self-protecting: even if a future
// refactor wires a call site without its own guard, off stays no-op.
func Observe(ctx context.Context, seg, op string, start time.Time, err error) {
	if !Enabled() {
		return
	}
	attrs := []any{
		"seg", seg,
		"op", op,
		"dur_us", time.Since(start).Microseconds(),
		"ok", err == nil,
		"n", n(),
		"req", obs.RequestID(ctx),
	}
	if k, ok := kFromContext(ctx); ok {
		attrs = append(attrs, "k", k)
	}
	slog.Info("bench", attrs...)
}

// UnaryInterceptor adapts Observe to gRPC's client-interceptor shape so the
// UNARY external calls are timed automatically: vault (DecryptScores/
// DecryptMetadata), embedder (Embed/EmbedBatch), and envector GetMetadata. It
// times exactly invoker() — the network round-trip plus remote processing —
// which is the boundary latency US-1 wants.
//
// NOTE: this only fires for unary RPCs (grpc.cc.Invoke). The N-sensitive
// envector Score (InnerProduct) and Insert (BatchInsertData) are STREAMING
// RPCs (grpc.cc.NewStream), which a unary interceptor never sees — and the
// envector SDK exposes no stream-interceptor option. Those two are timed at
// the adapter level instead (internal/adapters/envector/client.go), see
// docs/bench/us1-report-segment-coverage.md §3.
//
// Mirrors recovery.UnaryRecovery's signature so boot.go can chain them side
// by side. When chained as [recovery, bench], bench sits innermost and times
// the pure RPC.
func UnaryInterceptor(seg string) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		start := time.Now()
		err := invoker(ctx, method, req, reply, cc, opts...)
		Observe(ctx, seg, method, start, err)
		return err
	}
}
