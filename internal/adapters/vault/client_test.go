package vault_test

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"

	vaultpb "github.com/CryptoLabInc/rune-admin/vault/pkg/vaultpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/CryptoLabInc/rune-mcp/internal/adapters/vault"
)

// fakeServer implements VaultServiceServer + HealthServer for in-process tests.
type fakeServer struct {
	vaultpb.UnimplementedVaultServiceServer
	healthpb.UnimplementedHealthServer

	getAgentManifestFn func(*vaultpb.GetAgentManifestRequest) (*vaultpb.GetAgentManifestResponse, error)
	insertFn           func(*vaultpb.InsertRequest) (*vaultpb.InsertResponse, error)
	searchFn           func(*vaultpb.SearchRequest) (*vaultpb.SearchResponse, error)
	healthFn           func(*healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error)
	lookupWrapFn       func(*vaultpb.LookupWrapRequest) (*vaultpb.LookupWrapResponse, error)
	unwrapFn           func(*vaultpb.UnwrapRequest) (*vaultpb.UnwrapResponse, error)
}

func (f *fakeServer) LookupWrap(_ context.Context, req *vaultpb.LookupWrapRequest) (*vaultpb.LookupWrapResponse, error) {
	if f.lookupWrapFn != nil {
		return f.lookupWrapFn(req)
	}
	return nil, status.Error(codes.Unimplemented, "test server: LookupWrap not stubbed")
}

func (f *fakeServer) Unwrap(_ context.Context, req *vaultpb.UnwrapRequest) (*vaultpb.UnwrapResponse, error) {
	if f.unwrapFn != nil {
		return f.unwrapFn(req)
	}
	return nil, status.Error(codes.Unimplemented, "test server: Unwrap not stubbed")
}

func (f *fakeServer) GetAgentManifest(_ context.Context, req *vaultpb.GetAgentManifestRequest) (*vaultpb.GetAgentManifestResponse, error) {
	if f.getAgentManifestFn != nil {
		return f.getAgentManifestFn(req)
	}
	return nil, status.Error(codes.Unimplemented, "test server: GetAgentManifest not stubbed")
}

func (f *fakeServer) Insert(_ context.Context, req *vaultpb.InsertRequest) (*vaultpb.InsertResponse, error) {
	if f.insertFn != nil {
		return f.insertFn(req)
	}
	return nil, status.Error(codes.Unimplemented, "test server: Insert not stubbed")
}

func (f *fakeServer) Search(_ context.Context, req *vaultpb.SearchRequest) (*vaultpb.SearchResponse, error) {
	if f.searchFn != nil {
		return f.searchFn(req)
	}
	return nil, status.Error(codes.Unimplemented, "test server: Search not stubbed")
}

func (f *fakeServer) Check(_ context.Context, req *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	if f.healthFn != nil {
		return f.healthFn(req)
	}
	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
}

func startFakeServer(t *testing.T) (*fakeServer, vault.Client) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	fake := &fakeServer{}
	vaultpb.RegisterVaultServiceServer(srv, fake)
	healthpb.RegisterHealthServer(srv, fake)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(
		"passthrough://bufconn",
		grpc.WithContextDialer(func(_ context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(context.Background())
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return fake, vault.NewBufconnClient(conn, "test-token")
}

// ── GetAgentManifest (config-only bundle) ─────────────────────────

func TestGetAgentManifest_HappyPath(t *testing.T) {
	fake, c := startFakeServer(t)
	fake.getAgentManifestFn = func(req *vaultpb.GetAgentManifestRequest) (*vaultpb.GetAgentManifestResponse, error) {
		if req.GetToken() != "test-token" {
			return nil, status.Error(codes.Unauthenticated, "wrong token")
		}
		return &vaultpb.GetAgentManifestResponse{
			ManifestJson: `{"key_id":"key_test","index_name":"test-index","agent_id":"agent_test","dim":1024}`,
		}, nil
	}

	bundle, err := c.GetAgentManifest(context.Background())
	if err != nil {
		t.Fatalf("GetAgentManifest: %v", err)
	}
	if bundle.KeyID != "key_test" || bundle.IndexName != "test-index" || bundle.AgentID != "agent_test" || bundle.Dim != 1024 {
		t.Errorf("bundle mismatch: %+v", bundle)
	}
}

func TestGetAgentManifest_ResponseErrorString(t *testing.T) {
	fake, c := startFakeServer(t)
	fake.getAgentManifestFn = func(*vaultpb.GetAgentManifestRequest) (*vaultpb.GetAgentManifestResponse, error) {
		return &vaultpb.GetAgentManifestResponse{Error: "manifest build failed"}, nil
	}
	_, err := c.GetAgentManifest(context.Background())
	var ve *vault.Error
	if !errors.As(err, &ve) || !strings.Contains(ve.Message, "manifest build failed") {
		t.Fatalf("expected wrapped vault.Error, got %v", err)
	}
}

func TestGetAgentManifest_MalformedJSON(t *testing.T) {
	fake, c := startFakeServer(t)
	fake.getAgentManifestFn = func(*vaultpb.GetAgentManifestRequest) (*vaultpb.GetAgentManifestResponse, error) {
		return &vaultpb.GetAgentManifestResponse{ManifestJson: "not json {"}, nil
	}
	_, err := c.GetAgentManifest(context.Background())
	if err == nil || !strings.Contains(err.Error(), "parse manifest_json") {
		t.Fatalf("want parse error, got %v", err)
	}
}

// ── Insert ────────────────────────────────────────────────────────

func TestInsert_HappyPath(t *testing.T) {
	fake, c := startFakeServer(t)
	fake.insertFn = func(req *vaultpb.InsertRequest) (*vaultpb.InsertResponse, error) {
		if len(req.GetVector()) != 3 || req.GetMetadata() != `{"t":1}` {
			t.Errorf("insert req mismatch: %+v", req)
		}
		return &vaultpb.InsertResponse{Id: "id-xyz"}, nil
	}
	id, err := c.Insert(context.Background(), []float32{0.1, 0.2, 0.3}, `{"t":1}`, nil)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if id != "id-xyz" {
		t.Errorf("id: got %q, want id-xyz", id)
	}
}

func TestInsert_ResponseError(t *testing.T) {
	fake, c := startFakeServer(t)
	fake.insertFn = func(*vaultpb.InsertRequest) (*vaultpb.InsertResponse, error) {
		return &vaultpb.InsertResponse{Error: "insert failed"}, nil
	}
	_, err := c.Insert(context.Background(), []float32{1}, "", nil)
	if err == nil || !strings.Contains(err.Error(), "insert failed") {
		t.Fatalf("want insert error, got %v", err)
	}
}

// ── Search ────────────────────────────────────────────────────────

func TestSearch_HappyPath(t *testing.T) {
	fake, c := startFakeServer(t)
	fake.searchFn = func(req *vaultpb.SearchRequest) (*vaultpb.SearchResponse, error) {
		if req.GetTopK() != 5 {
			t.Errorf("topk: got %d, want 5", req.GetTopK())
		}
		return &vaultpb.SearchResponse{Hits: []*vaultpb.SearchHit{
			{Id: "a", Score: 0.95, Metadata: `{"title":"A"}`},
			{Id: "b", Score: 0.80, Metadata: `{"title":"B"}`},
		}}, nil
	}
	hits, err := c.Search(context.Background(), []float32{0.1}, 5)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(hits) != 2 || hits[0].ID != "a" || hits[0].Score != 0.95 || hits[0].Metadata != `{"title":"A"}` {
		t.Errorf("hits mismatch: %+v", hits)
	}
}

func TestSearch_GRPCError(t *testing.T) {
	fake, c := startFakeServer(t)
	fake.searchFn = func(*vaultpb.SearchRequest) (*vaultpb.SearchResponse, error) {
		return nil, status.Error(codes.PermissionDenied, "denied")
	}
	_, err := c.Search(context.Background(), []float32{1}, 5)
	var ve *vault.Error
	if !errors.As(err, &ve) || ve.Code != "VAULT_PERMISSION_DENIED" || ve.Retryable {
		t.Fatalf("want non-retryable VAULT_PERMISSION_DENIED, got %v", err)
	}
}

// ── Health / Endpoint / Close ─────────────────────────────────────

func TestHealthCheck(t *testing.T) {
	fake, c := startFakeServer(t)
	fake.healthFn = func(*healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
		return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
	}
	healthy, err := c.HealthCheck(context.Background())
	if err != nil || !healthy {
		t.Fatalf("healthy=%v err=%v", healthy, err)
	}
}

func TestEndpointAndClose(t *testing.T) {
	_, c := startFakeServer(t)
	if c.Endpoint() != "bufconn" {
		t.Errorf("Endpoint: got %q", c.Endpoint())
	}
	if err := c.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

// ── MapGRPCError matrix (unchanged mapping) ───────────────────────

func TestMapGRPCError_CodeMatrix(t *testing.T) {
	cases := []struct {
		grpcCode  codes.Code
		msg       string
		wantCode  string
		retryable bool
	}{
		{codes.Unauthenticated, "x", "VAULT_AUTH_FAILED", false},
		{codes.PermissionDenied, "x", "VAULT_PERMISSION_DENIED", false},
		{codes.InvalidArgument, "x", "VAULT_INVALID_INPUT", false},
		{codes.InvalidArgument, "top_k 8 exceeds limit 3 for role 'r'", "VAULT_TOPK_EXCEEDED", false},
		{codes.ResourceExhausted, "x", "VAULT_RATE_LIMITED", true},
		{codes.NotFound, "x", "VAULT_KEY_NOT_FOUND", false},
		{codes.Unavailable, "x", "VAULT_UNAVAILABLE", true},
		{codes.DeadlineExceeded, "x", "VAULT_TIMEOUT", true},
		{codes.Internal, "x", "VAULT_INTERNAL", true},
		{codes.Aborted, "x", "VAULT_INTERNAL", true},
	}
	for _, tc := range cases {
		err := vault.MapGRPCError(status.Error(tc.grpcCode, tc.msg))
		var ve *vault.Error
		if !errors.As(err, &ve) {
			t.Fatalf("expected *vault.Error, got %T", err)
		}
		if ve.Code != tc.wantCode || ve.Retryable != tc.retryable {
			t.Errorf("%v/%q → %s(retry=%v), want %s(retry=%v)", tc.grpcCode, tc.msg, ve.Code, ve.Retryable, tc.wantCode, tc.retryable)
		}
	}
}

func TestMapGRPCError_NilReturnsNil(t *testing.T) {
	if got := vault.MapGRPCError(nil); got != nil {
		t.Errorf("MapGRPCError(nil): got %v, want nil", got)
	}
}

func TestParseManifestJSON_NotJSON(t *testing.T) {
	if _, err := vault.ParseManifestJSON("nope"); err == nil {
		t.Fatal("expected parse error")
	}
}
