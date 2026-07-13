package vault_test

import (
	"context"
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

func startFakeRedeemServer(t *testing.T) (*fakeServer, *vault.Redeemer) {
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

	return fake, vault.NewRedeemerFromConn(conn)
}

func TestRedeemerLookup_MapsInviteInfo(t *testing.T) {
	fake, r := startFakeRedeemServer(t)
	fake.lookupWrapFn = func(req *vaultpb.LookupWrapRequest) (*vaultpb.LookupWrapResponse, error) {
		if req.GetHandle() != "aaaabbbbccccddddaaaabbbbccccdddd" {
			return nil, status.Error(codes.NotFound, "invite does not exist")
		}
		return &vaultpb.LookupWrapResponse{
			Email:        "kim@example.com",
			Role:         "member",
			ExpiresAt:    "2026-07-14T09:00:00Z",
			CreationPath: vault.ExpectedInviteCreationPath,
		}, nil
	}

	info, err := r.Lookup(context.Background(), "aaaabbbbccccddddaaaabbbbccccdddd")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if info.Email != "kim@example.com" || info.Role != "member" || info.ExpiresAt != "2026-07-14T09:00:00Z" {
		t.Errorf("info mismatch: %+v", info)
	}
}

func TestRedeemerLookup_RefusesForeignCreationPath(t *testing.T) {
	fake, r := startFakeRedeemServer(t)
	fake.lookupWrapFn = func(_ *vaultpb.LookupWrapRequest) (*vaultpb.LookupWrapResponse, error) {
		return &vaultpb.LookupWrapResponse{
			Email:        "kim@example.com",
			Role:         "member",
			CreationPath: "some.other.surface",
		}, nil
	}

	_, err := r.Lookup(context.Background(), "aaaabbbbccccddddaaaabbbbccccdddd")
	if err == nil || !strings.Contains(err.Error(), "creation path") {
		t.Fatalf("err = %v, want creation-path refusal", err)
	}
}

func TestRedeemerRedeem_ReturnsTokenAndMember(t *testing.T) {
	fake, r := startFakeRedeemServer(t)
	fake.unwrapFn = func(req *vaultpb.UnwrapRequest) (*vaultpb.UnwrapResponse, error) {
		return &vaultpb.UnwrapResponse{
			Token:    "evt_00000000000000000000000000000000",
			MemberId: "member-uuid-1",
		}, nil
	}

	token, memberID, err := r.Redeem(context.Background(), "aaaabbbbccccddddaaaabbbbccccdddd")
	if err != nil {
		t.Fatalf("Redeem: %v", err)
	}
	if token != "evt_00000000000000000000000000000000" || memberID != "member-uuid-1" {
		t.Errorf("got (%q, %q)", token, memberID)
	}
}

func TestRedeemerRedeem_AlreadyUsedSurfacesAlarm(t *testing.T) {
	fake, r := startFakeRedeemServer(t)
	fake.unwrapFn = func(_ *vaultpb.UnwrapRequest) (*vaultpb.UnwrapResponse, error) {
		// Mirrors the vault's FailedPrecondition mapping: "already used" is
		// the interception alarm and must reach the user verbatim.
		return nil, status.Error(codes.FailedPrecondition, "invite 'x' has already been used")
	}

	_, _, err := r.Redeem(context.Background(), "aaaabbbbccccddddaaaabbbbccccdddd")
	if err == nil || !strings.Contains(err.Error(), "already been used") {
		t.Fatalf("err = %v, want 'already been used' to surface", err)
	}
}
