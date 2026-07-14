// Invite redemption — the pre-auth half of the vault client.
//
// A new member receives a one-time invite code by mail. The code is not a
// secret (a coat-check ticket): the real access token stays sealed inside the
// vault until Unwrap exchanges the ticket for it, exactly once. These are the
// only vault calls made WITHOUT a token, and they happen once, before
// ~/.rune/config.json exists — which is why they live on their own
// short-lived Redeemer instead of Client (design-decisions §8.3/§8.4,
// model P: rune-mcp redeems the code itself, so the released token only ever
// travels vault → this process over TLS, never through a screen, a clipboard,
// or an agent conversation).
package vault

import (
	"context"
	"fmt"

	vaultpb "github.com/CryptoLabInc/rune-admin/vault/pkg/vaultpb"
	"google.golang.org/grpc"
)

// ExpectedInviteCreationPath is the wrap path every legitimate member invite
// is minted under (rune-admin admin_members.go, inviteCreationPath). The
// vault echoes an invite's creation path on LookupWrap; Lookup refuses any
// other value — the client half of the §8.3 wrap-path binding, so a wrap
// minted by some other surface cannot be passed off as a member invite.
const ExpectedInviteCreationPath = "admin.member.invite"

// InviteInfo is the secret-free invite view LookupWrap returns — enough for
// the user to see who and what they are accepting before the code is burned.
type InviteInfo struct {
	Email     string
	Role      string
	ExpiresAt string // RFC3339 UTC
}

// Redeemer performs the pre-auth redemption calls (LookupWrap, Unwrap) on one
// connection. Construct with NewRedeemer, Close after use.
type Redeemer struct {
	conn    *grpc.ClientConn
	ownConn bool
	stub    vaultpb.VaultServiceClient
}

// NewRedeemer dials endpoint under the same TLS/CA rules as NewClient.
func NewRedeemer(endpoint string, opts ClientOpts) (*Redeemer, error) {
	normalized, err := NormalizeEndpoint(endpoint)
	if err != nil {
		return nil, fmt.Errorf("vault: invalid endpoint: %w", err)
	}
	dialOpts, err := buildDialOpts(opts)
	if err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient(normalized, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("vault: grpc dial failed: %w", err)
	}
	return &Redeemer{conn: conn, ownConn: true, stub: vaultpb.NewVaultServiceClient(conn)}, nil
}

// NewRedeemerFromConn wraps an existing connection (tests: bufconn). The
// caller keeps ownership of conn.
func NewRedeemerFromConn(conn *grpc.ClientConn) *Redeemer {
	return &Redeemer{conn: conn, stub: vaultpb.NewVaultServiceClient(conn)}
}

// Lookup pre-validates an invite code: read-only, never consumes the code,
// never returns the token. It also enforces the creation-path binding.
func (r *Redeemer) Lookup(ctx context.Context, code string) (*InviteInfo, error) {
	ctx, cancel := withTimeout(ctx, DefaultTimeout)
	defer cancel()

	resp, err := r.stub.LookupWrap(ctx, &vaultpb.LookupWrapRequest{Handle: code})
	if err != nil {
		return nil, MapGRPCError(err)
	}
	if msg := resp.GetError(); msg != "" {
		return nil, &Error{Code: ErrVaultInternal.Code, Message: "LookupWrap: " + msg, Retryable: false}
	}
	if got := resp.GetCreationPath(); got != ExpectedInviteCreationPath {
		return nil, fmt.Errorf("vault: invite creation path %q is not the member-invite surface — refusing to redeem", got)
	}
	return &InviteInfo{
		Email:     resp.GetEmail(),
		Role:      resp.GetRole(),
		ExpiresAt: resp.GetExpiresAt(),
	}, nil
}

// Redeem consumes the one-time code and returns the released token plus the
// activated member id. The token must go straight into config.json — never
// into a tool result, a log line, or an error message.
func (r *Redeemer) Redeem(ctx context.Context, code string) (token, memberID string, err error) {
	ctx, cancel := withTimeout(ctx, DefaultTimeout)
	defer cancel()

	resp, err := r.stub.Unwrap(ctx, &vaultpb.UnwrapRequest{Handle: code})
	if err != nil {
		return "", "", MapGRPCError(err)
	}
	if msg := resp.GetError(); msg != "" {
		return "", "", &Error{Code: ErrVaultInternal.Code, Message: "Unwrap: " + msg, Retryable: false}
	}
	return resp.GetToken(), resp.GetMemberId(), nil
}

// Close releases the connection if this Redeemer dialed it.
func (r *Redeemer) Close() error {
	if r.ownConn && r.conn != nil {
		return r.conn.Close()
	}
	return nil
}
