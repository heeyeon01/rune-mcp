// Package vault is the Rune-Vault gRPC client.
//
// Under the runespace model the vault holds ALL FHE keys and is the sole
// runespace client. rune-mcp is a pure-Go client that only talks to this
// service — it never encrypts/decrypts or touches runespace directly.
//
// Responsibility:
//   - GetAgentManifest: fetch agent config (no keys)
//   - Insert: send a plaintext embedding + metadata; vault encrypts + seals + stores
//   - Search: send a plaintext query; vault searches + decrypts + opens metadata
package vault

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"time"

	vaultpb "github.com/CryptoLabInc/rune-admin/vault/pkg/vaultpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
)

// MaxMessageLength — 256MB on both send/recv (FHE ciphertexts are large, and
// the vault relays plaintext vectors which are small; keep the generous cap).
const MaxMessageLength = 256 * 1024 * 1024

// DefaultTimeout — per-RPC deadline.
const DefaultTimeout = 30 * time.Second

// HealthCheckTimeout — override on health probes.
const HealthCheckTimeout = 5 * time.Second

// Bundle is the agent manifest returned by GetAgentManifest. Under the
// client-side-crypto model it carries the PUBLIC EncKey pair and the caller's
// derived agent_dek so rune-mcp can encrypt/seal locally; SecKey and
// team_secret stay in the Vault.
type Bundle struct {
	AgentID   string
	KeyID     string
	IndexName string
	Dim       int

	EncKeyJSON         []byte // RMP EncKey envelope (verbatim)
	MMEncKey           []byte // MM EncKey raw bytes (base64-decoded)
	AgentDEK           []byte // metadata seal key (base64-decoded)
	CentroidSetVersion string // engine's current set; "" = none loaded yet
	InsertCapability   string // "pre_encrypted" for the target vault
}

type manifestJSON struct {
	AgentID            string `json:"agent_id"`
	KeyID              string `json:"key_id"`
	IndexName          string `json:"index_name"`
	Dim                int    `json:"dim"`
	EncKeyJSON         string `json:"EncKey.json"`
	MMEncKey           string `json:"mm_enc_key"`
	AgentDEK           string `json:"agent_dek"`
	CentroidSetVersion string `json:"centroid_set_version"`
	Insert             string `json:"insert"`
}

func ParseManifestJSON(raw string) (*Bundle, error) {
	var m manifestJSON
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return nil, fmt.Errorf("vault: parse manifest_json: %w", err)
	}
	b := &Bundle{
		AgentID:            m.AgentID,
		KeyID:              m.KeyID,
		IndexName:          m.IndexName,
		Dim:                m.Dim,
		EncKeyJSON:         []byte(m.EncKeyJSON),
		CentroidSetVersion: m.CentroidSetVersion,
		InsertCapability:   m.Insert,
	}
	if m.MMEncKey != "" {
		mm, err := base64.StdEncoding.DecodeString(m.MMEncKey)
		if err != nil {
			return nil, fmt.Errorf("vault: decode mm_enc_key: %w", err)
		}
		b.MMEncKey = mm
	}
	if m.AgentDEK != "" {
		dek, err := base64.StdEncoding.DecodeString(m.AgentDEK)
		if err != nil {
			return nil, fmt.Errorf("vault: decode agent_dek: %w", err)
		}
		b.AgentDEK = dek
	}
	return b, nil
}

// Hit is one decrypted, ranked search result. Metadata is plaintext JSON
// (the vault opened the sealed envelope).
type Hit struct {
	ID       string
	Score    float64
	Metadata string
}

// InsertItem is a client-encrypted capture item forwarded verbatim to
// runespace via the Vault. ID is client-generated so retries are idempotent.
type InsertItem struct {
	ID                 string
	RMPItem            []byte // EncryptFlat output
	MMItem             []byte // EncryptClustered output
	ClusterID          uint32
	CentroidSetVersion string
	SealedMetadata     string // client-sealed {"a","c"} envelope
	// ShareGroups is the caller's capture-time group selection (plan §6-D6): the
	// DIRECT groups (role >= write) to tag this item with. Empty = all of the
	// caller's direct write groups. Plaintext — the Vault reads these to compute
	// the runespace filter tags; never sealed into the metadata envelope.
	ShareGroups []string
}

// CentroidSet is the relayed IVF centroid set (runespace -> vault -> here).
// Preset is a version-hash ingredient — relayed through to runed so it can
// recompute and verify the content hash ("" when the vault predates it).
type CentroidSet struct {
	Version string
	Dim     int
	Preset  string
	Vectors [][]float32
}

// Client interface — implemented by gRPC client (and test mocks).
type Client interface {
	GetAgentManifest(ctx context.Context) (*Bundle, error)
	Insert(ctx context.Context, item InsertItem) (string, error)
	Search(ctx context.Context, vector []float32, topK int) ([]Hit, error)
	Centroids(ctx context.Context) (*CentroidSet, error)
	GetPermissions(ctx context.Context, rootGroup string, includeMemberRoles bool) (*Permissions, error)
	HealthCheck(ctx context.Context) (bool, error)
	Endpoint() string
	Close() error
}

type ClientOpts struct {
	CACertPath        string // path to PEM; empty = system CA bundle
	TLSDisable        bool
	UnaryInterceptors []grpc.UnaryClientInterceptor
}

// client is the gRPC implementation.
type client struct {
	endpoint string
	token    string
	conn     *grpc.ClientConn
	stub     vaultpb.VaultServiceClient
}

var defaultKeepalive = keepalive.ClientParameters{
	Time:                30 * time.Second,
	Timeout:             10 * time.Second,
	PermitWithoutStream: true,
}

func NewClient(endpoint, token string, opts ClientOpts) (Client, error) {
	normalized, err := NormalizeEndpoint(endpoint)
	if err != nil {
		return nil, fmt.Errorf("vault: invalid endpoint: %w", err)
	}

	dialOpts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(MaxMessageLength),
			grpc.MaxCallSendMsgSize(MaxMessageLength),
		),
		grpc.WithKeepaliveParams(defaultKeepalive),
	}
	if len(opts.UnaryInterceptors) > 0 {
		dialOpts = append(dialOpts, grpc.WithChainUnaryInterceptor(opts.UnaryInterceptors...))
	}

	switch {
	case opts.TLSDisable:
		slog.Warn("vault: TLS disabled — gRPC traffic is unencrypted. Only use for local development.")
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	case opts.CACertPath != "":
		creds, err := credentials.NewClientTLSFromFile(opts.CACertPath, "")
		if err != nil {
			return nil, fmt.Errorf("vault: failed to load CA cert %s: %w", opts.CACertPath, err)
		}
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(creds))
	default:
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(nil)))
	}

	conn, err := grpc.NewClient(normalized, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("vault: grpc dial failed: %w", err)
	}

	slog.Info("vault: connected", "endpoint", normalized)
	return newWithConn(normalized, token, conn), nil
}

// NewBufconnClient wraps an existing *grpc.ClientConn for tests.
func NewBufconnClient(conn *grpc.ClientConn, token string) Client {
	return newWithConn("bufconn", token, conn)
}

func newWithConn(endpoint, token string, conn *grpc.ClientConn) *client {
	return &client{
		endpoint: endpoint,
		token:    token,
		conn:     conn,
		stub:     vaultpb.NewVaultServiceClient(conn),
	}
}

func (c *client) authCtx(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+c.token)
}

func withTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if dl, ok := ctx.Deadline(); ok && time.Until(dl) <= d {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, d)
}

func (c *client) GetAgentManifest(ctx context.Context) (*Bundle, error) {
	ctx, cancel := withTimeout(c.authCtx(ctx), DefaultTimeout)
	defer cancel()

	resp, err := c.stub.GetAgentManifest(ctx, &vaultpb.GetAgentManifestRequest{Token: c.token})
	if err != nil {
		return nil, MapGRPCError(err)
	}
	if msg := resp.GetError(); msg != "" {
		return nil, &Error{Code: ErrVaultInternal.Code, Message: "GetAgentManifest: " + msg, Retryable: true}
	}
	return ParseManifestJSON(resp.GetManifestJson())
}

func (c *client) Insert(ctx context.Context, item InsertItem) (string, error) {
	ctx, cancel := withTimeout(c.authCtx(ctx), DefaultTimeout)
	defer cancel()

	resp, err := c.stub.Insert(ctx, &vaultpb.InsertRequest{
		Token:              c.token,
		Id:                 item.ID,
		RmpItem:            item.RMPItem,
		MmItem:             item.MMItem,
		ClusterId:          item.ClusterID,
		CentroidSetVersion: item.CentroidSetVersion,
		Metadata:           item.SealedMetadata,
		ShareGroups:        item.ShareGroups,
	})
	if err != nil {
		return "", MapGRPCError(err)
	}
	if msg := resp.GetError(); msg != "" {
		return "", &Error{Code: ErrVaultInternal.Code, Message: "Insert: " + msg, Retryable: true}
	}
	return resp.GetId(), nil
}

// Permissions is the caller's authorization view (GetPermissions).
type Permissions struct {
	Me          string       // caller email (the person key)
	Memberships []Membership // the caller's DIRECT (group, role) bindings
	Tree        []GroupNode  // groups reachable by effective role, depth-annotated
	MemberRoles []MemberRole // org-wide listing; admin + includeMemberRoles only
}

// Membership is one direct (group, role) binding the caller holds.
type Membership struct {
	GroupID   string
	GroupName string
	Role      string // read | write | edit
}

// GroupNode is one group the caller can reach by effective role (recall scope),
// depth-annotated within the group tree.
type GroupNode struct {
	GroupID       string
	Name          string
	ParentID      string // empty for a root group
	Depth         int    // 0 = root of the group tree
	EffectiveRole string
}

// MemberRole is one org-wide (user, group, role) row (admin-only listing).
type MemberRole struct {
	User      string // email
	GroupID   string
	GroupName string
	Role      string
}

// GetPermissions returns the caller's memberships + reachable group tree (and,
// for the org admin with includeMemberRoles, the org-wide member-roles listing).
func (c *client) GetPermissions(ctx context.Context, rootGroup string, includeMemberRoles bool) (*Permissions, error) {
	ctx, cancel := withTimeout(c.authCtx(ctx), DefaultTimeout)
	defer cancel()

	resp, err := c.stub.GetPermissions(ctx, &vaultpb.GetPermissionsRequest{
		Token:              c.token,
		RootGroup:          rootGroup,
		IncludeMemberRoles: includeMemberRoles,
	})
	if err != nil {
		return nil, MapGRPCError(err)
	}
	if msg := resp.GetError(); msg != "" {
		return nil, &Error{Code: ErrVaultInternal.Code, Message: "GetPermissions: " + msg, Retryable: true}
	}

	out := &Permissions{Me: resp.GetMe()}
	for _, m := range resp.GetMemberships() {
		out.Memberships = append(out.Memberships, Membership{
			GroupID:   m.GetGroupId(),
			GroupName: m.GetGroupName(),
			Role:      m.GetRole(),
		})
	}
	for _, n := range resp.GetTree() {
		out.Tree = append(out.Tree, GroupNode{
			GroupID:       n.GetGroupId(),
			Name:          n.GetName(),
			ParentID:      n.GetParentId(),
			Depth:         int(n.GetDepth()),
			EffectiveRole: n.GetEffectiveRole(),
		})
	}
	for _, mr := range resp.GetMemberRoles() {
		out.MemberRoles = append(out.MemberRoles, MemberRole{
			User:      mr.GetUser(),
			GroupID:   mr.GetGroupId(),
			GroupName: mr.GetGroupName(),
			Role:      mr.GetRole(),
		})
	}
	return out, nil
}

// Centroids pulls the relayed IVF centroid set (header + id-ordered batches).
func (c *client) Centroids(ctx context.Context) (*CentroidSet, error) {
	ctx, cancel := withTimeout(c.authCtx(ctx), DefaultTimeout)
	defer cancel()

	stream, err := c.stub.GetCentroids(ctx, &vaultpb.GetCentroidsRequest{Token: c.token})
	if err != nil {
		return nil, MapGRPCError(err)
	}
	cs := &CentroidSet{}
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, MapGRPCError(err)
		}
		switch p := chunk.GetPayload().(type) {
		case *vaultpb.CentroidChunk_Header:
			cs.Version = p.Header.GetVersion()
			cs.Dim = int(p.Header.GetDim())
			cs.Preset = p.Header.GetPreset()
			if n := p.Header.GetNlist(); n > 0 {
				cs.Vectors = make([][]float32, 0, n)
			}
		case *vaultpb.CentroidChunk_Batch:
			for _, ct := range p.Batch.GetCentroids() {
				cs.Vectors = append(cs.Vectors, ct.GetVec())
			}
		}
	}
	return cs, nil
}

func (c *client) Search(ctx context.Context, vector []float32, topK int) ([]Hit, error) {
	ctx, cancel := withTimeout(c.authCtx(ctx), DefaultTimeout)
	defer cancel()

	resp, err := c.stub.Search(ctx, &vaultpb.SearchRequest{
		Token:  c.token,
		Vector: vector,
		TopK:   int32(topK),
	})
	if err != nil {
		return nil, MapGRPCError(err)
	}
	if msg := resp.GetError(); msg != "" {
		return nil, &Error{Code: ErrVaultInternal.Code, Message: "Search: " + msg, Retryable: true}
	}
	out := make([]Hit, 0, len(resp.GetHits()))
	for _, h := range resp.GetHits() {
		out = append(out, Hit{ID: h.GetId(), Score: h.GetScore(), Metadata: h.GetMetadata()})
	}
	return out, nil
}

func (c *client) HealthCheck(ctx context.Context) (bool, error) {
	ctx, cancel := withTimeout(ctx, HealthCheckTimeout)
	defer cancel()

	stub := grpc_health_v1.NewHealthClient(c.conn)
	resp, err := stub.Check(ctx, &grpc_health_v1.HealthCheckRequest{Service: ""})
	if err != nil {
		return false, MapGRPCError(err)
	}
	return resp.GetStatus() == grpc_health_v1.HealthCheckResponse_SERVING, nil
}

func (c *client) Endpoint() string { return c.endpoint }

func (c *client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
