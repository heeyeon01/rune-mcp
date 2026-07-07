package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/CryptoLabInc/rune-mcp/internal/adapters/config"
	"github.com/CryptoLabInc/rune-mcp/internal/adapters/embedder"
	"github.com/CryptoLabInc/rune-mcp/internal/adapters/logio"
	"github.com/CryptoLabInc/rune-mcp/internal/adapters/vault"
	"github.com/CryptoLabInc/rune-mcp/internal/domain"
	"github.com/CryptoLabInc/rune-mcp/internal/lifecycle"
	"github.com/CryptoLabInc/rune-mcp/internal/spawn"
)

// LifecycleService holds the 6 lifecycle/operational tool implementations.
// Spec: docs/v04/spec/flows/lifecycle.md.
type LifecycleService struct {
	Vault     vault.Client
	State     *lifecycle.Manager
	IndexName string
	ConfigDir string // for CaptureHistory reading capture_log.jsonl

	// Key state (for diagnostics). In the runespace model the vault is the sole
	// key custodian; mcp holds no key material, only the KeyID from the manifest.
	KeyID string

	bootstrapWatcherMu      sync.Mutex
	bootstrapWatcherRunning bool

	embedderMu sync.RWMutex
	embedder   embedder.Client
}

// NewLifecycleService constructs.
func NewLifecycleService() *LifecycleService {
	return &LifecycleService{}
}

func (s *LifecycleService) Embedder() embedder.Client {
	s.embedderMu.RLock()
	defer s.embedderMu.RUnlock()
	return s.embedder
}

func (s *LifecycleService) SetEmbedder(c embedder.Client) {
	s.embedderMu.Lock()
	defer s.embedderMu.Unlock()
	s.embedder = c
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. rune_vault_status — read-only. server.py:L496-528. Spec §1.
// ─────────────────────────────────────────────────────────────────────────────

// VaultStatusResult — lifecycle.md §1.
type VaultStatusResult struct {
	OK                    bool    `json:"ok"`
	VaultConfigured       bool    `json:"vault_configured"`
	VaultEndpoint         *string `json:"vault_endpoint,omitempty"`
	SecureSearchAvailable bool    `json:"secure_search_available"`
	Mode                  string  `json:"mode"` // "secure (Vault-backed)" | "standard (no Vault)"
	VaultHealthy          *bool   `json:"vault_healthy,omitempty"`
	TeamIndexName         *string `json:"team_index_name,omitempty"`
	Warning               *string `json:"warning,omitempty"`
}

// VaultStatus — branches on vault == nil (standard mode) vs configured.
func (s *LifecycleService) VaultStatus(ctx context.Context) (*VaultStatusResult, error) {
	if s.Vault == nil {
		warning := "secret key may be accessible locally. Configure Vault for secure mode."
		return &VaultStatusResult{
			OK:              true,
			VaultConfigured: false,
			Mode:            "standard (no Vault)",
			Warning:         &warning,
		}, nil
	}

	endpoint := s.Vault.Endpoint()
	healthy, err := s.Vault.HealthCheck(ctx)
	if err != nil {
		slog.Warn("vault health check failed", "err", err)
		h := false
		return &VaultStatusResult{
			OK:              true,
			VaultConfigured: true,
			VaultEndpoint:   &endpoint,
			VaultHealthy:    &h,
			Mode:            "secure (Vault-backed)",
		}, nil
	}

	return &VaultStatusResult{
		OK:                    true,
		VaultConfigured:       true,
		VaultEndpoint:         &endpoint,
		SecureSearchAvailable: healthy,
		Mode:                  "secure (Vault-backed)",
		VaultHealthy:          &healthy,
	}, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. rune_diagnostics — read-only. server.py:L540-684. Spec §2.
// ─────────────────────────────────────────────────────────────────────────────

// DiagnosticsResult — aggregates 7 sub-sections (env + runtime ×6). Install
// state (config.json, runed binary, model file, socket) is a substrate
// concern owned by the `rune` CLI; agents wanting that visibility shell
// out to `rune verify` separately. Keeping the MCP server's diagnostics
// scoped to runtime state mirrors the rune ↔ rune-mcp repo boundary.
type DiagnosticsResult struct {
	OK            bool          `json:"ok"`
	Environment   EnvInfo       `json:"environment"`
	State         *string       `json:"state,omitempty"`
	DormantReason *string       `json:"dormant_reason,omitempty"`
	DormantSince  *string       `json:"dormant_since,omitempty"`
	Vault         VaultInfo     `json:"vault"`
	Keys          KeysInfo      `json:"keys"`
	Pipelines     PipelinesInfo `json:"pipelines"`
	Embedding     EmbeddingInfo `json:"embedding"`
	Envector      EnvectorInfo  `json:"envector"`
}

// EnvInfo — OS, Go runtime version, cwd.
type EnvInfo struct {
	OS      string `json:"os"`
	Runtime string `json:"runtime"`
	CWD     string `json:"cwd"`
	GOArch  string `json:"goarch"`
}

// VaultInfo — subset exposed in diagnostics.
//
// Configured = a Vault gRPC client is wired (boot loop reached Active).
// Healthy    = the most recent HealthCheck succeeded.
// Error      = HealthCheck error (operational, set only when Healthy=false).
// LastBootError = structured boot failure from lifecycle.Manager. Surfaces
//
//	the actual reason for waiting_for_vault state — agents
//	branch on .kind to fast-fail without manual probing. Nil
//	when boot has succeeded or no attempt has been made yet.
type VaultInfo struct {
	Configured    bool              `json:"configured"`
	Healthy       bool              `json:"healthy"`
	Endpoint      string            `json:"endpoint,omitempty"`
	Error         string            `json:"error,omitempty"`
	LastBootError *domain.BootError `json:"last_boot_error,omitempty"`
}

// KeysInfo — key custody status. In the runespace model the vault is the sole
// key custodian (EncKey/EvalKey/SecKey never leave it); the mcp process holds
// no key material. Provisioned reports that the vault has keys ready for this
// index.
type KeysInfo struct {
	Custodian   string `json:"custodian"`   // "vault" — sole key holder
	Provisioned bool   `json:"provisioned"` // vault has keys ready
	KeyID       string `json:"key_id,omitempty"`
}

// PipelinesInfo — scribe/retriever init state.
type PipelinesInfo struct {
	ScribeInitialized    bool `json:"scribe_initialized"`
	RetrieverInitialized bool `json:"retriever_initialized"`
}

// EmbeddingInfo - external embedder info snapshot
//
// Phase / BytesDone / BytesTotal / Message are populated when Status is
// LOADING
type EmbeddingInfo struct {
	Model         string `json:"model"`
	Mode          string `json:"mode"` // "external gRPC"
	VectorDim     int    `json:"vector_dim,omitempty"`
	DaemonVersion string `json:"daemon_version,omitempty"`
	SocketPath    string `json:"socket_path,omitempty"`
	Status        string `json:"status,omitempty"` // Health: OK / LOADING / DEGRADED / SHUTTING_DOWN
	UptimeSeconds int64  `json:"uptime_seconds,omitempty"`
	TotalRequests int64  `json:"total_requests,omitempty"`
	Phase         string `json:"phase,omitempty"`       // bootstrap sub-phase; meaningful when Status == LOADING
	BytesDone     int64  `json:"bytes_done,omitempty"`  // download progress
	BytesTotal    int64  `json:"bytes_total,omitempty"` // 0 when unknown / not downloading
	Message       string `json:"message,omitempty"`     // free-text detail for end-user display
	InfoError     string `json:"info_error,omitempty"`
	HealthError   string `json:"health_error,omitempty"`
}

// EnvectorInfo — reachability probe
type EnvectorInfo struct {
	Reachable bool    `json:"reachable"`
	LatencyMs float64 `json:"latency_ms,omitempty"`
	Error     string  `json:"error,omitempty"`
	ErrorType string  `json:"error_type,omitempty"` // connection_refused|auth_failure|deadline_exceeded|timeout|unknown
	ElapsedMs float64 `json:"elapsed_ms,omitempty"`
	Hint      string  `json:"hint,omitempty"`
}

// DiagnosticsTimeout — Python ENVECTOR_DIAGNOSIS_TIMEOUT (server.py:L633). 5s.
const DiagnosticsTimeout = 5 * time.Second

// Diagnostics collects all 7 sections + derives top-level OK.
func (s *LifecycleService) Diagnostics(ctx context.Context) *DiagnosticsResult {
	r := &DiagnosticsResult{OK: true}

	// Environment
	cwd, _ := os.Getwd()
	r.Environment = EnvInfo{
		OS:      runtime.GOOS,
		Runtime: runtime.Version(),
		CWD:     cwd,
		GOArch:  runtime.GOARCH,
	}

	// Config state
	cfg, err := config.Load()
	if err == nil && cfg != nil {
		state := cfg.State
		r.State = &state
		if cfg.DormantReason != "" {
			r.DormantReason = &cfg.DormantReason
		}
		if cfg.DormantSince != "" {
			r.DormantSince = &cfg.DormantSince
		}
	}

	// Vault
	r.Vault = s.collectVault(ctx, DiagnosticsTimeout)

	// Keys
	r.Keys = KeysInfo{
		Custodian:   "vault",
		Provisioned: r.Vault.Healthy,
		KeyID:       s.KeyID,
	}

	// Pipelines
	r.Pipelines = PipelinesInfo{
		ScribeInitialized:    s.State != nil && s.State.Current() == lifecycle.StateActive,
		RetrieverInitialized: s.State != nil && s.State.Current() == lifecycle.StateActive,
	}

	// Embedding
	r.Embedding = s.collectEmbedding(ctx, DiagnosticsTimeout)

	// Envector
	r.Envector = s.collectEnvector(ctx, DiagnosticsTimeout)

	if s.Vault != nil && !r.Vault.Healthy {
		r.OK = false
	}

	return r
}

func (s *LifecycleService) collectVault(ctx context.Context, timeout time.Duration) VaultInfo {
	info := VaultInfo{Configured: s.Vault != nil}

	// Surface the most recent boot failure regardless of client state.
	// When the boot loop is stuck on waiting_for_vault, s.Vault is nil but
	// LastBootError holds the actual reason — agents need this to skip
	// expensive trial-and-error diagnosis.
	if s.State != nil {
		if be := s.State.LastBootError(); be != nil {
			info.LastBootError = be
		}
	}

	if s.Vault == nil {
		return info
	}

	info.Endpoint = s.Vault.Endpoint()

	ctx2, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	healthy, err := s.Vault.HealthCheck(ctx2)
	if err != nil {
		info.Error = err.Error()
	}

	info.Healthy = healthy

	return info
}

func (s *LifecycleService) collectEmbedding(ctx context.Context, timeout time.Duration) EmbeddingInfo {
	info := EmbeddingInfo{Mode: "external gRPC"}
	e := s.Embedder()
	if e == nil {
		return info
	}
	info.SocketPath = e.SocketPath()

	infoCtx, cancelInfo := context.WithTimeout(ctx, timeout)
	defer cancelInfo()
	if snap, err := e.Info(infoCtx); err != nil {
		info.InfoError = err.Error()
	} else {
		info.Model = snap.ModelIdentity
		info.VectorDim = snap.VectorDim
		info.DaemonVersion = snap.DaemonVersion
	}

	healthCtx, cancelHealth := context.WithTimeout(ctx, timeout)
	defer cancelHealth()

	if health, err := e.Health(healthCtx); err != nil {
		info.HealthError = err.Error()
	} else {
		info.Status = health.Status
		info.UptimeSeconds = health.UptimeSeconds
		info.TotalRequests = health.TotalRequests
		info.Phase = health.Phase
		info.BytesDone = health.BytesDone
		info.BytesTotal = health.BytesTotal
		info.Message = health.Message
	}

	return info
}

func (s *LifecycleService) collectEnvector(ctx context.Context, timeout time.Duration) EnvectorInfo {
	if s.Vault == nil {
		return EnvectorInfo{}
	}
	info, _ := s.probeEnvector(ctx, timeout)
	return info
}

// probeEnvector reports runespace reachability via the vault (mcp no longer
// talks to the vector engine directly — the vault is the sole client).
func (s *LifecycleService) probeEnvector(ctx context.Context, timeout time.Duration) (EnvectorInfo, error) {
	ctx2, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ch := make(chan error, 1)
	t0 := time.Now()

	go func() {
		_, err := s.Vault.HealthCheck(ctx2)
		ch <- err
	}()

	select {
	case probeErr := <-ch:
		elapsed := time.Since(t0)
		if probeErr == nil {
			return EnvectorInfo{
				Reachable: true,
				LatencyMs: float64(elapsed.Milliseconds()),
			}, nil
		}
		errType, hint := ClassifyEnvectorError(probeErr, elapsed)
		return EnvectorInfo{
			Error:     probeErr.Error(),
			ErrorType: string(errType),
			Hint:      hint,
			ElapsedMs: float64(elapsed.Milliseconds()),
		}, probeErr
	case <-ctx2.Done():
		elapsed := time.Since(t0)
		return EnvectorInfo{
			Error: fmt.Sprintf(
				"Health check timed out after %.0fs (elapsed: %.1fms).",
				timeout.Seconds(), float64(elapsed.Milliseconds()),
			),
			ErrorType: string(EnvErrTimeout),
			ElapsedMs: float64(elapsed.Milliseconds()),
		}, ctx2.Err()
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. rune_capture_history — read-only. server.py:L1092-1111. Spec §4.
// ─────────────────────────────────────────────────────────────────────────────

// CaptureHistoryArgs — limit default 20, max 100.
type CaptureHistoryArgs struct {
	Limit  int     `json:"limit,omitempty"`
	Domain *string `json:"domain,omitempty"`
	Since  *string `json:"since,omitempty"` // ISO date lex compare
}

// CaptureHistoryResult — entries preserved as map for format flexibility.
type CaptureHistoryResult struct {
	OK      bool             `json:"ok"`
	Count   int              `json:"count"`
	Entries []map[string]any `json:"entries"`
}

// CaptureHistory — reverse-read capture_log.jsonl, filter, cap at limit.
func (s *LifecycleService) CaptureHistory(_ context.Context, args CaptureHistoryArgs) (*CaptureHistoryResult, error) {
	limit := args.Limit
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}

	logPath := filepath.Join(s.ConfigDir, logio.DefaultFilename)
	entries, err := logio.Tail(logPath, limit, args.Domain, args.Since)
	if err != nil {
		slog.Warn("capture history read failed (degraded)", "err", err)
		return &CaptureHistoryResult{OK: true, Entries: []map[string]any{}}, nil
	}

	// Convert to map[string]any for format flexibility
	mapEntries := make([]map[string]any, len(entries))
	for i, e := range entries {
		data, _ := json.Marshal(e)
		var m map[string]any
		_ = json.Unmarshal(data, &m)
		mapEntries[i] = m
	}

	return &CaptureHistoryResult{
		OK:      true,
		Count:   len(mapEntries),
		Entries: mapEntries,
	}, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. rune_delete_capture — soft-delete. server.py:L1123-1206. Spec §5.
// ─────────────────────────────────────────────────────────────────────────────

// DeleteCaptureArgs — single record ID target.
type DeleteCaptureArgs struct {
	RecordID string `json:"record_id"`
}

// DeleteCaptureResult.
type DeleteCaptureResult struct {
	OK       bool   `json:"ok"`
	Deleted  bool   `json:"deleted"`
	RecordID string `json:"record_id"`
	Title    string `json:"title"`
	Method   string `json:"method"` // "soft-delete (status=reverted)"
}

// DeleteCapture — soft-delete workflow:
//  1. SearchByID(id): find record
//  2. set metadata["status"] = "reverted"
//  3. re-embed + re-insert
//  4. capture_log append with mode="soft-delete", action="deleted"
func (s *LifecycleService) DeleteCapture(ctx context.Context, args DeleteCaptureArgs, capSvc *CaptureService) (*DeleteCaptureResult, error) {
	// Search by ID
	hit, err := SearchByID(ctx, s.Embedder(), s.Vault, s.IndexName, args.RecordID)
	if err != nil {
		return nil, fmt.Errorf("delete: search by ID: %w", err)
	}
	if hit == nil {
		return nil, &domain.RuneError{
			Code:    domain.CodeInvalidInput,
			Message: fmt.Sprintf("record %s not found", args.RecordID),
		}
	}

	// Mutate metadata
	metadata := hit.Metadata
	if metadata == nil {
		metadata = make(map[string]any)
	}
	metadata["status"] = "reverted"
	title := hit.Title

	// Re-embed + re-insert
	embedText := hit.ReusableInsight
	if embedText == "" {
		embedText = hit.PayloadText
	}

	vec, err := s.Embedder().EmbedSingle(ctx, embedText)
	if err != nil {
		return nil, fmt.Errorf("delete: re-embed: %w", err)
	}

	body, err := json.Marshal(metadata)
	if err != nil {
		return nil, fmt.Errorf("delete: marshal: %w", err)
	}

	// Re-insert via the vault (vault encrypts + seals + stores). Soft-delete
	// re-tagging (preserving the record's original share groups) is an M6/UpdateTags
	// concern — out of scope here, and delete_capture is gated out this release — so
	// share_groups is nil (vault applies its default tagging).
	if _, err := s.Vault.Insert(ctx, vec, string(body), nil); err != nil {
		return nil, fmt.Errorf("delete: re-insert: %w", err)
	}

	// Capture log
	if capSvc != nil && capSvc.CaptureLog != nil {
		_ = capSvc.CaptureLog.Append(domain.CaptureLogEntry{
			TS:     time.Now().UTC().Format(time.RFC3339),
			Action: "deleted",
			ID:     args.RecordID,
			Title:  title,
			Domain: hit.Domain,
			Mode:   "soft-delete",
		})
	}

	return &DeleteCaptureResult{
		OK:       true,
		Deleted:  true,
		RecordID: args.RecordID,
		Title:    title,
		Method:   "soft-delete (status=reverted)",
	}, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// 5. rune_configure — write Vault credentials to $HOME/.rune/config.json.
// ─────────────────────────────────────────────────────────────────────────────

type ConfigureArgs struct {
	Endpoint   string `json:"endpoint"`
	Token      string `json:"token"`
	CACertPath string `json:"ca_cert_path,omitempty"`
	TLSDisable bool   `json:"tls_disable,omitempty"`
}

type ConfigureResult struct {
	OK           bool   `json:"ok"`
	Path         string `json:"path"`
	State        string `json:"state"`
	ConfiguredAt string `json:"configured_at"`
	NextStep     string `json:"next_step,omitempty"`

	// Reachable=nil  : skip probe
	// Reachable-false: HealthCheck failed, ProbeError is the reason
	VaultReachable *bool  `json:"vault_reachable,omitempty"`
	ProbeError     string `json:"probe_error,omitempty"`
}

const ConfigureProbeTimeout = 5 * time.Second

func (s *LifecycleService) Configure(ctx context.Context, args ConfigureArgs) (*ConfigureResult, error) {
	if args.Endpoint == "" {
		return nil, &domain.RuneError{Code: domain.CodeInvalidInput, Message: "endpoint is required"}
	}
	if args.Token == "" {
		return nil, &domain.RuneError{Code: domain.CodeInvalidInput, Message: "token is required"}
	}

	cfg, err := config.Load()
	if err != nil {
		cfg = &config.Config{} // fall back to fresh config
	}

	cfg.Vault = config.VaultConfig{
		Endpoint:   args.Endpoint,
		Token:      args.Token,
		CACert:     args.CACertPath,
		TLSDisable: args.TLSDisable,
	}
	cfg.State = "active"
	cfg.DormantReason = ""
	cfg.DormantSince = ""

	now := time.Now().UTC().Format(time.RFC3339)
	if cfg.Metadata == nil {
		cfg.Metadata = map[string]any{}
	}
	cfg.Metadata["lastUpdated"] = now

	if err := config.Save(cfg); err != nil {
		return nil, fmt.Errorf("save config: %w", err)
	}

	path, _ := config.DefaultConfigPath()
	result := &ConfigureResult{
		OK:           true,
		Path:         path,
		State:        cfg.State,
		ConfiguredAt: now,
	}

	// Vault HealthCheck
	probeCtx, cancel := context.WithTimeout(ctx, ConfigureProbeTimeout)
	defer cancel()

	vc, probeErr := vault.NewClient(args.Endpoint, args.Token, vault.ClientOpts{
		CACertPath: args.CACertPath,
		TLSDisable: args.TLSDisable,
	})
	if probeErr == nil {
		_, probeErr = vc.HealthCheck(probeCtx)
		_ = vc.Close()
	}

	reachable := probeErr == nil
	result.VaultReachable = &reachable
	if probeErr != nil {
		result.ProbeError = probeErr.Error()
		result.NextStep = "Vault unreachable from this host - verify endpoint/token, then run /rune:activate to retry"
	} else {
		result.NextStep = "Run /rune:activate to apply the new credentials"
	}

	return result, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// 6. rune_activate - pre-check + reload
//
//  ActivateStatus:
//	  configure_required  - config.json missing or vault block empty
//	  install_pending     - runed socket absent (daemon not installed/running)
//	  active / waiting_for_vault / dormant - passed through from reload
// ─────────────────────────────────────────────────────────────────────────────

const (
	ActivateStatusConfigureRequired   = "configure_required"
	ActivateStatusInstallPending      = "install_pending"
	ActivateStatusActive              = "active"
	ActivateStatusWaitingForVault     = "waiting_for_vault"
	ActivateStatusWaitingForBootstrap = "waiting_for_bootstrap"
	ActivateStatusDormant             = "dormant"
)

// Runed reports during STATUS_LOADING
type BootstrapDetail struct {
	Phase      string `json:"phase,omitempty"`       // FETCHING_LLAMA_SERVER / FETCHING_MODEL / STARTING_LLAMA_SERVER
	BytesDone  int64  `json:"bytes_done,omitempty"`  // download progress
	BytesTotal int64  `json:"bytes_total,omitempty"` // 0 when unknown / not downloading
	Message    string `json:"message,omitempty"`     // free-text detail for end-user display
}

// When Status is active / waiting_for_vault / dormant, Reload mirrors ReloadPipilines
// When Status is waiting_for_bootstrap, Bootstrap mirrors runed's self-bootstrap progress
type ActivateResult struct {
	OK        bool                   `json:"ok"`
	Status    string                 `json:"status"`
	Hint      string                 `json:"hint,omitempty"`
	Bootstrap *BootstrapDetail       `json:"bootstrap,omitempty"`
	Reload    *ReloadPipelinesResult `json:"reload,omitempty"`
}

const bootstrapProbeTimeout = 2 * time.Second

func (s *LifecycleService) Activate(ctx context.Context) (*ActivateResult, error) {
	// Pre-check: config ($HOME/.rune/config.json)
	cfg, err := config.Load()
	if err != nil || cfg == nil {
		return &ActivateResult{
			OK:     true,
			Status: ActivateStatusConfigureRequired,
			Hint:   "Run /rune:configure to write Vault credentials.",
		}, nil
	}
	if cfg.Vault.Endpoint == "" || cfg.Vault.Token == "" {
		return &ActivateResult{
			OK:     true,
			Status: ActivateStatusConfigureRequired,
			Hint:   "Vault endpoint/token missing in ~/.rune/config.json. Run /rune:configure.",
		}, nil
	}

	// Clear a user-initiated deactivation before re-triggering the boot loop.
	//
	// /rune:activate is an explicit "come back online" intent. If the daemon
	// was put to sleep by /rune:deactivate, config.State is "dormant" with
	// reason "user_deactivated" (boot.go also treats a bare dormant state with
	// no reason as user_deactivated). The boot loop reads config.State != active
	// and immediately re-enters dormant, so without clearing the marker here the
	// reload below is a no-op and activation never takes effect.
	//
	// Scope is deliberately limited to user-deactivation: substrate-driven
	// dormancy (not_configured / vault_unconfigured) is already handled by the
	// configure_required pre-checks above, so reaching this point means valid
	// credentials exist and the user's own deactivation is the only thing
	// pinning the daemon dormant.
	if cfg.State == "dormant" && (cfg.DormantReason == "" || cfg.DormantReason == "user_deactivated") {
		if err := config.ClearDormant(); err != nil {
			return nil, fmt.Errorf("activate: clear user-deactivated state: %w", err)
		}
	}

	// Pre-check: runed socket path ($RUNE_EMBEDDER_SOCKET or $HOME/.runed/embedding.sock)
	socketPath := embedder.ResolveSocketPath("")
	if socketPath != "" {
		if br := s.ensureDaemon(ctx, socketPath); br != nil {
			return br, nil
		}
	}

	// Pre-check: runed bootstrap state. Show progress if runed is self-bootstrapping
	if s.Embedder() != nil {
		if br := s.probeBootstrap(ctx); br != nil {
			return br, nil
		}
	}

	// Call reload_pipelines
	rr, err := s.ReloadPipelines(ctx)
	if err != nil {
		return nil, fmt.Errorf("reload pipelines: %w", err)
	}

	return &ActivateResult{
		OK:     rr.OK,
		Status: rr.State,
		Reload: rr,
	}, nil
}

// Makes runed reachable at socketPath, spawning if needed
// On failure, install_pending hint tells agent command to run for recovery (spawn.AgentInstallRecoverHint)
func (s *LifecycleService) ensureDaemon(ctx context.Context, socketPath string) *ActivateResult {
	runeBin, err := spawn.ResolveRuneBinary()
	if err != nil {
		return &ActivateResult{
			OK:     true,
			Status: ActivateStatusInstallPending,
			Hint:   fmt.Sprintf("rune CLI not found (%v). Agent recovery: invoke %s, then retry /rune:activate.", err, spawn.AgentInstallRecoveryHint()),
		}
	}

	paths, err := embedderPaths(socketPath)
	if err != nil {
		return &ActivateResult{
			OK:     true,
			Status: ActivateStatusInstallPending,
			Hint:   fmt.Sprintf("could not resolve runed paths: %v", err),
		}
	}

	cfg := spawn.Config{
		RuneBinary:    runeBin,
		SocketPath:    socketPath,
		SpawnLockPath: paths.spawnLock,
	}
	if err := spawn.EnsureDaemon(ctx, cfg); err != nil {
		return &ActivateResult{
			OK:     true,
			Status: ActivateStatusInstallPending,
			Hint:   fmt.Sprintf("auto-spawn of runed via `%s runed --detach` failed: %v. Inspect %s for runed startup logs. Agent recovery: invoke %s, then retry /rune:activate.", runeBin, err, paths.logHint, spawn.AgentInstallRecoveryHint()),
		}
	}
	return nil
}

type runedSpawnPaths struct {
	spawnLock string
	logHint   string
}

func embedderPaths(socketPath string) (runedSpawnPaths, error) {
	dir := filepath.Dir(socketPath)
	if dir == "" || dir == "." {
		return runedSpawnPaths{}, fmt.Errorf("invalid socket path %q", socketPath)
	}
	return runedSpawnPaths{
		spawnLock: filepath.Join(dir, "spawn.lock"),
		logHint:   filepath.Join(dir, "logs", "daemon.log"),
	}, nil
}

func (s *LifecycleService) probeBootstrap(ctx context.Context) *ActivateResult {
	probeCtx, cancel := context.WithTimeout(ctx, bootstrapProbeTimeout)
	defer cancel()

	e := s.Embedder()
	if e == nil {
		return nil
	}

	h, err := e.Health(probeCtx)
	if err != nil || h.Status != "LOADING" {
		return nil // Health errors are ignored here
	}

	s.startBootstrapWatcher()
	return &ActivateResult{
		OK:     true,
		Status: ActivateStatusWaitingForBootstrap,
		Hint:   "runed is bootstrapping (downloading llama-server and/or the embedding model). Activation will complete automatically once the download finishes - no further /rune:activate needed.",
		Bootstrap: &BootstrapDetail{
			Phase:      h.Phase,
			BytesDone:  h.BytesDone,
			BytesTotal: h.BytesTotal,
			Message:    h.Message,
		},
	}
}

var bootstrapWatchInterval = 15 * time.Second
var bootstrapWatcherHealthTimeout = 5 * time.Second

const bootstrapWatcherMaxErrors = 3
const bootstrapWatcherDeadline = 30 * time.Minute

func (s *LifecycleService) startBootstrapWatcher() {
	s.bootstrapWatcherMu.Lock()
	if s.bootstrapWatcherRunning { // idempotency
		s.bootstrapWatcherMu.Unlock()
		return
	}
	s.bootstrapWatcherRunning = true
	s.bootstrapWatcherMu.Unlock()

	// Goroutine polls runed until it transition out of STATUS_LOADING,
	// then call State.Retrigger() so boot loop resumes without user interaction
	go s.runBootstrapWatcher()
}

func (s *LifecycleService) runBootstrapWatcher() {
	defer func() {
		s.bootstrapWatcherMu.Lock()
		s.bootstrapWatcherRunning = false
		s.bootstrapWatcherMu.Unlock()
	}()

	ticker := time.NewTicker(bootstrapWatchInterval)
	defer ticker.Stop()

	deadline := time.Now().Add(bootstrapWatcherDeadline)
	consecutiveErrors := 0

	for range ticker.C {
		if time.Now().After(deadline) {
			slog.Warn("bootstrap watcher: total deadline exceeded; operator must re-trigger /rune:activate",
				"deadline", bootstrapWatcherDeadline)
			return
		}

		e := s.Embedder()
		if e == nil { // Embedder is removed
			return
		}

		probeCtx, cancel := context.WithTimeout(context.Background(), bootstrapWatcherHealthTimeout)
		h, err := e.Health(probeCtx)
		cancel()

		if err != nil {
			consecutiveErrors++
			if consecutiveErrors >= bootstrapWatcherMaxErrors {
				slog.Warn("bootstrap watcher: persistent health probe failure; giving up",
					"consecutive_errors", consecutiveErrors, "last_err", err)
				return
			}
			continue
		}
		consecutiveErrors = 0

		switch h.Status {
		case "LOADING": // still bootstrapping
			continue
		case "OK", "IDLE": // bootstrap finished (IDLE = up but idle-suspended, still ready)
			if s.State != nil {
				s.State.Retrigger()
			}
			return
		default:
			// DEGRADED / SHUTTING_DOWN / UNSPECIFIED status need user interaction
			return
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 7. rune_reload_pipelines
// ─────────────────────────────────────────────────────────────────────────────

// ReloadPipelinesResult.
type ReloadPipelinesResult struct {
	OK                   bool   `json:"ok"`
	State                string `json:"state"`
	ScribeInitialized    bool   `json:"scribe_initialized"`
	RetrieverInitialized bool   `json:"retriever_initialized"`
	// LastBootError mirrors VaultInfo.LastBootError so callers (agent, UI)
	// can fast-fail on this single response — no follow-up diagnostics call
	// needed for the common case of "reload finished, boot failed, here's
	// why". Populated only when state != "active" AND a classified error
	// is available; nil otherwise.
	LastBootError  *domain.BootError `json:"last_boot_error,omitempty"`
	Errors         []string          `json:"errors,omitempty"`
	EnvectorWarmup *WarmupInfo       `json:"envector_warmup,omitempty"`
}

// WarmupInfo — GetIndexList probe (60s timeout).
type WarmupInfo struct {
	OK        bool     `json:"ok"`
	LatencyMs *float64 `json:"latency_ms,omitempty"`
	Error     *string  `json:"error,omitempty"`
}

// WarmupTimeout — Python WARMUP_TIMEOUT (server.py:L1059). 60s.
const WarmupTimeout = 60 * time.Second

// ReloadPipelines — re-trigger the boot loop from Dormant + warmup envector.
//
// On a terminal Dormant state (boot loop's goroutine has exited), call
// Manager.Retrigger to spawn a fresh RunBootLoop bound to the same ctx +
// Deps; main.go wires the spawn callback at startup. Manager.Retrigger
// is a silent no-op when state is Starting / WaitingForVault / Active —
// safe to call unconditionally if the trigger surface ever widens.
//
// /rune:activate from a freshly-spawned MCP server (no ~/.rune/config.json
// at boot, then user ran /rune:configure) reaches Active via this path.
// No process restart is required.
func (s *LifecycleService) ReloadPipelines(ctx context.Context) (*ReloadPipelinesResult, error) {
	// Always re-trigger so config changes such as new vault endpoint and rotated token are picked
	// without restarting MCP
	s.State.Retrigger()
	s.waitForBootProgress(ctx, 5*time.Second)

	result := &ReloadPipelinesResult{
		OK:    true,
		State: s.State.Current().String(),
	}

	if s.State.Current() == lifecycle.StateActive {
		result.ScribeInitialized = true
		result.RetrieverInitialized = true
	} else {
		// Boot did not reach active within the 5s wait window. Surface the
		// most recent classified boot error so the caller can fast-fail
		// without needing a separate diagnostics call. May still be nil
		// (e.g., boot loop is genuinely in-flight and hasn't recorded an
		// error yet) — in that case the agent should follow up with
		// diagnostics per the Fast-Fail Rule in SKILL.md.
		if be := s.State.LastBootError(); be != nil {
			result.LastBootError = be
		}
	}

	if s.Vault != nil {
		warmup := s.warmupEnvector(ctx, WarmupTimeout)
		result.EnvectorWarmup = warmup
	}

	return result, nil
}

// waitForBootProgress polls Manager.Current() until either Active or a
// terminal Dormant is reached, or the deadline elapses. The caller has
// already triggered a fresh boot loop; this just gives it room to make
// progress before we snapshot state for the response. WaitingForVault
// (transient retry) is treated as still-in-progress because the loop is
// actively retrying with backoff and may yet reach Active.
//
// Initial 150ms grace period: Retrigger schedules a `go RunBootLoop(...)`
// — there is a brief window between the call returning and the spawned
// goroutine reaching its first `m.SetState(StateStarting)`. Without the
// grace, the very first state read can still see the prior Dormant
// snapshot and exit immediately.
func (s *LifecycleService) waitForBootProgress(ctx context.Context, timeout time.Duration) {
	deadline := time.Now().Add(timeout)

	select {
	case <-ctx.Done():
		return
	case <-time.After(150 * time.Millisecond):
	}

	for time.Now().Before(deadline) {
		switch s.State.Current() {
		case lifecycle.StateActive, lifecycle.StateDormant:
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// warmupEnvector — GetIndexList under 60s timeout.
func (s *LifecycleService) warmupEnvector(ctx context.Context, timeout time.Duration) *WarmupInfo {
	ctx2, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	t0 := time.Now()
	_, err := s.Vault.HealthCheck(ctx2)
	elapsed := float64(time.Since(t0).Milliseconds())

	if err != nil {
		errStr := err.Error()
		return &WarmupInfo{OK: false, LatencyMs: &elapsed, Error: &errStr}
	}

	return &WarmupInfo{OK: true, LatencyMs: &elapsed}
}

// ─────────────────────────────────────────────────────────────────────────────
// 8. rune_permissions — read-only RBAC query (requirement 4, plan §6-D8).
//
// Returns the caller's own memberships and the depth-annotated group tree they
// can recall from (effective role, inheritance resolved by the vault). The vault
// owns policy; this tool is a read-only projection for the caller's token. An
// organization admin may pass include_member_roles for the org-wide (user,
// group, role) listing — a non-admin request for it is denied by the vault
// (PermissionDenied, plan §D11).
// ─────────────────────────────────────────────────────────────────────────────

// PermissionsArgs — input for the `permissions` tool.
type PermissionsArgs struct {
	RootGroup          string `json:"root_group,omitempty" jsonschema:"Optional group id or name; restrict the returned tree to that group's subtree. Empty = the full tree you can reach."`
	IncludeMemberRoles bool   `json:"include_member_roles,omitempty" jsonschema:"Organization admin only. When true, include the org-wide per-user (group, role) listing. A non-admin request is denied."`
}

// PermissionsResult — the caller's RBAC view. Field shape mirrors the backend
// API contract (rbac-backend-api-draft.md §1 GetPermissions).
type PermissionsResult struct {
	OK          bool                   `json:"ok"`
	Me          string                 `json:"me"`
	Memberships []PermissionMembership `json:"memberships"`
	Tree        []PermissionTreeNode   `json:"tree"`
	MemberRoles []PermissionMemberRole `json:"member_roles,omitempty"`
}

// PermissionMembership — one direct (group, role) binding the caller holds.
type PermissionMembership struct {
	GroupID   string `json:"group_id"`
	GroupName string `json:"group_name"`
	Role      string `json:"role"`
}

// PermissionTreeNode — one reachable group, depth-annotated.
type PermissionTreeNode struct {
	GroupID       string `json:"group_id"`
	Name          string `json:"name"`
	ParentID      string `json:"parent_id,omitempty"`
	Depth         int    `json:"depth"`
	EffectiveRole string `json:"effective_role"`
}

// PermissionMemberRole — one org-wide (user, group, role) row (admin listing).
type PermissionMemberRole struct {
	User      string `json:"user"`
	GroupID   string `json:"group_id"`
	GroupName string `json:"group_name"`
	Role      string `json:"role"`
}

// Permissions queries the vault for the caller's RBAC view. Read-only: it
// bypasses the pipeline state gate (like vault_status), but does require a wired
// vault — in standard (no-Vault) mode there is no policy to report.
func (s *LifecycleService) Permissions(ctx context.Context, args PermissionsArgs) (*PermissionsResult, error) {
	if s.Vault == nil {
		return nil, &domain.RuneError{
			Code:    domain.CodeVaultConnection,
			Message: "permissions requires a configured Vault (secure mode); none is wired. Run /rune:configure then /rune:activate.",
		}
	}

	perms, err := s.Vault.GetPermissions(ctx, args.RootGroup, args.IncludeMemberRoles)
	if err != nil {
		return nil, err
	}

	res := &PermissionsResult{OK: true, Me: perms.Me}
	for _, m := range perms.Memberships {
		res.Memberships = append(res.Memberships, PermissionMembership{
			GroupID:   m.GroupID,
			GroupName: m.GroupName,
			Role:      m.Role,
		})
	}
	for _, n := range perms.Tree {
		res.Tree = append(res.Tree, PermissionTreeNode{
			GroupID:       n.GroupID,
			Name:          n.Name,
			ParentID:      n.ParentID,
			Depth:         n.Depth,
			EffectiveRole: n.EffectiveRole,
		})
	}
	for _, mr := range perms.MemberRoles {
		res.MemberRoles = append(res.MemberRoles, PermissionMemberRole{
			User:      mr.User,
			GroupID:   mr.GroupID,
			GroupName: mr.GroupName,
			Role:      mr.Role,
		})
	}
	return res, nil
}
