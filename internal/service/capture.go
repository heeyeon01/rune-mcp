// Package service holds the orchestration layer — multi-phase flows that
// coordinate adapters + policy. MCP tool handlers (internal/mcp/tools.go)
// delegate to these services; business logic lives here, not in handlers.
//
// Spec:
//
//	docs/v04/spec/flows/capture.md (7-phase)
//	docs/v04/spec/flows/recall.md (7-phase)
//	docs/v04/spec/flows/lifecycle.md (6 tools)
package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/CryptoLabInc/rune-mcp/internal/adapters/embedder"
	"github.com/CryptoLabInc/rune-mcp/internal/adapters/logio"
	"github.com/CryptoLabInc/rune-mcp/internal/adapters/seal"
	"github.com/CryptoLabInc/rune-mcp/internal/adapters/vault"
	"github.com/CryptoLabInc/rune-mcp/internal/domain"
	"github.com/CryptoLabInc/rune-mcp/internal/lifecycle"
	"github.com/CryptoLabInc/rune-mcp/internal/policy"
)

// CaptureService orchestrates the 7-phase capture flow.
// Python: mcp/server/server.py:L1208-1407 _capture_single + L810-896 tool_batch_capture.
type CaptureService struct {
	Vault      vault.Client
	Embedder   embedder.Client
	Encryptor  Encryptor
	CaptureLog *logio.CaptureLog
	State      *lifecycle.Manager

	// Injected from Vault bundle at boot.
	IndexName string
	AgentID   string // for the seal envelope's "a" field
	AgentDEK  []byte // metadata seal key (agent_dek from manifest)

	Now func() time.Time // injectable clock (default: time.Now)
}

// Encryptor is the client-side FHE encrypt surface (runespacecrypto). Kept as
// an interface so capture tests need no cgo. EncryptFlat/EncryptClustered take
// an l2-normalized vector and return the tier ciphertexts.
type Encryptor interface {
	EncryptFlat(vec []float32) ([]byte, error)
	EncryptClustered(vec []float32) ([]byte, error)
}

// NewCaptureService constructs with default clock.
func NewCaptureService() *CaptureService {
	return &CaptureService{Now: time.Now}
}

// EncryptSealInsert embeds+routes text, encrypts the vector (EncKey), seals
// metadataJSON (agent_dek), and forwards the ciphertext item to the vault
// under a fresh idempotent id. Shared by the capture flow and DeleteCapture's
// tombstone re-insert so there is one client-side crypto path.
//
// Centroid desync self-heals here (§9.2): buildInsertItem covers C4 (runed has
// no set), and a WRONG_CENTROID_VERSION rejection covers C3 — resync, rebuild
// the item under the new set (fresh cluster_id + version, same id), and retry
// exactly once.
func (s *CaptureService) EncryptSealInsert(ctx context.Context, text, metadataJSON string, shareGroups []string) (string, error) {
	if s.Encryptor == nil {
		return "", fmt.Errorf("capture: encryptor not initialized")
	}
	id := uuid.NewString()
	item, err := s.buildInsertItem(ctx, id, text, metadataJSON, shareGroups)
	if err != nil {
		return "", err
	}
	insertedID, err := insertWithRecovery(ctx, s.State, s.Vault, item)
	if err == nil || !isWrongCentroidVersion(err) {
		return insertedID, err
	}
	// C3: the engine replaced its centroid set after we routed. The vault has
	// already dropped its stale cache, so a resync now yields the new set.
	slog.Warn("capture: insert rejected for stale centroid version; resyncing and retrying once", "id", id)
	if rerr := s.resyncCentroids(ctx); rerr != nil {
		return "", fmt.Errorf("insert rejected (%w) and centroid resync failed: %w", err, rerr)
	}
	item, err = s.buildInsertItem(ctx, id, text, metadataJSON, shareGroups)
	if err != nil {
		return "", err
	}
	return insertWithRecovery(ctx, s.State, s.Vault, item)
}

// buildInsertItem runs the client-side crypto pipeline — embed+route (runed) →
// encrypt (EncKey) → seal (agent_dek) — and assembles the vault item under the
// given idempotent id. On runed FAILED_PRECONDITION (no centroid set — C4,
// e.g. the best-effort boot relay failed or runed restarted with a cold cache)
// it pushes the set once and retries the route.
func (s *CaptureService) buildInsertItem(ctx context.Context, id, text, metadataJSON string, shareGroups []string) (vault.InsertItem, error) {
	routed, err := s.Embedder.EmbedRoute(ctx, text)
	if isNoCentroids(err) {
		slog.Warn("capture: runed has no centroid set; resyncing and retrying once")
		if rerr := s.resyncCentroids(ctx); rerr != nil {
			return vault.InsertItem{}, fmt.Errorf("embed route: %w (centroid resync failed: %w)", err, rerr)
		}
		routed, err = s.Embedder.EmbedRoute(ctx, text)
	}
	if err != nil {
		return vault.InsertItem{}, fmt.Errorf("embed route: %w", err)
	}
	rmp, err := s.Encryptor.EncryptFlat(routed.Vector)
	if err != nil {
		return vault.InsertItem{}, fmt.Errorf("encrypt flat: %w", err)
	}
	mm, err := s.Encryptor.EncryptClustered(routed.Vector)
	if err != nil {
		return vault.InsertItem{}, fmt.Errorf("encrypt clustered: %w", err)
	}
	sealed, err := seal.Seal(s.AgentDEK, s.AgentID, []byte(metadataJSON))
	if err != nil {
		return vault.InsertItem{}, fmt.Errorf("seal metadata: %w", err)
	}
	return vault.InsertItem{
		ID:                 id,
		RMPItem:            rmp,
		MMItem:             mm,
		ClusterID:          routed.ClusterID,
		CentroidSetVersion: routed.CentroidSetVersion,
		SealedMetadata:     sealed,
		ShareGroups:        shareGroups,
	}, nil
}

// Handle — single capture. Called by internal/mcp/tools.go ToolCapture.
// Python: server.py:L1208-1407 _capture_single.
//
// Flow (per spec/flows/capture.md):
//
//	Phase 1 (in handler): state gate → PIPELINE_NOT_READY if not active
//	Phase 2: validate text + parse extracted (Detection + ExtractionResult split)
//	Phase 3: embedder.EmbedSingle(text_to_embed) — reusable_insight > payload.text
//	Phase 4: Vault.Score → Vault.DecryptScores(top_k=3) → novelty classify
//	         near_duplicate (≥0.95) → return {captured:false, novelty{class, score, related}}
//	         failures non-fatal (server.py:L1370-1372 logger.warning)
//	Phase 5: policy.BuildPhases → embedder.EmbedBatch(texts) → seal.Seal × N
//	Phase 6: Vault.Insert (atomic batch, D17)
//	Phase 7: capture_log append (degrade per D19) → respond
func (s *CaptureService) Handle(ctx context.Context, req *domain.CaptureRequest) (*domain.CaptureResponse, error) {
	// Phase 2
	detection, extraction, err := domain.ParseExtractionFromAgent(req.Extracted)
	if err != nil {
		return nil, err
	}
	if extraction == nil {
		return nil, &domain.RuneError{Code: domain.CodeInvalidInput, Message: "extraction is nil after parse"}
	}

	// D14 (lifecycle.md §3): an item with neither raw text nor any embeddable
	// extraction content must be rejected, never captured. Single capture always
	// carries a validated req.Text, so this only fires for agent-supplied
	// extractions with no usable fields — an empty batch item, or the
	// {text, extracted} wrapper anti-pattern whose fields nest under "extracted"
	// where ParseExtractionFromAgent's top-level lookup can't see them. Enforcing
	// D14 here (the shared path) keeps single capture and batch in agreement and
	// makes the contentless-record corpus poisoning (identical boilerplate →
	// ~1.0 self-similarity → cascading false near_duplicate) structurally
	// impossible: a contentless item never reaches the embedder.
	if strings.TrimSpace(req.Text) == "" && !extraction.HasContent() {
		return nil, &domain.RuneError{
			Code:    domain.CodeInvalidInput,
			Message: "item has no usable extraction content: provide a top-level decision field (\"reusable_insight\", \"title\", \"rationale\", or \"problem\"), or the {group_title, phases} multi-phase shape. A bare \"group_title\" without \"phases\" is not enough — it is only read in the multi-phase shape. Each batch item is a flat extracted object, not a {text, extracted} wrapper.",
		}
	}

	// Phase 5: build policy
	rawEvent := &domain.RawEvent{
		Text:    req.Text,
		Source:  req.Source,
		User:    req.User,
		Channel: req.Channel,
	}
	if rawEvent.User == "" {
		rawEvent.User = "unknown"
	}
	if rawEvent.Channel == "" {
		rawEvent.Channel = "claude_session"
	}

	records, err := policy.BuildPhases(rawEvent, detection, extraction, s.Now())
	if err != nil {
		return nil, fmt.Errorf("build phases: %w", err)
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("build phases returned 0 records")
	}

	// Phase 3, 4
	// TODO: reconsider per-record handling vs records[0] representative
	embeddingText := pickEmbedText(&records[0])
	var noveltyInfo *domain.NoveltyInfo

	noveltyInfo, earlyResp, _ := s.runNoveltyCheck(ctx, embeddingText)
	if earlyResp != nil {
		return earlyResp, nil // near_duplicate
	}
	if noveltyInfo == nil {
		noveltyInfo = &domain.NoveltyInfo{Score: 1.0, Class: "novel"}
	}

	// Phase 5: embed. Metadata is the plaintext record JSON; the vault seals it.
	// Phase 5-6: embed (with IVF routing), encrypt each record locally
	// (EncKey), seal its metadata (agent_dek), and forward the ciphertext to
	// the vault. Plaintext vectors and metadata never leave this process.
	if s.Encryptor == nil {
		return nil, fmt.Errorf("capture: encryptor not initialized")
	}
	if s.Encryptor == nil {
		return nil, fmt.Errorf("capture: encryptor not initialized")
	}
	for i := range records {
		body, err := json.Marshal(records[i])
		if err != nil {
			return nil, fmt.Errorf("marshal record %d: %w", i, err)
		}
		if _, err := s.EncryptSealInsert(ctx, pickEmbedText(&records[i]), string(body), req.ShareGroups); err != nil {
			return nil, fmt.Errorf("vault insert %d: %w", i, err)
		}
	}

	// Phase 7
	first := records[0]
	if s.CaptureLog != nil {
		var noveltyScore *float64
		var noveltyClass string
		if noveltyInfo != nil {
			nsc := noveltyInfo.Score
			noveltyScore = &nsc
			noveltyClass = string(noveltyInfo.Class)
		}
		_ = s.CaptureLog.Append(domain.CaptureLogEntry{
			TS:           s.Now().UTC().Format(time.RFC3339),
			Action:       "captured",
			ID:           first.ID,
			Title:        first.Title,
			Domain:       string(first.Domain),
			Mode:         "agent-delegated",
			NoveltyClass: noveltyClass,
			NoveltyScore: noveltyScore,
		})
	}

	resp := &domain.CaptureResponse{
		OK:       true,
		Captured: true,
		RecordID: first.ID,
		Title:    first.Title,
		Domain:   first.Domain,
		Novelty:  noveltyInfo,
	}

	return resp, nil
}

// Batch — call Handle sequentially N times
// Per-item independent processing; one item's failure does not abort others.
// Each item classified: captured / skipped / near_duplicate / error.
//
// Future optimizations:
//   - Phase 3/5 embed: runed.EmbedBatch (N to 1 call)
//   - Phase 4 score: Vault native multi-vector query
//   - Phase 6 insert: Vault.Insert is already batch-native (N to 1 call)
func (s *CaptureService) Batch(ctx context.Context, args BatchCaptureArgs) (*BatchCaptureResult, error) {
	var rawItems []map[string]any
	if err := json.Unmarshal([]byte(args.Items), &rawItems); err != nil {
		return nil, &domain.RuneError{Code: domain.CodeInvalidInput, Message: "invalid items JSON array"}
	}

	result := &BatchCaptureResult{
		OK:      true,
		Total:   len(rawItems),
		Results: make([]BatchItemResult, 0, len(rawItems)),
	}

	for i, item := range rawItems {
		// A batch item carries no per-item raw text — the embeddable content lives
		// entirely in the flat extracted object. Hand it straight to Handle, whose
		// shared D14 guard rejects a contentless item (empty Text + no extraction
		// content) as an error. This keeps batch and single capture on one code
		// path, so the gate cannot drift from what ParseExtractionFromAgent/
		// RenderPayloadText actually embed (e.g. {group_title, phases} or a
		// phase-only item with no top-level title is accepted, exactly as in
		// single capture; the {text, extracted} wrapper is rejected).
		req := &domain.CaptureRequest{
			Text:        "",
			Source:      args.Source,
			ShareGroups: args.ShareGroups,
			Extracted:   item,
		}
		if args.User != nil {
			req.User = *args.User
		}
		if args.Channel != nil {
			req.Channel = *args.Channel
		}

		resp, err := s.Handle(ctx, req)
		bir := BatchItemResult{Index: i}

		if err != nil {
			errMsg := err.Error()
			bir.Status = "error"
			bir.Error = &errMsg
			result.Errors++
		} else if resp.Captured {
			bir.Status = "captured"
			bir.Title = resp.Title
			if resp.Novelty != nil {
				bir.Novelty = string(resp.Novelty.Class)
			}
			result.Captured++
		} else {
			bir.Status = "skipped"
			if resp.Novelty != nil && resp.Novelty.Class == domain.NoveltyClassNearDuplicate {
				bir.Status = "near_duplicate"
			}
			result.Skipped++
		}
		result.Results = append(result.Results, bir)
	}

	return result, nil
}

// runNoveltyCheck — Phase 4 helper. Returns novelty info + nil if proceed,
// or a pre-built response if near_duplicate (caller short-circuits).
func (s *CaptureService) runNoveltyCheck(ctx context.Context, embeddingText string) (*domain.NoveltyInfo, *domain.CaptureResponse, error) {
	if s.Embedder == nil || s.Vault == nil {
		return &domain.NoveltyInfo{Score: 1.0, Class: "novel"}, nil, nil
	}

	vec, err := s.Embedder.EmbedSingle(ctx, embeddingText)
	if err != nil {
		slog.Warn("novelty check: embed failed (non-fatal)", "err", err)
		return &domain.NoveltyInfo{Score: 1.0, Class: "novel"}, nil, nil
	}

	hits, err := searchWithRecovery(ctx, s.State, s.Vault, vec, 3)
	if err != nil || len(hits) == 0 {
		slog.Warn("novelty check: search failed or empty (non-fatal)", "err", err)
		return &domain.NoveltyInfo{Score: 1.0, Class: "novel"}, nil, nil
	}

	maxSim := 0.0
	for _, h := range hits {
		if h.Score > maxSim {
			maxSim = h.Score
		}
	}

	class, score := policy.ClassifyNovelty(maxSim, policy.DefaultNoveltyThresholds)
	noveltyInfo := &domain.NoveltyInfo{
		Score:   score,
		Class:   class,
		Related: buildRelatedTop3(hits),
	}

	if class == domain.NoveltyClassNearDuplicate {
		return noveltyInfo, &domain.CaptureResponse{
			OK:       true,
			Captured: false,
			Reason:   "Near-duplicate - virtually identical insight already stored",
			Novelty:  noveltyInfo,
		}, nil
	}

	return noveltyInfo, nil, nil
}

func pickEmbedText(r *domain.DecisionRecord) string {
	if r.ReusableInsight != "" {
		return r.ReusableInsight
	}
	return r.Payload.Text // fallback
}

func buildRelatedTop3(hits []vault.Hit) []domain.RelatedRecord {
	n := len(hits)
	if n > 3 {
		n = 3
	}

	records := make([]domain.RelatedRecord, n)
	for i := 0; i < n; i++ {
		records[i] = domain.RelatedRecord{
			ID:         hits[i].ID,
			Similarity: math.Round(hits[i].Score*1000) / 1000,
		}
	}

	return records
}

// ─────────────────────────────────────────────────────────────────────────────
// Batch types — lifecycle.md §3
// ─────────────────────────────────────────────────────────────────────────────

// BatchCaptureArgs — Python: server.py:L810 tool_batch_capture args.
//
// The jsonschema tags below are surfaced verbatim in the tool's inputSchema
// (go-sdk reads the `jsonschema` struct tag as the property description). They
// exist to steer the model on first call: `items` is a string-typed param, so
// the schema cannot otherwise express the per-element shape, and the single
// `capture` tool's {text, source, extracted} layout invites a wrong-by-analogy
// guess (a [{text, extracted}, ...] wrapper). Keep them in sync with the
// runtime validation error in Batch (capture.go).
type BatchCaptureArgs struct {
	Items       string   `json:"items" jsonschema:"JSON array string. Each element is a FLAT extracted object, NOT a {text, extracted} wrapper. Shape per item: {title, decision, problem, rationale, domain?, status?, tags?[]} or the multi-phase shape {group_title, phases[]}. An item must carry at least one of title/decision/problem/rationale (a bare group_title is read only inside the multi-phase shape)."`
	Source      string   `json:"source,omitempty" jsonschema:"Batch-level source identifier; applied to every item. Per-item source is not read."`
	User        *string  `json:"user,omitempty" jsonschema:"Batch-level user; applied to every item."`
	Channel     *string  `json:"channel,omitempty" jsonschema:"Batch-level channel; applied to every item."`
	ShareGroups []string `json:"share_groups,omitempty" jsonschema:"Optional. Batch-level; applied to every item (per-item share_groups is not read). Which of YOUR DIRECT write-capable groups to share these captures with — only members whose recall scope includes one of these groups will find them. Empty = all of your direct write groups. Inherited (descendant) groups are not valid: a superior group's memory must never leak downward. The Vault resolves/validates and rejects any group you are not a direct write member of."`
}

// BatchCaptureResult — aggregated response.
type BatchCaptureResult struct {
	OK       bool              `json:"ok"`
	Total    int               `json:"total"`
	Results  []BatchItemResult `json:"results"`
	Captured int               `json:"captured"`
	Skipped  int               `json:"skipped"`
	Errors   int               `json:"errors"`
}

// BatchItemResult — per-item outcome.
type BatchItemResult struct {
	Index   int     `json:"index"`
	Title   string  `json:"title"`
	Status  string  `json:"status"` // "captured" | "skipped" | "near_duplicate" | "error"
	Novelty string  `json:"novelty,omitempty"`
	Error   *string `json:"error,omitempty"`
}
