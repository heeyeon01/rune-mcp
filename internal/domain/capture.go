package domain

// MCP Capture tool I/O.
// Spec: docs/v04/spec/types.md §4.1.
// Python: mcp/server/server.py:L698-806 (entry) · L1208-1407 (_capture_single).
// Flow: docs/v04/spec/flows/capture.md (7-phase).

// CaptureRequest — §4.1.
// Extracted is the flat wire format split into Detection + ExtractionResult
// internally (see types.md §3a.4 mapping).
type CaptureRequest struct {
	Text        string         `json:"text" jsonschema:"Raw source text the decision was extracted from."`
	Source      string         `json:"source" jsonschema:"Source identifier for the capture (e.g. channel, doc, or session name)."`
	User        string         `json:"user,omitempty" jsonschema:"Optional author of the captured context."`
	Channel     string         `json:"channel,omitempty" jsonschema:"Optional channel the context originated from."`
	ShareGroups []string       `json:"share_groups,omitempty" jsonschema:"Optional. Which of YOUR DIRECT groups (you must be a write-or-higher member) to share this capture with — only members whose recall scope includes one of these groups will find it. Empty = all of your direct write-capable groups. Inherited (descendant) groups are not valid: a superior group's memory must never leak downward. The Vault resolves/validates these and rejects any group you are not a direct write member of."`
	Extracted   map[string]any `json:"extracted" jsonschema:"FLAT extraction object (the agent-delegated extraction result). Fields: {title, decision, problem, rationale, domain?, status?, tags?[]}. Do not nest these under another key; the object itself is the extraction."` // see types.md §3a.4
}

// CaptureResponse — §4.1.
// Note: no `similar_to` field (D10 Archived — Python parity). Duplicate record
// info flows via Novelty.Related[] (see NoveltyInfo in query.go).
type CaptureResponse struct {
	OK       bool   `json:"ok"`
	Captured bool   `json:"captured"`
	RecordID string `json:"record_id,omitempty"`
	Title    string `json:"title,omitempty"`
	Domain   Domain `json:"domain,omitempty"`

	Reason  string       `json:"reason,omitempty"`
	Novelty *NoveltyInfo `json:"novelty,omitempty"`

	Error string `json:"error,omitempty"`
}

// RawEvent — input to record_builder.BuildPhases.
// Python: agents/scribe/record_builder.py RawEvent (dataclass at top of file).
type RawEvent struct {
	Text    string
	Source  string
	User    string
	Channel string
	// TS, metadata fields — see Python original
}
