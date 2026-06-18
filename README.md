# rune-mcp

A session-local MCP server for Rune's encrypted organizational memory. It is a Go
port of the agent-delegated path of Python rune v0.3.x.

An agent host (Claude Code, Codex, etc.) spawns one instance per session over stdio.
It takes capture/recall requests and runs embedding → AES encryption → enVector
storage (or FHE search), delegating key management and decryption to Vault over gRPC.

## Build / Run

```sh
go build ./...
go build -o rune-mcp ./cmd/rune-mcp   # build the binary
./rune-mcp --version
```

As an MCP server it is normally started over stdio by the agent host rather than run directly.

On macOS, run tests with `TMPDIR=/tmp go test ./...` — the default temp dir produces unix socket paths that exceed the 104-byte limit, failing the spawn tests with `bind: invalid argument`.

## Layout

```
cmd/rune-mcp        entrypoint (stdio + boot loop)
internal/mcp        MCP SDK wiring · 10 tool handlers · state gate
internal/service    capture / recall / lifecycle orchestration
internal/policy     pure logic (novelty · rerank · query · PII redaction)
internal/adapters   external I/O (vault gRPC · envector SDK · embedder · config)
internal/domain     core types (leaf — no imports from other internal packages)
internal/lifecycle  state machine · boot retry · graceful shutdown
internal/obs        slog + request_id + sensitive-data redaction
```

Dependency direction: `mcp → service → {policy, adapters, lifecycle, obs} → domain`.
Reverse imports are forbidden.

State machine: `starting → waiting_for_vault → active ↔ dormant`. Before boot
completes, write tools are rejected with `PIPELINE_NOT_READY` and only read-only
tools run in a degraded mode.

## MCP tools (10)

`activate` · `capture` · `batch_capture` · `recall` · `capture_history` ·
`delete_capture` · `configure` · `diagnostics` · `vault_status` · `reload_pipelines`

## Docs

- [docs/runed/](docs/runed/) — Go implementation reference (architecture, communication, capture/recall flows, MCP/CLI layer)
- [docs/migration/](docs/migration/) — Python → Go migration analysis
- [internal/](internal/) — per-package details (see the directory README)

## Dependencies

- `runed` — shared daemon runtime
- enVector Go SDK — vector storage/search (developed separately)
- Vault gRPC — key management · FHE decryption
- MCP Go SDK — `modelcontextprotocol/go-sdk`
