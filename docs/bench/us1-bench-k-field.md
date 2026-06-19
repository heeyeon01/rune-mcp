# US-1 bench 라인 `k=` 필드 — vault_topk top-K 계측 (방출 측, 2026-06-19)

> **이 문서의 위치**: rune-mcp는 US-1 벤치의 **방출(emit) 측**이다. 이 문서는 bench 라인에
> 새로 추가한 `k=` 필드가 *무엇을·왜·어떻게* 방출되는지를 적는다. 같은 필드를 **소비(consume)
> 측**에서 어떻게 파싱·집계하는지는 rune-bench
> `internal/operational/docs/plans/2026-06-19-step3-bench-k-field.ko.md`를 본다.
>
> **선행 문서**: `us1-bench-instrumentation-dataflow.md`(bench 계측 전체 구조),
> `us1-per-call-latency-explainer.md`(왜 호출 단위로 재는가),
> `us1-report-segment-coverage.md` §2.2(seg/op → 리포트 구간 매핑).

---

## 0. 한 줄 요약

vault `DecryptScores`(= 리포트 구간 `vault_topk`)의 **top-K**를 bench 라인에 `k=`로 실어
보낸다. recall 한 번이 부르는 여러 DecryptScores 호출은 **top-K가 제각각**(본검색 = 요청 topk,
보강검색 = 그룹 크기)이라, k 없이는 vault_topk latency가 *이질적 모집단의 평균*으로 뭉뚱그려진다.
`k=`가 있으면 소비 측이 본검색/보강을 갈라 이봉(bimodal)을 펼칠 수 있다.

---

## 1. 왜 필요했나 — vault_topk는 k가 섞인 분포

`recall` 한 번이 내부에서 `searchSingle`을 여러 번 돈다(`internal/service/recall.go`).
각 `searchSingle`은 `score`(envector inner_product) 직후 `DecryptScores`(vault FHE 복호)를
한 번씩 부른다. 그런데 그 **top-K가 호출마다 다르다**:

```
searchSingle #1~#4  본검색·폴백   → DecryptScores(topK = 요청 topk, 예: 10)
searchSingle #5~#6  R9 누락그룹 보강 → DecryptScores(topK = g.total, 그룹 크기, recall.go:515)
```

FHE 복호 비용은 k에 비례하므로, 큰 그룹(`g.total=20`)을 보강하면 그 호출만 latency가
튄다. bench 라인에 k가 없으면 vault_topk 셀은 "k=10 무거운 복호"와 "k=작은/큰 보강"이
한 통에 섞여, p95가 *진짜 꼬리*인지 *k 큰 무리*인지 구분되지 않는다. (배경 상세:
`us1-per-call-latency-explainer.md`, rune-bench explainer §6~§7.)

---

## 2. 무엇을 방출하나 — bench 라인 계약 변경

### 2.1 라인 포맷 (additive)

`k=`는 **추가 필드**다. 기존 6필드는 그대로, vault 복호 라인에만 `k=`가 **꼬리에** 붙는다:

```
# 기존 (envector score 등 — 변화 없음)
msg=bench seg=envector op=/ES2E.ES2EService/inner_product dur_us=12345 ok=true n=100000 req=ab12

# 신규 (vault DecryptScores — k= 추가)
msg=bench seg=vault op=/rune.vault.v1.VaultService/DecryptScores dur_us=88000 ok=true n=100000 req=ab12 k=20
```

| 필드 | 어느 라인에 | 의미 |
|---|---|---|
| `k=` | **vault DecryptScores 라인에만** | 그 호출의 top-K. 다른 모든 seg/op에는 없음 |

> **하위호환**: 추가-온리라 기존 소비자를 안 깬다. 소비 측은 모르는 키를 무시하거나(구버전),
> `k=` 없으면 `-1`("해당 없음")로 채운다(신버전, `n=-1`과 같은 센티넬 컨벤션).

### 2.2 계약 테스트 (Contract 2b)

`internal/bench/bench_test.go`:
- `TestObserve_EmitsKWhenTagged` — ctx가 `WithK`로 태깅되면 라인에 `k=20` 방출.
- `TestObserve_NoKWhenUntagged` — 태깅 안 된 ctx(모든 비-vault seg)는 `k=` 미방출.

---

## 3. 어떻게 방출하나 — context-carried k (설계 결정)

### 3.1 장애물: 인터셉터는 k를 이름으로 못 본다

vault `DecryptScores`는 **unary RPC**라 제네릭 `UnaryInterceptor`(`bench.go`)가 자동으로
잰다(`boot.go:442` `withBench("vault", …)`). 그런데 인터셉터가 받는 건
`ctx, method, req, reply`뿐이고 `topK`는 `req`(= `*vaultpb.DecryptScoresRequest`) **안에
묻혀 있다.** 꺼내려면 인터셉터에서 proto를 type-assert해야 하는데 —

```go
// ❌ 안 하는 이유
if r, ok := req.(*vaultpb.DecryptScoresRequest); ok { k = int(r.TopK) }
```

이러면 bench가 **leaf 모듈**(obs·grpc만 import하는 일방향 의존, `bench.go` 패키지 주석)
원칙을 깬다. vaultpb를 끌어들이고 제네릭 인터셉터에 vault 특례를 박는 셈이라 격리가 무너진다.

### 3.2 택한 길: 어댑터가 ctx에 싣고, Observe가 읽는다

`topK`를 **맨손에 쥔** 곳은 vault 어댑터다(`DecryptScores(ctx, blob, topK int)`). 거기서
ctx에 k를 태깅하고, 인터셉터의 `Observe`는 "ctx에 int 있으면 붙인다"만 안다 — proto를 모름.

```go
// internal/bench/bench.go
func WithK(ctx context.Context, k int) context.Context   // ctx에 top-K 태깅
func Observe(...) { ... if k, ok := kFromContext(ctx); ok { attrs = append(attrs, "k", k) } }

// internal/adapters/vault/client.go  DecryptScores (단일 길목)
if bench.Enabled() {                 // prod(off)는 alloc 0
    ctx = bench.WithK(ctx, topK)
}
resp, err := c.stub.DecryptScores(ctx, &vaultpb.DecryptScoresRequest{ TopK: int32(topK), ... })
```

**왜 어댑터 한 곳인가**: `DecryptScores`는 recall 본검색·보강·capture novelty의 **모든
복호가 지나는 단일 길목**이다. 여기 한 줄이면 서비스 호출 지점(`recall.go`/`capture.go`)은
하나도 안 건드리고 전부 커버된다.

### 3.3 배선 (end-to-end)

```
recall.searchSingle
  └─ vault.DecryptScores(ctx, blob, topK)
       └─ ctx = bench.WithK(ctx, topK)        # 어댑터가 태깅
            └─ c.stub.DecryptScores(ctx, …)   # gRPC 호출
                 └─ bench.UnaryInterceptor("vault")  # boot.go:442로 체이닝
                      └─ Observe(ctx, "vault", method, …)  # ctx에서 k 읽어 k= 방출
```

---

## 4. 변경 파일

| 파일 | 변경 |
|---|---|
| `internal/bench/bench.go` | `WithK`/`kFromContext` 추가, `Observe`가 ctx의 k를 `k=`로 append |
| `internal/adapters/vault/client.go` | `DecryptScores`에서 `bench.Enabled()` 가드 후 `WithK`(1곳) + bench import |
| `internal/bench/bench_test.go` | Contract 2b 2종 추가 |

검증: `go test ./internal/bench/... ./internal/adapters/vault/...` ok,
`go build ./...` / `go vet ./internal/...` / `gofmt` clean,
service·lifecycle 회귀 없음(mock vault라 WithK 경로 미접촉).

---

## 5. 경계 (안 한 것)

- **비-vault 라인은 그대로**. k는 DecryptScores에만. `DecryptMetadata`(meta_decrypt)는
  searchSingle 밖 1회 호출이라 fan-out도 k 변동도 없어 대상 아님.
- **본검색/보강 *라벨* 자체는 안 실음**. k 값으로 간접 추론만 가능(k==요청topk→본검색,
  k==그룹크기→보강). 완전한 라벨이 필요하면 추가 계측 필요.
- **prod 영향 0**. `bench.Enabled()`(RUNE_MCP_BENCH=1) 가드로 off일 때 ctx 태깅조차 안 함.

---

## 6. 소비 측 연결

이 `k=`를 받아 파싱·집계·CSV로 내보내는 쪽은 rune-bench다:
- 파싱: bench 라인 `k=` → `parse.Line.K`/`parse.Sample.K`(없으면 -1)
- 출력: raw CSV에 `k` 컬럼 추가(`n,scenario,run_idx,path,phase,k,latency_ms`)
- summary 집계 키는 `(N,path,phase)` 그대로 — k 분리는 raw group-by의 몫

상세: rune-bench `internal/operational/docs/plans/2026-06-19-step3-bench-k-field.ko.md`.
