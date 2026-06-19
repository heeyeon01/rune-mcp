# US-1 구간 측정 커버리지 — v1.2.2/v1.4.3 비교 리포트를 재생산할 수 있는가

> **목적**: `feat/us1-bench-instrumentation`의 현재 계측으로
> `rune/benchmark/reports/_comparison_v122_vs_v143_body.md`가 쓰는
> **capture/recall 구간별(per-segment) 측정**을 재생산할 수 있는지 판정한다.
>
> **검증 근거 (2026-06-18)**:
> - 대상 계측: `rune-mcp` `internal/bench/bench.go`, `internal/lifecycle/boot.go`,
>   `internal/mcp/tools.go` (데이터 흐름은 `us1-bench-instrumentation-dataflow.md`)
> - reference 구현: `rune` 레포(v0.4.0; Python — 이 문서 대상 `rune-mcp`는 v0.1.0) `benchmark/envector-latency-comparison-1.2.2` 브랜치
>   `benchmark/runners/latency_bench.py` (`_single_capture_phases` L775~, `_single_recall_phases` L835~)
> - 리포트: `rune/benchmark/reports/_comparison_v122_vs_v143_body.md`
>
> **상태 (2026-06-18)**: §3의 진단대로 `score`·`insert`는 streaming이라 인터셉터에 안
> 잡혔다. → **어댑터 레벨 `bench.Observe`로 계측을 옮겨 해결**했다
> (`internal/adapters/envector/client.go` `Score`/`Insert`, §7 P0·P1 완료). 아래 표의 상태는
> *수정 후* 기준이며, "왜 인터셉터로는 안 됐는가"의 분석은 §3에 근거로 보존한다.
> 남은 일: `runebench` 하네스(§6)·목표 확정(§7 P2).

---

## 0. 한 줄 결론

**계측은 완성됐다(P0·P1 완료). 남은 건 리포트를 만들 `runebench` 하네스와, 일부 구간의
비교가능성 선언이다.** recall/capture의 모든 RPC 구간
(`embed·score·vault_topk·get_metadata·meta_decrypt·insert`)과 `total`이 bench 라인으로 나온다 —
US-1의 중심인 `score`·`insert`는 streaming이라 인터셉터에 안 걸렸으나 **어댑터 레벨
`bench.Observe`로 옮겨 해결**했고(§3), unary인 나머지는 인터셉터 유지 → **하이브리드**(§2.1).

단 두 가지는 reference와 1:1이 아니다: `remind`는 D26(vault-위임 메타 복호)로 분해가 다르고(§4),
searchable 3-phase는 v0.1.0에 미존재다(§5). 그리고 N 사전적재·시나리오 구동·로그 집계를 하는
`runebench`(reference의 `latency_bench.py`에 해당)가 아직 없다(§6).

> **정정(분석 보존)**: `score`에는 클라이언트측 암호화가 없다(평문 쿼리 전송) — 잴 암호화는
> `insert` 경로에만 있다(§3.2).

---

## 1. 리포트가 요구하는 구간

리포트(§0 표·기능별 순서)가 정의하는 구간과 기능별 순서:

- **recall**: `embed → score → vault_topk → remind → total`
- **capture**: `embed → score → vault_topk → insert → total`
- **searchable**: `embed → score → vault_topk → insert_rpc → merge_wait → publish_wait → total`

> **`score`는 recall·capture 양쪽 단계다 — 의미가 다르다**(2026-06-18 코드 재검증).
> capture의 `score`+`vault_topk`는 저장 전 **novelty(중복) 검사**다 —
> `runNoveltyCheck`(`internal/service/capture.go:278` `scoreWithRecovery` →
> `:288` `DecryptScores(..., 3)`)가 near-duplicate면 저장을 건너뛴다. recall의
> `score`는 본검색(`internal/service/recall.go:214` `searchSingle`). **둘 다 같은
> 어댑터 `Score`(`adapters/envector/client.go:178`)를 타므로 N-민감이고, bench 출력이
> `seg=envector op=…/inner_product`로 완전히 동일하다** → §2 경고 참조. 뒤따르는
> `vault_topk`의 `top_k`는 다르다: recall=호출자 요청 변수(`recall.go:238`),
> novelty=고정 3(`capture.go:288`). (`SearchByID` `search.go:37`도 Score를 부르나
> US-1 capture/recall 시나리오 밖.)
>
> **reference도 동일하다**(2026-06-18 `latency_bench.py` 직접 확인, 브랜치
> `benchmark/envector-latency-comparison-1.2.2`): `_single_capture_phases`가 capture의
> score를 **`# [2] Novelty score (encrypted similarity search)`**(`:785`)로 명시하고
> `vault_topk`를 `top_k=3`(`:795`)으로 잰다. recall(`_single_recall_phases`)은
> `top_k=topk`(변수, `:852`). → rune-mcp(novelty=3 `capture.go:288`, recall=변수
> `recall.go:238`)와 **구간·top_k가 1:1로 대응**한다. capture가 score를 갖는 것은
> rune-mcp 특유가 아니라 **리포트 정의 자체**가 그렇다.

reference(`latency_bench.py`)는 **하네스가 SDK 핸들을 직접 들고** 각 호출을 `_Timer`로
감싸 쟀다 (하네스 자신이 파이프라인을 오케스트레이션):

```python
# _single_recall_phases (latency_bench.py L835~)
with _Timer() as t_embed:  vec = self._embedding.embed_single(query)          # embed
with _Timer() as t_score:  score_res = self._adapter.score(index, vec)        # score = SDK score 호출 전체
with _Timer() as t_vault:  await self._vault_decrypt_with_retry(blob, topk)   # vault_topk
with _Timer() as t_remind: self._adapter.remind(index, results, ["metadata"]) # remind = 조회+로컬복호
```

핵심: reference는 **함수 호출 전체**를 타이머로 감싼다 → streaming이든 unary든, 암호화가
안에 있든 다 포함된다. rune-mcp의 gRPC 인터셉터 방식은 이 두 성질을 못 따라간다(§3).

### 1.1 rune-mcp v0.1.0 실제 phase 정의 (코드 기준 — reference보다 풍부)

위 reference 구간(§1)은 Python 하네스가 **어댑터를 직접 부른 단순화 버전**이다. rune-mcp의
실제 서비스 파이프라인은 더 길다. 아래는 `internal/service/` 코드를 전수 대조한 결과
(2026-06-18; 서비스 코드는 `git diff v0.1.0 -- internal/service/`가 빈 결과 → **v0.1.0과
동일**). `op=`은 unary는 gRPC FullMethod(인터셉터), envector streaming은 어댑터 상수
(`client.go:34-35`).

표 읽는 법: **`등장기능`(대분류 recall/capture)을 맨 앞에, 위=recall·아래=capture**로 두고,
각 기능 안에서 **실제 코드 실행 순서(R1…/C1…)**로 정렬했다. 열은
`대분류 → 중분류(구간) → 컴포넌트 → 소분류(함수→op/path)` 4단 계층이라 — "무슨 구간을(중분류),
어디서(컴포넌트), 어느 함수로(소분류)" 1회 호출인지 순서대로 읽힌다 → **행 단위로 phase
latency를 귀속**할 수 있다. `측정`열은 그 행을 무엇으로 잡는지다: 인터셉터(unary) / 어댑터
`bench.Observe`(streaming) / 로컬(별도 라인 없음 → `total` 잔차로만 남음).

| 대분류 | # | 중분류(구간) | 컴포넌트 | 소분류 (함수 → `op`/path) | 의미 / 비고 | `seg=` | 측정 | 코드 근거 |
|---|---|---|---|---|---|---|---|---|
| **recall** | R1 | `parse` | rune-mcp(로컬) | `policy.Parse` | 쿼리 파싱·확장(≤3) | — | 로컬 (잔차) | `recall.go:69` |
| **recall** | R2 | `embed` | embedder(runed) | `EmbedBatch` → `/runed.v1.RunedService/EmbedBatch` | 확장쿼리 N개 일괄 임베딩 — **recall 주경로** | `embedder` | 인터셉터(unary) | `recall.go:78` |
| **recall** | R2′ | `embed` | embedder(runed) | `EmbedSingle` → `/runed.v1.RunedService/Embed` | 원문 폴백·그룹확장 임베딩 | `embedder` | 인터셉터(unary) | `recall.go:177`·`:510` |
| **recall** | R3 | `score` | envector | `scoreWithRecovery`→`client.Score` → `/ES2E.ES2EService/inner_product` | 암호화 내적 본검색 — **N-민감, 확장마다 반복** | `envector` | 어댑터 `Observe` (server-stream) | `recall.go:214`→`client.go:178` |
| **recall** | R4 | `vault_topk` | vault(runevault) | `DecryptScores(topk=변수)` → `/rune.vault.v1.VaultService/DecryptScores` | 상위 k 점수 복호 (k=호출자 요청) | `vault` | 인터셉터(unary) | `recall.go:238` |
| **recall** | R5 | `get_metadata` (=remind ①) | envector | `GetMetadata` → `/ES2E.ES2EService/get_metadata` | 후보 메타 조회 | `envector` | 인터셉터(unary) | `recall.go:256`→`client.go:198` |
| **recall** | R6 | `classify` (=remind ②) | rune-mcp(로컬) | `classifyMetadata` | 메타 포맷 3-way 분류 | — | 로컬 (잔차) | `recall.go:303`·`:352` |
| **recall** | R7 | `meta_decrypt` (=remind ③, D26) | vault(runevault) | `DecryptMetadata` → `/rune.vault.v1.VaultService/DecryptMetadata` | AES 메타봉투 **Vault 위임** 복호 | `vault` | 인터셉터(unary) | `recall.go:364` |
| **recall** | R8 | `resolve` (=remind ④) | rune-mcp(로컬) | `resolveMetadata`/`toSearchHit` | 복호 메타 → SearchHit 조립 | — | 로컬 (잔차) | `recall.go:269`·`:399` |
| **recall** | R9 | `group_expand`/filter/rerank | (R2′~R8 반복) | `expandPhaseChains`/`assembleGroups`/`policy.*` | 누락 그룹 보강검색(embed+score+vault+meta 재호출)·필터·재랭크 — **score/vault N 추가** | — | 재호출 → R2′~R8에 합산 | `recall.go:96`–`:103`·`:507` |
| **recall** | R10 | `build` | rune-mcp(로컬) | `buildResult` | confidence·sources 응답 조립 | — | 로컬 (잔차) | `recall.go:111` |
| **recall** | R11 | `total` | — | `benchWrap` | recall 핸들러 end-to-end (op=`recall`) | `tool` | 핸들러 wrap | `tools.go:238` |
| **capture** | C1 | `validate` | rune-mcp(로컬) | `ParseExtractionFromAgent` (+D14) | 텍스트 검증·extraction 파싱 | — | 로컬 (잔차) | `capture.go:71`·`:89` |
| **capture** | C2 | `build` | rune-mcp(로컬) | `BuildPhases` | 정책 레코드 생성(N records) | — | 로컬 (잔차) | `capture.go:110` |
| **capture** | C3 | `embed` (novelty) | embedder(runed) | `EmbedSingle` → `/runed.v1.RunedService/Embed` | novelty용 단건 임베딩 | `embedder` | 인터셉터(unary) | `capture.go:272` |
| **capture** | C4 | `score` (novelty) | envector | `scoreWithRecovery`→`client.Score` → `/ES2E.ES2EService/inner_product` | novelty 유사도 — **N-민감, recall R3과 동일 op** (§2 경고) | `envector` | 어댑터 `Observe` (server-stream) | `capture.go:278`→`client.go:178` |
| **capture** | C5 | `vault_topk` (novelty) | vault(runevault) | `DecryptScores(top_k=3 고정)` → `/rune.vault.v1.VaultService/DecryptScores` | 상위 3 점수 복호 | `vault` | 인터셉터(unary) | `capture.go:288` |
| **capture** | C6 | `classify` (novelty) | rune-mcp(로컬) | `policy.ClassifyNovelty` | near_dup(≥0.95) → **조기반환, C7~C10 skip** | — | 로컬 (잔차) | `capture.go:301`·조기반환 `:124` |
| **capture** | C7 | `embed_batch` | embedder(runed) | `EmbedBatch` → `/runed.v1.RunedService/EmbedBatch` | 저장 레코드 N개 일괄 임베딩 | `embedder` | 인터셉터(unary) | `capture.go:137` |
| **capture** | C8 | `seal` | rune-mcp(로컬) | `sealMetadata`→`envector.Seal` | 메타 AES 봉인(암호화) | — | 로컬 (잔차) | `capture.go:142`→`:332` |
| **capture** | C9 | `insert` | envector | `insertWithRecovery`→`client.Insert` → `/ES2E.ES2EService/batch_insert_data` | FHE 암호화+적재 (암호화는 RPC 직전 client-side, §3) | `envector` | 어댑터 `Observe` (client-stream) | `capture.go:153`→`client.go:152` |
| **capture** | C10 | `capture_log` | rune-mcp(로컬) | `CaptureLog.Append` | 캡처 로그 append(D19, 실패 허용) | — | 로컬 (잔차) | `capture.go:172` |
| **capture** | C11 | `total` | — | `benchWrap` | capture 핸들러 end-to-end (op=`capture`) | `tool` | 핸들러 wrap | `tools.go:238` |

**계측 시 주의 (위 표에서 직접 따라옴):**

- **recall `embed`는 `EmbedSingle`이 아니라 `EmbedBatch`가 주경로**(R2, `recall.go:78`). 이전 판이
  인용한 `:177`(EmbedSingle)은 *원문 폴백*, `:510`은 *그룹확장*용 추가 호출이다 → R2′로 분리.
- **`searchSingle`(`recall.go:212`)은 표에 행이 없다 — phase가 아니라 R3~R8을 묶는 컨테이너**다
  (`score`→`vault_topk`→`get_metadata`→`resolveMetadata`{`classify`+`meta_decrypt`+`resolve`}).
  그 자체 duration = R3+…+R8 합이라 행으로 넣으면 이중 계산이다. R2 `embed`는 이 함수 **바깥**
  (`Handle`/`searchWithExpansions`)에서 먼저 일어난다. **중요: `searchSingle`이 곧 N-루프 단위**다 —
  확장쿼리마다(`:151`)+원문 폴백(`:180`)+누락 그룹마다(`:515`=R9) 반복 호출되므로, 한 `req` 안에
  R3~R8 라인이 **searchSingle 호출 횟수만큼 여러 벌** 찍힌다. → `(req, op)`로 묶어 합산·횟수
  집계하면 되고, 개별 호출을 분리할 필요는 없다(중분류로 합치므로).
- **R3·R5·R7(+R4)은 확장쿼리마다, 다시 R9에서 또 호출**된다(위 `searchSingle` 루프). 즉 한
  recall이 score를 여러 번 부른다 → **각 호출을 1샘플로(per-call) p50/p95** 낸다(reference도 단일
  호출 단위라 1:1; runebench 설계서 **D3**). 호출 횟수는 **합산하지 말고** 별도 기록 — 합산하면
  SDK 비용과 팬아웃 횟수가 섞인다(end-to-end는 `seg=tool` total).
  → **이 결정의 구체적 예시·로그·집계 흐름은 `us1-per-call-latency-explainer.md` 참고**(R9 보강검색이
  score 호출 수를 들쭉날쭉하게 만드는 과정을 단계별로 풀어 설명).
- **R3 `score` ↔ C4 `score`는 같은 `seg=envector op=…/inner_product`**를 방출한다(§2 경고). `op=`만으로는
  recall 본검색과 capture novelty가 한 버킷에 섞이므로 **`req=`로 부모 `seg=tool`(op=recall/capture)에
  join**해 경로를 가른 뒤 집계해야 한다. `vault_topk`도 동일(R4 k=변수 vs C5 k=3).
- reference의 단일 `remind`(로컬) = rune-mcp의 **R5~R8 네 조각**(R6·R8은 로컬 분류·조립)이고 R7은 Vault 왕복(D26)이 추가다(§4).
- **searchable**(`insert_rpc/merge_wait/publish_wait`)은 v0.1.0에 미존재(grep 0건) → 비교 불가(§5).

> **reference 표를 그대로 쓰면 틀리는 점 3가지**: ㈎ capture는 4단계가 아니라
> novelty(embed+score+vault_topk) + **embed_batch + seal** + insert로 더 길다(Python은
> `adapter.insert`를 직접 불러 batch-embed·seal이 안 보였을 뿐). ㈏ recall `remind`는
> "로컬"이 아니라 **Vault RPC(`DecryptMetadata`, D26) 포함**. ㈐ `embed`는 순수 로컬이
> 아니라 **runed 별도 프로세스 unix-socket gRPC 왕복**.

---

## 2. 매핑 — 리포트 구간 ↔ reference ↔ 현재 rune-mcp bench

| 리포트 구간 | reference(Python)가 잰 것 | 현재 rune-mcp bench 라인 | 상태 |
|---|---|---|---|
| `embed` | `embedding.embed_single` (로컬) | `seg=embedder` | ✅ 됨 (runed로의 unix-socket gRPC 왕복 포함) |
| `score` | `adapter.score(idx, vec)` | `seg=envector op=…/inner_product` (어댑터 레벨) | ✅ 됨 (P0, §3) |
| `vault_topk` | `vault_decrypt_with_retry` | `seg=vault op=…/DecryptScores` | ✅ 됨 |
| `remind` (recall) | `adapter.remind` = 메타 조회 + **로컬** AES 복호 | `seg=envector op=…/get_metadata` + `seg=vault op=…/DecryptMetadata` | ⚠️ **분해 다름** (§4) |
| `insert` (capture) | `adapter.insert` | `seg=envector op=…/batch_insert_data` (어댑터 레벨) | ✅ 됨 (P0, §3) |
| `total` | 전체 wrap | `seg=tool` | ✅ 됨 |
| `insert_rpc/merge_wait/publish_wait` | searchable 3-phase | ❌ 없음 | ❌ (§5) |

> rune-mcp는 **gRPC 메서드 단위**(`op=…/get_metadata`)로 찍고, 리포트의 의미 이름
> (`score`/`remind`)으로 찍지 않는다. 하네스가 `op=`→구간명 매핑 + `req=` group-by로
> 재구성해야 한다.
>
> ⚠️ **`score`는 `op=`만으로 recall↔capture를 못 가른다.** recall 본검색과 capture
> novelty 검사가 **같은 `seg=envector op=…/inner_product`**를 방출하므로(§1 노트),
> `op=`→구간명 매핑으로는 둘이 한 버킷에 섞인다. **반드시 `req=`로 부모 `seg=tool`
> 라인(`op=recall`/`op=capture`)에 join해 경로를 귀속시킨 뒤** 구간을 집계해야 한다.
> 안 그러면 ㈎ recall N-커브에 capture novelty가 오염되고 ㈏ capture/recall 비용
> 비교가 깨진다. (`vault_topk`도 같은 이유로 경로별 분리 필요 — top_k가 다름.)

### 2.1 검증된 실제 커버리지 (RPC 방식별)

무엇이 잡히고 안 잡히는지는 **각 호출의 gRPC 방식**이 결정한다. bench는 **unary
인터셉터만** 설치하고(`boot.go` `withBench`), envector SDK는 **stream 인터셉터 옵션을
아예 노출하지 않는다**(`clientoptions.go` — `WithUnaryInterceptor`만).

| 구간 | 호출 | gRPC 방식 | 근거 | 현재 잡히나 |
|---|---|---|---|---|
| `seg=tool` (total) | benchWrap (gRPC 아님) | — | `tools.go` `benchWrap` | ✅ |
| `embed` | `Embed`/`EmbedBatch` | unary `cc.Invoke` | runed `runed_grpc.pb.go:64,74` | ✅ |
| `vault_topk` | `DecryptScores` | unary `cc.Invoke` | vault grpc `:63` | ✅ |
| `remind` 일부 | `GetMetadata` | unary `cc.Invoke` | es2e grpc `:73` | ✅ |
| `remind` 일부 | `DecryptMetadata` | unary `cc.Invoke` | vault grpc `:73` | ✅ |
| **`score`** | **`InnerProduct`** | **server-streaming `NewStream`** | es2e grpc `:80-82` | ✅ **어댑터 레벨** (P0) |
| **`insert`** | **`BatchInsertData`** | **client-streaming `NewStream`** | es2e grpc `:99-101` | ✅ **어댑터 레벨** (P0) |

> **원래 결함**: unary(embed·vault·get_metadata)만 인터셉터에 걸리고, US-1의 두 핵심
> 지표가 정확히 streaming(score·insert)이라 한 줄도 안 나왔다.
> **P0 수정**: score·insert는 인터셉터가 아니라 **어댑터에서 `bench.Observe`로 직접**
> 잡는다(`client.go` `Score`/`Insert`). vault·embedder·get_metadata는 unary라 인터셉터
> 유지 → **하이브리드**. seg=는 `envector`로 통일, op=는 gRPC full-method 형식 유지.

### 2.2 `op` → 구간 매핑 (harness 파싱 테이블)

`runebench`(§6)가 bench 로그 한 줄(`seg=… op=… req=…`)을 §1.1의 구간(R*/C*)으로 환원할 때
쓰는 **결정 테이블**이다. 키는 `(seg, op)`이고, 같은 `(seg, op)`가 recall·capture 양쪽에
나오는 행은 **`req=`로 부모 `seg=tool` 라인(`op=recall`\|`capture`)에 join**해야 비로소 구간이
하나로 정해진다(§2 경고). 즉 분리 키 = **`op` 단독으로 충분한가**의 답이다.

| `seg` | `op` (gRPC full-method) | recall → 구간 | capture → 구간 | `op` 단독으로 분리되나 |
|---|---|---|---|---|
| `embedder` | `/runed.v1.RunedService/EmbedBatch` | `embed` (R2) | `embed_batch` (C7) | ❌ → `req`로 `tool.op` join |
| `embedder` | `/runed.v1.RunedService/Embed` | `embed` 폴백·그룹확장 (R2′) | `embed` novelty (C3) | ❌ → `req`로 `tool.op` join |
| `envector` | `/ES2E.ES2EService/inner_product` | `score` (R3) | `score` novelty (C4) | ❌ **필수 join** (N-커브 오염 방지) |
| `vault` | `/rune.vault.v1.VaultService/DecryptScores` | `vault_topk` k=변수 (R4) | `vault_topk` k=3 (C5) | ❌ → `req` join (k 의미 다름) |
| `envector` | `/ES2E.ES2EService/get_metadata` | `get_metadata` (R5) | — | ✅ recall 전용 |
| `vault` | `/rune.vault.v1.VaultService/DecryptMetadata` | `meta_decrypt` D26 (R7) | — | ✅ recall 전용 |
| `envector` | `/ES2E.ES2EService/batch_insert_data` | — | `insert` (C9) | ✅ capture 전용 |
| `tool` | `op=recall` | `total` (R11) | — | ✅ 부모 라인 |
| `tool` | `op=capture` | — | `total` (C11) | ✅ 부모 라인 |

> **로컬 구간(R1·R6·R8·R10, C1·C2·C6·C8·C10)은 이 표에 없다** — bench 라인을 안 내므로
> `op`이 없다. 해당 시간은 `seg=tool`(total)에서 위 RPC 구간 합을 뺀 **잔차**로만 추정된다.
> "이 표에 있는 것 = bench 라인으로 직접 측정되는 것"이고, 없는 구간은 잔차 추정임을 혼동하지 말 것.
>
> 위 표의 **소스는 §1.1 표의 `소분류` 열**이다(path가 곧 매핑 키). §1.1에 행을 추가/수정하면
> 이 테이블도 같이 갱신해야 둘이 어긋나지 않는다 — harness 코드의 매핑 상수는 이 표를 1:1로 옮긴 것.
>
> **재시도 라인**: `scoreWithRecovery`/`insertWithRecovery`는 복구 가능 에러 시 `Score`/`Insert`를
> 1회 재시도하므로(`recovery.go:47`), 한 `req`에 같은 `op`가 **실패(`ok=false`)+성공(`ok=true`)
> 두 줄** 찍힐 수 있다 — 기록은 그대로 남는다(`client.go:190`, err 검사 *전* `Observe`). 집계 시
> latency p50/p95에선 **`ok=false`를 제외**(성공 경로만; reference엔 복구 로직 자체가 없음),
> `ok=false` 줄은 "복구가 몇 번 발동했나" 신호로 보존한다.

### 2.3 로컬 구간 분해 — 기본은 잔차 (결정)

**결정: 로컬 구간(R1·R6·R8·R10, C1·C2·C6·C8·C10)은 쪼개지 않고 `total` 잔차 한 칸으로 둔다.**
US-1의 목적은 `score` N-커브(`bench.go` 주석)이고, 로컬은 N-독립 sub-ms라 SDK 판정을 못 바꾼다.

무시 못 할 로컬이 있는지는 코드 변경 0으로 본다: 라인의 slog `time=`(ms)과 `dur_us`로 인접
span의 gap을 ms로 역산 — `gap(K→K+1) ≈ (time_{K+1} − dur_{K+1}) − time_K`. `~0ms`면 무시 가능,
수 ms면 그 값으로 충분. **ms가 분해능 하한**이다(sub-ms는 잴 필요가 없는 영역).

한계: 앞/뒤 끝 로컬(R1·R10, C1·C2·C10)은 RPC 짝이 없어 `tool` span 대비 합으로만 잡히고
**C1·C2는 분리 불가**. gap엔 GC·스케줄링·slog 쓰기가 섞여 **p50으로만** 신뢰. §2.2 집계는 대체 안 함.

---

## 3. `score`·`insert` — streaming이라 인터셉터로는 미측정 → 어댑터 레벨로 해결 (P0 완료)

> **정정 노트**: 이전 판에는 "score에 클라이언트측 FHE 암호화가 있어 인터셉터가 그
> 일부를 놓친다"고 적었으나 **사실이 아니다.** ㈎ `Score`는 쿼리를 **평문으로** 보낸다
> (암호화 없음) ㈏ 진짜 문제는 `Score`·`Insert`가 **streaming RPC라 unary 인터셉터에
> 아예 안 걸린다**는 것이다.

### 3.1 왜 안 잡히나 — streaming RPC

bench 인터셉터는 `grpc.UnaryClientInterceptor`다(`bench.go:99`). unary 인터셉터는
`cc.Invoke`(요청-응답 1회)에만 발동하고, `cc.NewStream`으로 여는 **streaming 호출에는
발동하지 않는다.**

- `Score` = `InnerProduct` → **server-streaming** (`es2e-api_grpc.pb.go:80`,
  `c.cc.NewStream(...)` 후 `stream.Recv()` 루프 — SDK `score.go:42`)
- `Insert` = `BatchInsertData` → **client-streaming** (`es2e-api_grpc.pb.go:99`,
  `stream.Send()` — SDK `insert.go:65`)

게다가 envector SDK는 `WithUnaryInterceptor`만 노출하고 **stream 인터셉터 옵션이 없다**
(`clientoptions.go:67`). → **인터셉터 경로로는 score·insert를 원천적으로 못 잰다.**

### 3.2 암호화는 어디에 있나 (정정)

`score`는 평문 쿼리를 보내므로 **잴 암호화가 없다**(`score.go`의 요청은 `PlainVector`,
`Data: query`). 암호화는 **`insert` 경로에만** 있다 — `Index.Insert`가 RPC 전에
`Keys.Encrypt`를 호출한다(SDK `insert.go:60`):

```go
// SDK insert.go (요약)
ciphers, innerCounts, err := i.keys.Encrypt(req.Vectors) // ① 클라이언트 FHE 암호화 (cgo)
stream, err := i.client.stub.BatchInsertData(...)        // ② client-streaming RPC
```

암호화(①)는 **stream이 열리기 전**에 끝난다. 따라서 **어떤 gRPC 인터셉터(unary든
stream이든)로도 ①을 못 잡는다** — 빨라야 ②(NewStream) 시점에 걸리기 때문이다.

### 3.3 그래서 어떻게 재나

| 무엇 | 방법 | 비고 |
|---|---|---|
| score (recall) | **어댑터에서 `bench.Observe`로 `c.idx.Score` 감싸기** | streaming 무관하게 잡힘. 잴 암호화 없음 → 그대로 reference와 비교 가능 |
| insert 전체 (capture) | **어댑터에서 `c.idx.Insert` 감싸기** | "암호화 ① + RPC ②" 합계 |
| insert의 암호화만 | **`Keys.Encrypt`를 라이브 Insert 호출 밖에서 직접 호출** (public API, `keys.go:80`) | N-독립 상수 → 1회 캘리브레이션. Insert는 평문만 받아 내부에서 합치므로 호출 내부 분해는 불가 |

→ **핵심: envector 구간은 인터셉터가 아니라 어댑터 레벨 계측으로 옮긴다.**
**적용됨** (`internal/adapters/envector/client.go`):

```go
func (c *client) Score(ctx context.Context, vec []float32) ([][]byte, error) {
    if c.idx == nil { return nil, &Error{...} }
    start := time.Now()
    blobs, err := c.idx.Score(ctx, vec)              // streaming — 인터셉터 못 봄
    bench.Observe(ctx, "envector", opScore, start, err) // ← 여기서 직접 (bench.Enabled() self-guard)
    if err != nil { return nil, MapSDKError(err) }
    return blobs, nil
}
```

`opScore`/`opInsert`는 gRPC full-method 경로 상수(`/ES2E.ES2EService/inner_product` 등)로,
unary 인터셉터가 내는 `op=` 형식과 통일해 harness 파싱을 단순화한다. `Insert`도 동일
패턴(암호화+RPC 합계를 한 줄로).

---

## 4. ⚠️ `remind` — D26(vault-위임 메타 복호)로 분해가 바뀜

- **reference(v1.2.2)**: 메타 조회 + AES 복호를 **로컬 한 타이머**로 (`adapter.remind`)
- **rune-mcp(v0.1.0)**: `GetMetadata`(envector RPC) + `DecryptMetadata`(**vault 왕복**) +
  로컬 조립 — `internal/service/recall.go` phase 5 "metadata classification +
  Vault-delegated decrypt (D26)", `s.Vault.DecryptMetadata` (recall.go:364)

→ 재구성하려면 두 bench 라인을 합쳐야 하고, **reference엔 없던 vault 왕복이 추가**된다.
로컬 조립 시간은 따로 안 잡혀 `seg=tool` 잔차에만 남는다. 즉 `remind`는 v1.2.2와
**원천적으로 1:1이 아니다**.

---

## 5. ❌ searchable — 측정 안 됨 (질문 범위 밖이나 명시)

`insert_rpc/merge_wait/publish_wait`는 v1.4.3 IVF_VCT의 **서버 lifecycle**
(MERGED_SAVED→SEARCHABLE) 단계다. 애초에 `insert`조차 현재 미측정인 데다(§3, streaming),
병합/공개 대기는 별도 상태 폴링이 필요해 더더욱 안 잡힌다. 리포트 §5·§7도 이미
"메커니즘이 달라 total만, SDK 우열로 읽지 말 것"이라 했다 → **비교 불가로 선언**.

---

## 6. 빠진 절반 — `runebench` 하네스

이번 PR은 rune-mcp **안쪽 계측**만 넣었다. 리포트를 만들려면 reference
`latency_bench.py`가 하던 일이 별도로 필요하다:

- **N 사전적재** — reference는 **직접 envector batch insert**로 채운다(측정 경로 우회):
  전용 bench 인덱스에 deterministic 랜덤 벡터(seed `0xBEEF`, `BENCH_DIM`)를 `PRIMER_BATCH_ROWS`
  청크로 적재 후 searchable 폴링(`latency_bench.py:671` `_prime_bench_index`; `run_sweep`은
  `--direct-envector` 요구 `:1479`). **per-N 격리**: N마다 drop+create+prime한 fresh 인덱스
  (`{bench}_N{N}_…`) — mutating(capture)=시나리오별, read-only(recall)=그룹 공유. 프로덕션
  인덱스는 안 건드림.
- 시나리오(T1/T2/T5/T6…) 구동 · 12회 반복 · warmup 3 · p50/p95 집계
- **bench 로그 파싱 → `op=`→구간 매핑(§2.2) → `req=` group-by**
- **rune-mcp를 ACTIVE로 부팅시키는 프로비저닝(harness 책무)** — 평소 `/rune:configure`+
  `/rune:activate`(rune 플러그인/CLI)가 하던 일이 bench엔 없다. 그게 없으면 `boot.go`는
  **Dormant에서 멈춘다**("config.json not found" `boot.go:355`). 그래서 runebench가 대신해야 한다:
  ① `~/.rune/config.json`(vault endpoint·token·state=`active`) 작성 ② vault·runed(embedder)를
  띄워두기 — 부팅이 `GetAgentManifest`로 키+인덱스명을 받고(`boot.go:452`) OpenIndex하므로
  ③ state=`active`면 기동 시 자동 부팅, 아니면 `activate` 툴 호출. config는 디스크에 남으니
  **1회 프로비저닝 후 per-N 재기동은 ACTIVE 직행**. ⚠️ **인덱스명은 vault manifest에서 온다**
  (`boot.go:498` `bundle.IndexName`) — 로컬 config가 아니라 → per-N 인덱스 전환이 vault
  프로비저닝과 결합된다(고정 인덱스 재-prime vs per-N manifest, 미결 하위결정).
- **부팅 장치(`RUNE_BENCH_N`은 프로세스당 고정)** — bench는 env-gate(`bench.go:39`)뿐 rune-mcp엔
  플래그가 없다. `RUNE_BENCH_N`은 env라 프로세스당 1값이므로, sweep마다 N을 바꾸려면 **N 지점마다
  rune-mcp를 `RUNE_MCP_BENCH=1 RUNE_BENCH_N=<N>`로 (재)기동**해야 한다. 이게 "bench-on 부팅"
  장치이자 **별도-프로세스 모델을 사실상 강제**하는 지점(아래 열린 결정과 직결). rune-mcp 쪽
  새 코드는 불필요.

> **적재 경로 = insert 오염 여부**: reference처럼 **직접 envector**로 적재하면 rune-mcp `Insert`를
> 안 타 bench 라인이 안 나온다 → insert 구간 오염 없음(boot `OpenIndex` 빈-req 1줄만). 반대로
> 적재를 rune-mcp **capture로** 하면 `op=…/batch_insert_data` 라인 N개가 측정 통계에 섞이므로,
> **직접-envector 적재를 권장**한다(그러면 §2.2의 빈-req 분리 부담도 boot 1줄로 줄어든다).

이는 **확정된 결정**(2026-06-18 — US-1 harness는 rune-mcp를 **별도 프로세스**로 구동;
아키텍처 doc §4)과 직결된다. 현재 계측이 **slog 로그로 방출**하도록 설계된 것은 바로
그 "별도 프로세스 + 로그 파싱" 모델을 전제한 것이다.

---

## 7. 판정 & 우선순위 (검토 포인트)

이건 "정밀도 튜닝"이 아니라 **계측이 주목적(score N-커브, insert)을 못 잰다는 버그**다.
목표(가/나)와 무관하게 score·insert는 잡혀야 한다.

**우선순위 제안**
1. ✅ **(P0, 완료) envector 계측을 어댑터 레벨로 이동** — `Score`/`Insert`를
   `bench.Observe`로 감쌌다(`client.go`). 인터셉터는 streaming을 못 잡고 SDK도 stream
   인터셉터를 안 주므로 이게 **유일한 길**. vault·embedder는 unary라 인터셉터 유지 →
   **하이브리드**. (부수효과: "어댑터 코드 안 건드린다"는 원래 장점을 envector에 한해
   포기 — 단 그 장점은 인터셉터가 envector에서 작동을 안 해 이미 무의미했다.)
2. ✅ **(P1, 완료) 회귀 테스트용 `idx` 인터페이스 seam** — `idx`를 구체
   `*envector.Index` → `sdkIndex` 인터페이스로 추출(`client.go`). fake를 주입해
   **streaming(score·insert) 경로에서 bench 라인이 나오는지**를 회귀로 고정했다
   (`client_bench_test.go`: emit/off/error 4종). `bench.Observe`를 지우면 어서션이
   `full: ""`로 잡아 원래 결함(조용히 0줄) 재발을 막는다 — teeth 확인 완료.
   추가로 암호화 독립 측정은 `encrypt_bench_test.go`(`BenchmarkEncrypt`, 서버-프리).
3. **(P2) 목표 확정** — (가) reference(`rune` v0.4.0)의 v1.2.2 리포트 숫자 재생산 vs
   (나) `rune-mcp` v0.1.0 새 N-커브 기준선. score는 평문 쿼리라 암호화 보정이 필요 없어
   **어느 쪽이든 어댑터 계측이면 충분**하다.
4. **(P3) remind·searchable은 "비교 불가"로 선언** — D26·서버 lifecycle 차이로 v1.2.2와
   1:1 불가. 억지로 맞추지 말 것(리포트 §5/§7과 동일 톤).
5. **(P4) `runebench` 하네스**는 별도 작업. 그 전에 프로세스 경계 결정부터 닫는다.

---

## 부록 — 검증 근거 위치

| 무엇 | 위치 |
|---|---|
| reference 구간 정의 (recall) | `rune` `benchmark/.../latency_bench.py:835~` (`_single_recall_phases`) |
| reference 구간 정의 (capture) | 〃 `:775~` (`_single_capture_phases`) |
| `score` = 평문 쿼리 (암호화 없음) | envector-go-sdk `score.go:30-41` (`PlainVector`, `Data: query`) |
| `score` = server-streaming | envector-go-sdk `es2e-api_grpc.pb.go:80-82` (`InnerProduct`, `NewStream`) |
| `insert` = client-streaming | envector-go-sdk `es2e-api_grpc.pb.go:99-101` (`BatchInsertData`, `NewStream`) |
| `insert` 암호화 위치 | envector-go-sdk `insert.go:60` (`Keys.Encrypt`, RPC 전) / public API `keys.go:80` |
| SDK는 stream 인터셉터 미노출 | envector-go-sdk `clientoptions.go:67` (`WithUnaryInterceptor`만) |
| 메타 복호 (D26) | `rune-mcp internal/service/recall.go:364` (`Vault.DecryptMetadata`) |
| bench 인터셉터(=unary만) | `rune-mcp internal/bench/bench.go:99` (`UnaryInterceptor`) |
| bench 라인 스키마 | `rune-mcp internal/bench/bench.go:69` (`Observe`) |
</content>
