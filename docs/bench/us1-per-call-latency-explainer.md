# US-1 per-call latency 측정 — 왜 합산하지 않고 호출 단위로 재는가 (해설)

> **이 문서의 목적**: `us1-report-segment-coverage.md` L133~135의 한 줄
> — *"각 호출을 1샘플로(per-call) p50/p95 낸다 … 호출 횟수는 합산하지 말고 별도 기록"* —
> 이 왜 그렇게 결정됐는지를 **구체적인 예시와 실제 로그**로 풀어 설명한다.
> 판정용 본 리포트가 압축적이라, 처음 읽는 사람이 막히는 지점을 여기서 해소한다.
>
> **선행 지식**: 본 리포트 §1.1 표(특히 R3·R9), `internal/service/recall.go`
> (`searchSingle`, `expandPhaseChains`).

---

## 0. 한 줄 요약

`recall` 한 번이 내부에서 `score` RPC를 **여러 번**(데이터 상태에 따라 4~6번 등) 부른다.
이 여러 호출을 **합산해 하나의 숫자로 만들면**, "SDK가 느린가"와 "검색을 많이 부르나"가
섞여 진단이 불가능해진다. 그래서 **각 호출을 독립 샘플로 모아 p50/p95**를 내고,
**호출 횟수(팬아웃)는 별도 칸**에 기록한다.

---

## 1. 왜 score가 "여러 번" 불리는가 — searchSingle 루프

`recall("우리 팀 인증 방식 어떻게 정했더라?")` 한 번을 따라가 보자.

### 1단계 — embed (searchSingle 바깥, R2)

쿼리를 확장한다(`recall.go:71` `ExpandedQueries`, 최대 3개, `:72`):

```
["우리 팀 인증 방식", "OAuth JWT 세션 결정", "로그인 인증 정책"]
```

이 3개를 `EmbedBatch`로 한 번에 임베딩(`recall.go:78`). **searchSingle 바깥**이라 score
루프와 무관하다.

### 2단계 — searchSingle 루프 (핵심)

`searchSingle`이 한 recall 안에서 **여러 번** 돈다(본 리포트 L128~132):

```
searchSingle #1  ← 확장쿼리 "우리 팀 인증 방식"
searchSingle #2  ← 확장쿼리 "OAuth JWT 세션 결정"
searchSingle #3  ← 확장쿼리 "로그인 인증 정책"
searchSingle #4  ← 원문 폴백
   (결과가 반쪽 → 누락 그룹 보강검색 발동, R9 — 아래 §2)
searchSingle #5  ← 누락 그룹 A 보강
searchSingle #6  ← 누락 그룹 B 보강
```

**`searchSingle` 하나가 돌 때마다 그 안에서 `score`(envector inner_product) RPC를 1번씩**
부른다(R3, `recall.go:214`). 즉 **이 recall 한 번 = score RPC 6번 호출**.

---

## 2. R9 "누락 그룹 보강검색"이란 — searchSingle #5·#6의 정체

### 2.1 "그룹"이란 무엇인가

`SearchHit`에는 `GroupID`와 `PhaseTotal` 필드가 있다(`recall.go:468`·`:475`). 즉 저장된
기억(record)이 항상 낱개가 아니라, **여러 개가 하나의 "그룹"으로 묶일 수 있다.**

비유: 책의 **챕터**. 긴 의사결정 하나를 capture하면 여러 record로 쪼개져 저장되고,
이들은 같은 `GroupID`를 공유하며 각자 `PhaseTotal=4`(이 그룹은 총 4개)를 기억한다.

```
GroupID = "auth-decision"  (PhaseTotal = 4)
  ├─ record #1: "OAuth vs 세션 비교"
  ├─ record #2: "JWT 만료 정책"
  ├─ record #3: "리프레시 토큰 처리"
  └─ record #4: "최종 결론: JWT 채택"
```

### 2.2 왜 "누락"이 생기나

1차 검색은 **유사도 점수가 높은 record만** 개별 페이지 단위로 끌어온다. 그래서 챕터가
찢어진다:

```
✅ "auth-decision" record #1  (점수 높음)
✅ "auth-decision" record #4  (점수 높음)
❌ record #2, #3 — 점수가 애매해 안 끌려옴
```

코드가 이걸 잡는다(`recall.go:489~492`):

```go
for _, g := range groups {
    if g.total > g.present {   // 그룹 전체(4) > 찾은 것(2) → 누락!
        incomplete = append(incomplete, g)
    }
}
```

`total(4) > present(2)` → "이 챕터는 4페이지인데 2개밖에 못 찾았다." **결론만 있고 근거가
빠진 결과는 쓸모없으니** 빈 페이지를 채우러 간다.

### 2.3 보강검색 = searchSingle 추가 호출

`recall.go:507~518`:

```go
for _, g := range incomplete {                  // 누락 그룹마다 (최대 2개, :497)
    query := fmt.Sprintf("Group: %s", g.gid)    // "이 그룹 멤버를 찾아줘"
    vec, _ := s.Embedder.EmbedSingle(...)
    hits, _ := s.searchSingle(ctx, vec, g.total) // ← searchSingle 또 호출
    ...
}
```

**누락 그룹 하나당 `searchSingle` 한 번 추가** → 각각 score를 1번씩 더 부른다. `recall.go:497`
에서 **최대 2개로 제한**하므로 보강은 많아야 2번.

### 2.4 전체 그림

```
recall("인증 방식?") 한 번
│
├─ 1차 검색 (확장쿼리 3 + 폴백 1)
│    searchSingle #1~#4  → score 4번
│    결과: auth-decision 그룹이 2/4만 채워짐 ❌
│
├─ "누락 그룹 있네?" 감지 (expandPhaseChains, recall.go:489)
│    → 보강 대상 최대 2개 선정 (bestScore 순, :494)
│
└─ 2차 보강검색
     searchSingle #5  → score 1번  (auth-decision 빈칸 채우기)
     searchSingle #6  → score 1번  (다른 그룹 빈칸 채우기)

총 score 호출 = 6번
```

> **설계 트레이드오프**: 유사도 검색은 빠르지만 챕터를 통째로 못 가져온다. 그래서 ①빠르게
> 1차로 긁고 ②찢어진 그룹만 타깃을 좁혀(`Group: xxx`) 2차로 메꾼다. 전부 완전하게
> 가져오면 무겁고, 안 하면 결과가 반쪽 → **"점수 높은 누락 그룹 최대 2개만 보강"**이라는 절충.

---

## 3. 그래서 bench 로그에 찍히는 것

이 recall 하나가 끝나면 로그에 남는 라인 (`dur_us`=μs, 값은 예시):

```
req=r001 seg=tool     op=recall                   dur_us=84200   ← 전체 (R11)
req=r001 seg=embedder op=.../EmbedBatch           dur_us=9100
req=r001 seg=envector op=.../inner_product        dur_us=11200   ← score #1
req=r001 seg=vault    op=.../DecryptScores        dur_us=3100
req=r001 seg=envector op=.../get_metadata         dur_us=4200
req=r001 seg=vault    op=.../DecryptMetadata      dur_us=2900
req=r001 seg=envector op=.../inner_product        dur_us=10800   ← score #2
req=r001 seg=envector op=.../inner_product        dur_us=13500   ← score #3
req=r001 seg=envector op=.../inner_product        dur_us=12100   ← score #4
req=r001 seg=envector op=.../inner_product        dur_us=11900   ← score #5
req=r001 seg=envector op=.../inner_product        dur_us=14200   ← score #6
```

**같은 `req=r001` 안에 `inner_product` 라인이 6줄**. score가 6개의 측정값을 남겼다:

```
score 측정값 6개: [11200, 10800, 13500, 12100, 11900, 14200]  (μs)
```

---

## 4. 두 갈래 — 합산 vs per-call

### ❌ 합산 (하지 말 것)

```
이 recall의 score 시간 = 11200+10800+13500+12100+11900+14200 = 73700μs
```

"이 recall은 score에 73.7ms 썼다"가 나온다. **그런데 73.7ms가 큰 이유가:**

- envector SDK가 느려서(호출당 ~12ms)?
- 아니면 6번이나 불러서?

→ **구분 불가.** 옆 recall이 score를 3번만 부르면 36ms가 나오는데, "이 recall이 더 빠른
SDK를 썼다"는 **착시**가 생긴다. 실제론 SDK 속도는 같고 호출 수만 다른 것.

### ✅ per-call (채택)

6개를 **합치지 않고 6개 그대로** 측정 풀에 넣고, **호출 횟수 6은 별도 칸**에 기록:

```
score 샘플 풀 ← [11200, 10800, 13500, 12100, 11900, 14200, ...]
recall r001의 score 팬아웃 = 6회   (별도 기록)
```

---

## 5. 여러 recall로 확장 (실제 집계)

시나리오를 12번 반복(본 리포트 L338)하면 호출마다 score 샘플이 쌓인다:

```
r001 → score 6개  [11200, 10800, 13500, 12100, 11900, 14200]
r002 → score 4개  [10900, 11500, 12800, 11100]
r003 → score 7개  [ ... ]
 ...
r012 → score 5개  [ ... ]
```

**모든 샘플을 하나의 풀로** 합친다 (요청 단위가 아니라 호출 단위):

```
전체 score 풀 = [11200, 10800, 13500, ...]   ← 총 64개 샘플
정렬 후:
  p50 ≈ 11,800μs   ("score 한 번에 보통 11.8ms")
  p95 ≈ 14,000μs   ("느릴 땐 14ms까지")

score 팬아웃 = recall당 평균 5.3회 (min 4, max 7)   ← 별도
```

---

## 6. 최종 리포트는 3개의 독립된 숫자로

| 지표 | 예시 값 | 의미 | 산출 |
|---|---|---|---|
| `seg=tool` total p50/p95 | 84ms / 91ms | 사용자 체감 전체(end-to-end) | tool 라인 직접 (합산 아님) |
| score per-call p50/p95 | 11.8ms / 14ms | **SDK 한 호출 속도** | 모든 호출을 샘플로 |
| score 팬아웃 | 평균 5.3회 | **recall이 유발한 호출 수** | 별도 카운트 |

세 지표가 **각각 다른 질문**에 답한다. 섞으면 어느 질문에도 못 답한다.

| 지표 | 답하는 질문 |
|---|---|
| score per-call p50/p95 | "envector SDK가 검색 한 번에 얼마나 빠른가?" (SDK 성능) |
| score 팬아웃 횟수 | "recall 로직이 검색을 얼마나 많이 유발하나?" (rune-mcp 설계 비용) |

**reference(Python)도 score를 호출 1번 단위로 쟀으므로**(본 리포트 L72) `11.8ms vs
reference 12.1ms` 같은 1:1 비교가 성립한다. 만약 73.7ms 합산값으로 뒀다면 reference의
호출당 12.1ms와 비교할 길이 없어진다(단위가 다름).

---

## 7. 진단 관점에서 왜 이 분리가 가치 있나

나중에 "recall이 느려졌다"는 리포트가 나왔을 때, 이 3층 구조가 원인을 한눈에 가른다:

- **score per-call p95↑** → envector SDK가 느려진 것 (N 증가? 인프라?)
- **팬아웃 횟수↑** → recall 로직이 검색을 더 많이 부르게 바뀐 것 (그룹 보강이 자주 터짐?)
- **둘 다 그대로인데 total↑** → 로컬 구간(잔차)이나 다른 RPC 문제

합산해 한 숫자로 뭉갰으면 이 진단이 불가능했다. **측정은 나중에 디버깅할 사람을 위한 것**.

---

## 8. 짚고 넘어갈 함의

- **팬아웃에 상한이 있다**: 보강검색은 최대 2개(`recall.go:497`)라, 찢어진 그룹이 5개여도
  score 추가 호출은 2번에서 멈춘다. 즉 팬아웃 분포에 인위적 천장이 있음 — 측정 해석 시
  "팬아웃이 안 늘어난다"가 데이터가 좋아서가 아니라 **상한에 걸린 것**일 수 있음에 유의.
- **per-call 풀의 가중치**: recall마다 호출 수가 달라 호출을 많이 한 recall의 샘플이 풀에
  더 많이 들어간다. 이는 "SDK 호출 1회의 분포"를 보려는 목적엔 **의도된 동작**이다(호출이
  곧 모집단). recall 단위 비교가 필요하면 그건 `seg=tool` total로 본다.
- **score는 recall·capture가 같은 `op=inner_product`를 방출**한다(본 리포트 §2 경고). per-call
  풀을 만들기 전에 반드시 `req=`로 부모 `seg=tool`(op=recall|capture)에 join해 경로를 가른다.

---

## 부록 — 근거 위치

| 무엇 | 위치 |
|---|---|
| 쿼리 확장(≤3) | `recall.go:71~74` |
| 확장 임베딩(EmbedBatch) | `recall.go:78` |
| searchSingle 내 score 호출 | `recall.go:214` |
| 그룹 누락 판정 (`total > present`) | `recall.go:489~492` |
| 보강 대상 최대 2개 제한 | `recall.go:497` |
| 보강검색 (searchSingle 재호출) | `recall.go:507~518` |
| per-call p50/p95 결정 (D3) | `us1-report-segment-coverage.md` L133~135 |
