---
title: "9. Past DAG 재실행과 Downstream 자동 트리거"
status: draft (실측 진행 중)
tags:
  - airflow
  - asset
  - manual-trigger
  - backfill
  - clear-and-rerun
  - experiment
created: 2026-07-24
updated: 2026-07-24
---

# 9. Past DAG 재실행과 Downstream 자동 트리거

> Producer 의 과거 dag_run 을 수동 재실행 (clear+rerun / manual trigger / backfill) 했을 때, asset-scheduled downstream 이 어떻게 반응하는지에 대한 실측 문서.

## 왜 이 문제가 중요한가

Asset scheduling 이 실무에 자리잡으려면 반드시 답해야 할 질문들:

1. **운영 중 실수/장애 복구 시** — Producer 를 되돌려 실행할 때 downstream 파급이 예측 가능해야 함
2. **Story 팀 시연 시** — Producer 재실행 데모가 downstream storm 을 안 만들어야 함
3. **Provider 확장 설계 시** — Stamp task 의 `skip_on_run_type=["backfill"]` 게이트가 실제로 효과 있는지, 어떤 종류의 재실행에 걸리는지 명확해야 함
4. **3.2/3.3 이관 결정 시** — Partition_key dedup 동작이 재실행 케이스에서 어떻게 다른지 판단 근거 필요

Airflow 문서에 이 부분이 산발적으로 언급될 뿐 체계적으로 정리 안 됨. **실측이 유일한 답**.

## 재실행의 종류 (용어 정리)

| 방법 | 명칭 | 시나리오 | run_type |
|---|---|---|---|
| UI 에서 dag_run clear | Clear + Rerun | 실패한 run 재실행, 로직 수정 후 재처리 | `scheduled` (원래 것 유지) |
| UI/CLI `airflow dags trigger` | Manual Trigger | 임의 시점 실행 | `manual` |
| CLI `airflow backfill create` | Backfill | 과거 기간 일괄 실행 | `backfill` |
| Task 개별 clear | Task Clear | 특정 task 만 재실행 | 원래 run_type 유지 |

**시나리오별로 asset event 발신 여부와 downstream 파급이 다를 수 있음** — 이게 실측 항목의 핵심.

## Airflow 이론상 동작 (실측 전 예상)

### 규칙 1 — Outlet emission 은 task 성공 시 무조건

Task 가 성공 상태로 끝나면 `run_type` 에 관계없이 outlet event 를 emit. Airflow 는 "scheduled run 만 emit" 같은 필터를 기본 제공 안 함.

**시사점**: Clear + Rerun / Manual Trigger / Backfill 모두 downstream 을 깨움.

### 규칙 2 — Downstream 은 unconsumed event 를 감지해 dag_run 생성

Producer 가 새 event 를 emit → 스케줄러가 downstream 의 subscribed asset 을 스캔 → unconsumed event 있으면 downstream dag_run 생성.

**시사점**: Producer 재실행 → downstream 자동 트리거 (거의 확실).

### 규칙 3 — Downstream 의 dag_run 시각은 트리거 시각 (producer 의 원래 시각이 아님)

Producer 를 `2026-07-20` 로 clear+rerun 해도 downstream dag_run 의 `data_interval_start` 는 트리거 순간 (`2026-07-24`).

**시사점**: Downstream 에서 producer 의 원래 시각을 알려면 `triggering_asset_events[...].source_run_id` 를 파싱하거나 `extra` 에 담긴 값을 봐야 함.

### 규칙 4 — Consumed event 는 재사용 안 됨

한번 downstream 이 consume 한 event 는 다시 트리거 요인이 안 됨. 다만 **재실행 시 producer 가 새 event 를 emit 한다면** 그건 별개의 unconsumed event 로 취급될 것으로 예상.

**시사점**: Producer 를 clear+rerun → 새 event → downstream 재트리거 (예상).

### 규칙 5 — Partition_key 있는 경우 (3.2+) 는 미상

같은 partition_key 로 여러 번 emit 시 downstream 이 매번 뜨는지, dedup 되는지 문서에 명확한 답 없음. **실측 필수**.

## 실측 테스트 시나리오 매트릭스

Diana 님이 지금 테스트 진행 중. 아래 표에 결과 채워 나감.

### 시나리오 A — 단순 Clear + Rerun

**세팅**: Producer 정상 실행됐고 downstream 도 완료된 상태.

| 테스트 | 예상 | 실측 결과 | 비고 |
|---|---|---|---|
| A-1. Producer dag_run 1개 clear + rerun | Downstream dag_run 1개 새로 생성 | ✅ 확인됨 (2026-07-24) | Downstream 자동 트리거 됨 |
| A-2. Downstream 이 running 중일 때 A-1 실행 | 큐잉, 현재 run 끝난 뒤 다음 run 생성 | TODO | |
| A-3. 같은 producer dag_run 을 두 번 연속 clear+rerun | Downstream dag_run 2개 생성? 아니면 dedup? | TODO | **dedup 확인 핵심** |

### 시나리오 B — 여러 개 순차 재실행

**세팅**: Producer 최근 5시간치 (h1..h5) 재실행 필요.

| 테스트 | 예상 | 실측 결과 | 비고 |
|---|---|---|---|
| B-1. h1, h2, h3, h4, h5 를 순서대로 clear + rerun | Downstream dag_run 개수는 5개? 아니면 뭉침? | TODO | `max_active_runs` 영향 |
| B-2. h1~h5 를 UI 에서 한번에 multi-select clear | 위와 같은지 다른지 | TODO | |
| B-3. h1~h5 를 backfill CLI 로 한번에 | run_type=backfill. Downstream 반응 | TODO | |

### 시나리오 C — Manual Trigger

**세팅**: Producer 를 임의 시점으로 manual trigger.

| 테스트 | 예상 | 실측 결과 | 비고 |
|---|---|---|---|
| C-1. Producer 를 지금 시각으로 manual trigger | Downstream 정상 트리거 | TODO | |
| C-2. Producer 를 과거 logical_date 로 manual trigger | Downstream 트리거, data_interval 은 트리거 시각 | TODO | |
| C-3. Downstream 완료 후 producer 를 같은 logical_date 로 manual trigger | 재트리거 되는지 | TODO | |

### 시나리오 D — Downstream 관측

**세팅**: Downstream 에서 event 정보 확인.

| 테스트 | 예상 | 실측 결과 | 비고 |
|---|---|---|---|
| D-1. Downstream 의 `triggering_asset_events` 내용 | Producer 재실행마다 새 event object | TODO | run_type, source_run_id 값 확인 |
| D-2. Downstream 의 `data_interval_start` | 트리거 시각 (producer 의 원래 시각 아님) | TODO | |
| D-3. Event 의 `extra` 에 stamp payload 넣은 경우 | Producer 재실행 시 payload 도 반영? | TODO | Stamp task 도입 시 확인 |
| D-4. Event 의 `source_run_id` 형식 | `scheduled__...`, `manual__...`, `backfill__...` 접두어 확인 | TODO | 파싱 fragility 확인 |

### 시나리오 E — Multi-asset AND downstream

**세팅**: Downstream 이 `schedule=[A, B]` 로 AND 구독. Producer 는 두 asset 을 emit.

| 테스트 | 예상 | 실측 결과 | 비고 |
|---|---|---|---|
| E-1. Producer 재실행 시 A, B 모두 emit → downstream 1회 트리거 | 정상 트리거 | TODO | |
| E-2. Producer 재실행 시 A 만 emit 되게 (예: task 하나만 clear) | Downstream 트리거 되는지? A 는 왔지만 B 는 새로운 event 없음 | TODO | **AND 매칭 재판정 규칙** |

## 실측 결과 정리 (Diana 님 채워넣기)

### 실측 환경

- Airflow 버전: (예: 3.1.7)
- Composer 환경: (예: test-airflow3, dev-dp-project-354904)
- Producer DAG: `berriz_0101_bizberry_hourly_integration`
- Downstream DAG: `berriz_bizberry_downstream_demo_integration`
- 실측 시각: 2026-07-24 __:__ KST

### 발견한 사실 (요약)

- [ ] Clear + Rerun 은 downstream 을 트리거하는가?
- [ ] 같은 dag_run 을 두 번 clear+rerun 하면 downstream 도 두 번 뜨는가? (dedup 여부)
- [ ] Multi-select clear vs 순차 clear 차이가 있는가?
- [ ] Backfill 재실행 시 downstream 파급 규모는?
- [ ] Manual trigger 재실행 시 downstream 트리거 방식은?
- [ ] Downstream 이 이미 실행되고 완료된 상황에서 producer 재실행 시 재트리거되는가?
- [ ] `triggering_asset_events` 의 `source_run_id` 형식은 (특히 clear+rerun vs original run 사이 차이)?

### 지금 진행 중인 자연 실험 (2026-07-24)

**상황**:
- 과거 producer dag_run 을 clear+rerun 했음
- 마침 정기 스케줄 시각이 돌아와서 이번 시간 dag_run 도 새로 시작
- **Producer 두 개가 동시에 실행 중** (재실행 + 신규)

**관찰 포인트** (지금 봐야 할 것):

1. **두 producer 가 끝나는 순간 downstream 이 몇 개 뜨는지**
   - 각자 별개 dag_run 2개 → event 매칭이 producer run 단위
   - 하나로 뭉친 dag_run 1개 (2개 event 다 consume) → 스케줄러 batching 이 그 사이에 낀 것
   - `max_active_runs=1` 인 downstream 특성상 뭉침 가능성 높음

2. **Downstream 의 `triggering_asset_events` 안 event 개수**
   - Producer 두 개 → asset event 도 두 개 (asset 당). AND 조건이면 총 4개일 수도
   - 로그로 확인: downstream 의 첫 task 에 `print(context["triggering_asset_events"])` 있으면 볼 수 있음

3. **Producer 두 dag_run 이 완료된 순서와 downstream 트리거 시각 대조**
   - Producer #1 (재실행분) 이 먼저 끝났으면 → downstream 1번 뜨고, #2 끝나기 전이면 대기
   - Producer 두 개가 거의 동시에 끝났으면 → 뭉쳐서 1번

### 예상과 다른 결과

(예상하지 못한 동작이 있으면 여기 기록)

### 스크린샷 / 로그

(UI 스크린샷, `airflow tasks list-mapped ...` 결과 등)

## 3.2/3.3 Partitioned Assets 에서의 예상 동작 (미실측)

Partition_key 가 first-class 가 되면 재실행 동작이 아마 달라짐:

**시나리오 재해석 (예상)**:

| 시나리오 | 3.1.7 (현재 예상) | 3.2/3.3 (partitioned assets) |
|---|---|---|
| Clear + Rerun 1회 | Downstream 1회 트리거 | 같은 partition_key → dedup? or re-fire? |
| 같은 dag_run 두 번 clear+rerun | 2회 트리거 | Partition_key 기준 dedup 이면 1회 |
| Backfill 5시간치 | 5개 event → downstream batching | 5개 partition_key → 각자 downstream (partition 별 dag_run) |
| Downstream 이 이미 그 partition 처리한 후 producer 재실행 | 새 event 로 재트리거 (예상) | Partition consumed 상태면 dedup? 재트리거? |

**이 표를 실측 채워 넣는 게 3.2/3.3 이관 결정의 핵심 근거**.

## Story 팀 설계에 미치는 영향

### 시나리오별 대응 지침 (실측 확정 후 확정 예정)

**A. Downstream 이 재트리거 되는 게 정상적으로 예상되고 문제 없는 경우**
- 별도 처리 필요 없음
- Producer 재실행 = 명시적인 재처리 의도로 간주

**B. Downstream 이 재트리거 되면 안 되는 경우** (예: 알림 발송 downstream)
- **옵션 1**: Stamp task 조건부 emit — `skip_on_run_type=["backfill"]` 로 backfill 은 억제
- **옵션 2**: Downstream 에서 `triggering_asset_events[...].source_dag_run.run_type` 검사 후 skip
- **옵션 3**: Downstream DAG 을 재실행 중 pause

**C. Downstream 이 뭉쳐서 뜨는 게 문제인 경우** (원하는 개수보다 적게)
- `max_active_runs` 를 줄임
- 또는 partition 별 처리를 dynamic task mapping 으로

**D. Downstream 이 dedup 되어 안 뜨는 경우**
- Producer 가 재실행 시 새 event 를 확실히 emit 하도록 확인
- Partition_key 재발행 시 offset/timestamp 를 key 에 포함 (MDL 스타일)

### 시연 (Story 팀 chapter 3 대비)

Chapter 3 에서 "backfill 억제" 를 시연할 때, 실측 결과에 따라 시나리오 조정:
- Backfill 억제가 자연스러운 케이스면 → `skip_on_run_type=["backfill"]` 시연
- Clear+rerun 이 흔한 케이스면 → run_type 은 `scheduled` 라서 다른 조건 필요
- Manual trigger 는? run_type=manual 인지 확인

## 관련 문서

- [[2_문제_정의]] — 조건부 emit 필요성 원리
- [[1_동작_원리]] — Event batching / dag_run 생성 로직
- [[4_해결_패턴]] — Pattern A 의 `AirflowSkipException` 게이트
- [[dataset_test]] — MDL 팀의 재수집 시나리오 (partition_key + offset 조합)

## TODO

- [ ] 실측 완료 후 표들 채워 넣기
- [ ] 예상과 다른 결과 있으면 [[1_동작_원리]] 문서 수정
- [ ] Story 팀 stamp task 설계에 반영
- [ ] 3.2/3.3 partitioned assets 실측 시 이 시나리오들 재검증
- [ ] Provider 확장 제안 ([[7_Provider_확장_제안]]) 의 `KakaoAssetStampOperator` API 에 `skip_on_run_type` 파라미터 검증 근거로 사용
