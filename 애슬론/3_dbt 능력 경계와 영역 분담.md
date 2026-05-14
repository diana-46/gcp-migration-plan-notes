---
title: "dbt 능력 경계와 영역 분담"
status: draft
tags:
  - athlon
  - dbt
  - airflow
  - lineage
  - decision
created: 2026-05-14
updated: 2026-05-14
---

# dbt 능력 경계와 영역 분담

> dbt가 도입되면 기존 athlon의 모든 액션을 dbt로 옮길 수 있을까? **아니다.** 어디까지가 dbt이고 어디부터가 Airflow operator인지, 그리고 두 영역을 athlon이 어떻게 묶는지에 대한 분석.
>
> 결론을 한 줄로: **"dbt가 못 하는 게 거의 없지만, 하지 말아야 할 영역은 있다. 그리고 둘 사이를 잇는 lineage는 dbt 혼자서는 절대 못 그린다."**

## 1. 큰 그림: dbt vs non-dbt

```
┌─────────────────────────────────────────────────────────────────┐
│  dbt 영역 (SQL 변환 중심)                                         │
│  ────────────────────────                                         │
│  ✓ BQ 안에서 일어나는 모든 변환                                     │
│  ✓ BQ ↔ GCS export/import (EXPORT DATA / LOAD DATA)              │
│  ✓ 데이터 품질 테스트, 문서화, 모델 내부 lineage                     │
│  ✓ Incremental / SCD                                              │
│  ✗ (가능하지만 안 함) 외부 API 호출, sensor, 알림                   │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  Airflow operator 영역 (오케스트레이션·외부 통신·sensor)             │
│  ──────────────────────                                          │
│  ✓ 스케줄 / 트리거 / 재시도 / SLA / 알림                            │
│  ✓ Sensor (외부 이벤트 대기) ← dbt가 절대 못 함                     │
│  ✓ Extract (외부 시스템에서 데이터 가져오기)                         │
│  ✓ 외부 시스템 통신 (사내 API, Slack 등)                            │
│  ✓ dbt run 자체를 task로 실행                                       │
└─────────────────────────────────────────────────────────────────┘

         ↑                                            ↑
         └──── ActionGroup (athlon이 묶는 단위) ──────┘
              + DataHub 통합 lineage
```

## 2. dbt가 "실은 할 수 있는" 것 (한계가 좁다)

저렴하게 가능한 케이스:

| 케이스 | dbt가 어떻게 가능한가 |
|---|---|
| BQ → GCS export | `EXPORT DATA OPTIONS(uri='gs://...', format='PARQUET') AS SELECT ...` — 순수 SQL |
| GCS → BQ load | `LOAD DATA INTO ... FROM FILES (uris=['gs://...'])` — SQL |
| 임의 GCS 파일 조작 | **dbt Python model** (Dataproc Serverless) — `gcsfs` 등으로 가능 |
| 외부 API 호출 | **BigQuery Remote Function** (Cloud Run wrapper) 호출 또는 Python model |
| Pub/Sub 발행 | Remote Function / Python model |
| HTTP 추출 | Python model |
| 표준 BQ insert / merge / partition 관리 | dbt 매크로 |

→ dbt Python model + BigQuery Remote Function + on-run hook 조합으로 **이론상 거의 모든 것** 이 dbt 안에서 가능.

## 3. dbt의 진짜 한계 (hard wall)

여기는 진짜 못 함, 워크어라운드도 어색함:

| 한계 | 왜 |
|---|---|
| **Sensor / 외부 이벤트 대기** | dbt는 one-shot 실행 모델. "외부 파일이 도착할 때까지 대기" 같은 건 dbt 패러다임에 안 맞음. Airflow의 `ExternalTaskSensor`, `GCSObjectExistenceSensor`, `deferrable Sensor` 영역 |
| **장기 실행 스트리밍** | dbt는 batch 도구. Dataflow / Streaming 영역 |
| **DAG-level 스케줄 / SLA / 재시도 / 우선순위** | dbt는 cron으로 띄우는 도구. 정교한 오케스트레이션은 orchestrator 영역 |
| **DAG 시각화 (운영자 시점)** | dbt 자체 그래프는 모델 의존성. Airflow의 Grid / Graph view 같은 운영 뷰는 없음 (Cosmos가 해결) |

## 4. 철학적 한계 (가능하지만 안 함)

| 케이스 | 왜 안 함 |
|---|---|
| 외부 API 호출, 사내 통신 | Python model로 가능하지만 dbt의 강점(자동 lineage, 테스트, partial run, 캐시)을 잃음. Dataproc Serverless 비용 추가 |
| 사소한 파일 ops | 같은 이유 |
| 외부 시스템 알림 | 같은 이유 |
| 재시도 정책 / SLA | orchestrator 영역에서 다루는 게 표준 |

## 5. 🚨 수집 → ETL 연결의 부재 (가장 중요한 gap)

### 문제

dbt의 `source()`는 외부 데이터를 **선언만** 함:

```yaml
sources:
  - name: raw
    tables:
      - name: user_log_raw
```

```sql
SELECT * FROM {{ source('raw', 'user_log_raw') }}
```

dbt가 이 선언으로 할 수 있는 것:

- ✅ "이 raw 테이블을 입력으로 쓴다" 표시
- ✅ dbt manifest에 source entity 등록 → DataHub Dataset
- ✅ source → staging → marts lineage

**dbt가 절대 못 하는** 것:

- ❌ 그 `user_log_raw` 테이블이 **어디서 왔는지** (어떤 외부 system, 어떤 extract job)
- ❌ extract 작업의 실행 이력 / 실패 / 지연 정보
- ❌ extract → raw 사이 변환/필터링이 있었는지

→ **dbt는 raw 테이블을 "땅에서 솟아난 것"으로 본다.** 그 이전 chain은 dbt 시야 밖.

### 우리 컨텍스트에서 왜 critical?

우리는 **수집 → ETL 의 chain이 있는** 데이터 플랫폼:

```
[외부 MySQL / 사내 시스템 / API]
     │
     │ athlon-extract operator
     ↓
[BQ raw 테이블]    ← 여기가 dbt 시작점
     │
     │ dbt 모델
     ↓
[BQ staging / marts]
     │
     │ GCS export / Slack notify
     ↓
[외부 consumer]
```

dbt 만으로는 위 그림에서 **첫 번째와 마지막 화살표**가 lineage에 안 나타남.

### 그래서 athlon이 채워야 할 missing edges

```
1. 외부 시스템 → athlon-extract operator (외부 source URN 등록)
2. athlon-extract operator → BQ raw  (lineage edge)
3. BQ marts → GCS export / Slack notify  (downstream edge)
4. 그리고 ActionGroup 단위 lineage (워크플로 단위)
```

→ 이게 **athlon의 진짜 distinguishing value**. dbt + Airflow + DataHub 만으로는 안 됨.

## 6. Custom Operator Triage (4유형)

athlon이 만든 / 만들 custom operator를 4유형으로 분류해서 dbt 흡수 여부 판정:

| 유형 | 예시 | dbt 흡수? | 권장 |
|---|---|---|---|
| **1. SQL/BQ 작업 래퍼** | 특수 옵션 붙은 BQ query, 표준 partition 관리, dry-run 후 실행 패턴 | ✅ 완전 가능 | **dbt 매크로로 흡수** — 모델별 자동 적용 + lineage 살아남 |
| **2. 사내/외부 시스템 통신** | 사내 API, LDAP, Slack 사내 채널, Pub/Sub | △ 가능하지만 X | **Airflow operator 유지** — 재사용성·discoverability·디버깅이 모두 우월 |
| **3. 오케스트레이션 성격** | poll → 조건 분기 → trigger, Sensor류 | ❌ 불가 | **Airflow 유지** — dbt 패러다임 자체와 안 맞음 |
| **4. 데이터 추출 (extract)** | SFTP → GCS, REST API → BQ, MySQL CDC | △ 부분 | **추출은 Airflow, 변환은 dbt** — ELT 정석 분담 |

> 권장 PoC: athlon의 actions_meta 8개 type 각각을 이 매트릭스로 분류하기. 결과는 잔존 actions_meta 목록을 정하는 근거.

## 7. 잔존 actions_meta 예상 매트릭스

Neptune 문서에서 본 actions_meta type 8개 + CUSTOM 의 운명 예상:

| actions_meta type | 운명 | 근거 |
|---|---|---|
| `HIVE_ETL` | **dbt 모델로 흡수** | SQL 변환 그 자체 |
| `BIGQUERY_JOB` | **대부분 dbt** | 특수 옵션은 매크로. 일부 비표준 케이스만 잔존 |
| `GCS_UPLOAD` | **분기**: BQ→GCS는 dbt EXPORT DATA / 순수 파일 이동은 Airflow operator | 두 케이스가 섞여있을 가능성 |
| `EXTRACT` (외부 → BQ raw) | **Airflow operator 유지** | dbt 외부 영역 |
| `SENSOR` | **Airflow 유지** | hard wall |
| `SLACK_NOTIFY` (또는 사내 알림) | **Airflow 유지** | 권장 분리 |
| `CUSTOM` (Diana 팀이 만든 wrapper들) | **케이스별** | 위 4유형 매트릭스로 분류 |
| (기타 alert / kpi 류) | **Airflow 유지** | 대개 2~3유형 |

→ **잔존 actions_meta type 5~8개 → 2~3개로 축소** 예상. 8개 중 ~6개가 dbt로 이동.

## 8. ActionGroup이 두 영역을 묶는 방법

dbt 영역과 Airflow operator 영역이 같이 굴러가려면 athlon이 **단일 운영 단위**를 만들어야 함. 그게 ActionGroup:

```
ActionGroup A: 데이터 수집
  ├─ extract_user_log     (Airflow operator)  ← dbt 모름
  └─ sensor_external_file (Airflow sensor)    ← dbt 모름

ActionGroup B: 변환  ← A 에 의존
  ├─ stg_user_log    (dbt model)              ← dbt-internal
  ├─ daily_user_agg  (dbt model)              ← dbt-internal (ref stg)
  └─ test_user_stats (dbt test)               ← dbt-internal

ActionGroup C: 후처리  ← B 에 의존
  ├─ gcs_export      (dbt EXPORT DATA)        ← dbt 영역
  └─ slack_notify    (Airflow operator)       ← dbt 외 영역
```

ActionGroup의 핵심 속성:

- ActionGroup **안**: 동종/이종 items 자유롭게 (dbt + Airflow 섞임 OK)
- ActionGroup **간**: athlon-defined 의존성 — DataHub로도 push되어 lineage 형성
- ActionGroup 단위 백필 / 단위 lineage / 단위 트리거

## 9. 두 의존성 레이어 공존: dbt-internal deps + ActionGroup deps

dbt가 자체 의존성 그래프를 가지면서 ActionGroup이 또 의존성을 정의 — **두 레이어가 공존**하지만 **충돌하지 않음**:

| 레이어 | 무엇 | 누가 정의 | 누가 실행 |
|---|---|---|---|
| **dbt 의존성** | dbt 모델 간 SQL 의존 (`ref()`, `source()`) | SQL 안에서 자동 | dbt가 topological order로 |
| **ActionGroup 의존성** | "이 묶음 다음에 저 묶음" 워크플로 의존 (이종 묶음 간) | athlon에서 명시 | Airflow가 task edge로 |

### 정상 케이스

ActionGroup 안 dbt 모델들끼리의 ref는 dbt가 알아서 처리. ActionGroup 간 deps는 athlon이 Airflow에 반영.

### 까다로운 케이스: cross-ActionGroup dbt ref

```
ActionGroup B: stg_user_log (dbt)
ActionGroup D: daily_analysis (dbt) ← ref('stg_user_log')  ⚠️ 다른 그룹 참조!
```

athlon이 해야 할 일 3가지 옵션:

| 옵션 | 동작 |
|---|---|
| **A. 자동 감지·강제** | dbt manifest 파싱 → cross-group ref 발견 시 ActionGroup 의존성 자동 추가 |
| **B. 검증** | 사용자가 ActionGroup B → D 의존성을 안 걸어뒀으면 경고 / 저장 거부 |
| **C. 자유** | dbt가 알아서 (런타임 실패 가능) — ❌ 권장 안 함 |

→ **A 또는 B가 정답**. 우리 의사결정 필요.

## 10. ActionGroup → Airflow 변환 패턴 3가지

| 옵션 | 설명 | 장단 |
|---|---|---|
| **1. ActionGroup = 1 Airflow task** | `dbt run --select tag:group_B` 같은 단일 명령 | ✅ 단순, Airflow 그래프 깔끔 / ❌ 모델 단위 재시도·SLA 안 됨 |
| **2. Cosmos (모델 = task 분해)** | 각 dbt 모델이 Airflow task | ✅ 모델별 재시도·SLA·lineage / ❌ task 수 폭증, ActionGroup 단위와 어긋남 |
| **3. ActionGroup = 1 dbt task + N Airflow task** ⭐ | Airflow는 단순, athlon UI는 풍부 | ✅ 양 layer 추상화 분리 / ❌ 모델별 재시도는 dbt retry로 처리 |

→ 우리 케이스에는 **1 또는 3** 이 자연스러움. Cosmos는 ActionGroup 개념과 어긋날 위험.

## 11. DataHub 통합 lineage 전략

세 가지 출처에서 DataHub로 lineage 들어옴:

| 출처              | 도구                                     | 무엇을 보냄                                                          |
| --------------- | -------------------------------------- | --------------------------------------------------------------- |
| **dbt**         | `datahub-dbt` (manifest 기반)            | model → model 의존성, column-level lineage, test, doc, tag         |
| **Airflow**     | `datahub-airflow-plugin` (OpenLineage) | task lineage (inlet/outlet), BQ operator는 자동, custom은 인스트루먼트 필요 |
| **BigQuery 직접** | DataHub BQ ingestion                   | BQ 메타데이터, audit log 기반 query lineage                            |

세 출처가 **BQ 테이블 URN**을 공통 키로 자동 stitching.

### Athlon이 추가로 push해야 할 4가지

| 작업 | 왜 |
|---|---|
| 1. dbt run 후 manifest를 DataHub에 push | dbt 모델 lineage 자동 흡수 |
| 2. Custom operator에 OpenLineage hook 또는 inlet/outlet 등록 | non-dbt task lineage 수집 |
| 3. ActionGroup → DataFlow / DataJob 매핑 push | dbt+Airflow 위 추상화 |
| 4. Cross-group dbt ref 검증 | dbt manifest vs ActionGroup deps 일관성 |

### Custom Operator OpenLineage 인스트루먼트 패턴 3가지

| 패턴 | 설명 | 장단 |
|---|---|---|
| **A. OpenLineage hook 강제** | 모든 custom operator가 `get_openlineage_facets_*` 구현 | ✅ 표준 / ❌ 개발 부담 |
| **B. athlon UI 등록** | `actions_meta`에 `inlets: []` / `outlets: []` 필드 → 사용자가 등록 → athlon이 DataHub push | ✅ UI-native, 일관성 / ❌ 자동성 ↓ |
| **C. Hybrid** ⭐ | 표준 operator는 OpenLineage 자동 / custom은 UI 등록으로 보완 | 현실적 절충 |

## 12. End-to-end lineage 완성형

instrument 끝나면 DataHub에서 한 그래프로 보임:

```
[MySQL: kakaopage_user_db.user_action]
   │  ← athlon extract operator OpenLineage push
   ↓
[BQ: raw.user_action_raw]
   │  ← dbt source()
   ↓
[BQ: stg.stg_user_action]   ← dbt ref()
   │
   ↓
[BQ: marts.daily_user_summary]   ← dbt ref()
   │  ← athlon export operator OpenLineage push
   ↓
[GCS: gs://data-export/daily/...]
   │
   ↓
[다운스트림 분석 시스템]
```

→ "이 GCS 파일이 어디서 왔지?" 질문에 **외부 MySQL까지 한 번에 추적 가능**.

## 13. 그래서 athlon의 미션 (5축 통합)

```
athlon의 미션:

  dbt가 흡수한 영역(SQL 변환) + Airflow operator로 남은 영역(orchestration·sensor·extract)
  을 다음 5축으로 통합 관리하는 플랫폼:

  1. UI            - 단일 화면에서 dbt 모델 + actions_meta + ActionGroup 편집
  2. 실행 그래프    - ActionGroup 단위로 묶고, Airflow에서 task로 변환
  3. 백필          - ActionGroup 단위, dbt vars + non-dbt 파라미터 일관 주입
  4. Git Sync      - dbt project + actions_meta yml 한 레포 형상관리
  5. DataHub Lineage - dbt manifest + Airflow + athlon-등록 edge 통합 그래프
```

각 축의 디테일은 [[1_개요]]의 결정 포인트로.

## 관련 문서

- [[1_개요]] — 위 5축 + 결정 포인트
- [[2_Git 동기화·dbt 전환 계획]] — Confluence 원본 (Git sync 메커니즘)
