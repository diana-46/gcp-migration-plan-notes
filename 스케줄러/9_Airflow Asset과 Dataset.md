---
title: "Airflow Asset과 Dataset (진화)"
status: draft
tags:
  - airflow
  - 스케줄러
  - asset
  - dataset
created: 2026-05-14
updated: 2026-05-14
---

# Airflow Asset과 Dataset (진화)

> Airflow 2.4 에서 도입된 **Dataset** 이 Airflow 3 에서 **Asset** 으로 진화. 단순 이름 변경이 아니라 패러다임 확장. 우리 Asset-Centric 디자인의 기반.

## 한 줄 핵심

> **Dataset = 의존성 표시용 라벨** (URI 만 있는 단순 객체)
> **Asset = 데이터 자산 객체** (메타데이터·watcher·alias·decorator 등 풍부한 컨셉)

## 진화 타임라인

| 버전 | 출시 | 주요 추가 |
|---|---|---|
| **2.4** (2022-09) | Dataset 도입 | URI 기반 dependency primitive. `outlets=[Dataset(...)]`, `schedule=[Dataset(...)]` |
| **2.10** (2024-08) | Dataset 개선 | Dataset alias, conditional dataset scheduling 일부 |
| **3.0** (2025-04) | **Asset 으로 개명·확장** | AssetWatcher, AssetAlias, @asset decorator, extras, 표준 URI 스키마 |

## 큰 그림 차이

```
Airflow 2.4 (Dataset):

  [DAG A] ──produce──→ [Dataset("s3://...")] ──trigger──→ [DAG B]
                              │
                              │ 단순 URI 라벨
                              ↓
                          " 이 DAG 끝나면 갱신됐다고 표시 "

────────────────────────────────────────────────────────────

Airflow 3 (Asset):

  [DAG A] ──produce──→ [Asset("bq://...", extras={...})] ──trigger──→ [DAG B]
                              │
                              ├─ extras (metadata)
                              ├─ AssetAlias (별칭)
                              ├─ AssetWatcher (외부 이벤트 감지)
                              └─ @asset decorator (선언적 정의)

  [외부 시스템 이벤트] ──AssetWatcher──→ [Asset 갱신] ──trigger──→ [DAG]
```

## 비교 매트릭스

| 측면 | Dataset (2.4+) | Asset (3.0) |
|---|---|---|
| 이름 | `Dataset(...)` | `Asset(...)` |
| 트리거 메커니즘 | DAG → Dataset → DAG (체인) | DAG + 외부 이벤트 + 스케줄 |
| 메타데이터 | URI + name only | `extras={...}` (owner, tags, schema 힌트 등) |
| 별칭 | ❌ (2.10에 alias 일부) | ✅ `AssetAlias("...")` |
| 외부 이벤트 감지 | ❌ | ✅ `AssetWatcher(trigger=...)` |
| 선언적 데코레이터 | ❌ | ✅ `@asset` |
| URI 스키마 권고 | 느슨 | 표준 권고 (`bigquery://...`, `s3://...` 등) |
| Task-less 자산 정의 | ❌ | ✅ `@asset` 만으로 가능 |
| Group / 도메인 | ❌ | ✅ `group="..."` |

## 새 기능 5가지 (3.0)

### 1. Asset (가장 기본)

```python
# Airflow 2.4 — Dataset
from airflow import Dataset
my_data = Dataset("s3://bucket/path.parquet")

# Airflow 3 — Asset (개명 + extras)
from airflow.sdk import Asset
my_data = Asset(
    "s3://bucket/path.parquet",
    name="user_action_daily",
    group="user_domain",
    extras={
        "owner": "data-platform",
        "schema": "v2",
        "tags": ["daily", "production"],
    },
)
```

→ **단순 URI 만이 아니라 metadata 가 자산에 묶임**.

### 2. AssetAlias (별칭 / 환경 분리)

```python
from airflow.sdk import Asset, AssetAlias

# 환경별 alias
prod_user = Asset("bigquery://prod.marts.daily_user")
dev_user  = Asset("bigquery://dev.marts.daily_user")

daily_user_alias = AssetAlias("daily_user")  # 추상 별칭

# 환경에 따라 alias 가 다른 실제 asset 을 가리킴
```

→ Profile/환경 분리할 때 자연스러움. 같은 모델, 다른 매장.

### 3. AssetWatcher (외부 이벤트 감지) ⭐

이게 가장 큰 변화. **외부 시스템 이벤트가 자산 갱신을 트리거**:

```python
from airflow.sdk import Asset, AssetWatcher
from airflow.providers.google.cloud.triggers.pubsub import PubSubMessageTrigger

# Pub/Sub 메시지가 오면 이 asset 이 갱신됐다고 표시
user_signup = Asset(
    "pubsub://my-project/user-signup-events",
    watchers=[
        AssetWatcher(
            name="user_signup_arrival",
            trigger=PubSubMessageTrigger(
                project_id="my-project",
                subscription="user-signup-sub",
            ),
        )
    ],
)

# 이 Asset 이 갱신되면 자동 트리거되는 DAG
with DAG("process_user_signup", schedule=[user_signup]) as dag:
    ...
```

지원되는 trigger 예시:
- **FileTrigger** — 파일 도착 감지
- **PubSubMessageTrigger** — Pub/Sub 메시지
- **TimeDeltaTrigger** — 시간 경과
- 커스텀 Trigger 작성 가능

→ **기존 polling Sensor 일부 대체**. 폴링 비용 없고, 이벤트 즉시 반응.

### 4. @asset 데코레이터 (선언적 정의)

```python
from airflow.sdk import asset, Asset

@asset(
    schedule="0 6 * * *",
    extras={"owner": "data-platform"},
)
def daily_user_summary():
    """일별 사용자 통계."""
    # 이 함수가 실행되면 asset 이 갱신됨
    # outlets 명시 안 해도 함수 자체가 outlets
    return run_dbt_model("daily_user_summary")
```

→ **task-less 정의**. DAG 안에 task 안 만들고 자산 자체로 선언. Dagster 스타일.

### 5. URI 스키마 표준화

이전엔 `Dataset("anything")` 가능했지만, Airflow 3 는 권고 스키마:

| 스키마 | 용도 | 예시 |
|---|---|---|
| `bigquery://` | BigQuery 테이블 | `bigquery://prod.marts.daily_user` |
| `gs://` / `s3://` | object storage | `gs://reports/daily/` |
| `mysql://` / `postgres://` | RDB | `mysql://prod.kakaopage_user.action` |
| `pubsub://` | Pub/Sub | `pubsub://my-project/topic` |
| `kafka://` | Kafka | `kafka://broker/topic` |
| `external://` | 일반 외부 시스템 | `external://api.kakaoent/v1/users` |

→ DataHub Dataset URN 과 거의 동일 형식. **stitching 자동.**

## 마이그레이션 (Dataset → Asset)

Airflow 3 에서 `Dataset` 은 deprecation warning 으로 backward-compat:

```python
# 작동은 함 (deprecation warning)
from airflow import Dataset
my_data = Dataset("s3://...")

# 권장 (Asset 으로 갱신)
from airflow.sdk import Asset
my_data = Asset("s3://...")
```

기존 코드 작동에는 영향 적지만 새 코드는 `Asset` 권장. 2.10 → 3 마이그레이션 시 자동 변환 가능 (단순 import 교체 수준).

## 우리 케이스 활용

### A. dbt 모델 자산화

```python
# Cosmos 또는 athlon-compiler 가 자동 emit
stg_user_action = Asset(
    "bigquery://${BQ_PROJECT}.staging.stg_user_action",
    group="user_domain",
    extras={"dbt_model": "stg_user_action"},
)
```

### B. Extract — AssetWatcher 활용 가능 영역

```python
# 외부 MySQL 이 CDC 이벤트 발행하면 자동 갱신
user_action_raw = Asset(
    "bigquery://${BQ_PROJECT}.raw.user_action_raw",
    watchers=[
        AssetWatcher(
            name="kakaopage_user_cdc",
            trigger=PubSubMessageTrigger(...),  # CDC 이벤트 도착
        )
    ],
)
```

### C. AssetAlias 로 환경 분리

```python
daily_summary_alias = AssetAlias("daily_user_summary")
# Profile 에 따라 실제 Asset 으로 resolve
# dev → Asset("bigquery://dev.marts.daily_user")
# prod → Asset("bigquery://prod.marts.daily_user")
```

### D. Sensor 일부 폐기

| 기존 Airflow 2 패턴 | Airflow 3 Asset |
|---|---|
| `GCSObjectExistenceSensor` polling | `AssetWatcher(trigger=GCSFileTrigger(...))` |
| `ExternalTaskSensor` | `schedule=[Asset("...")]` |
| `BigQueryTablePartitionExistenceSensor` | AssetWatcher (BQ 변경 감지) or 폴링 유지 |

→ 폴링 비용 ↓, 이벤트 기반 즉시 반응.

## 우리 Asset-Centric 디자인과의 매핑

[[../애슬론/4_Asset-Centric 아키텍처 안]] 의 4개 엔티티가 Airflow 3 Asset 과 1:1 매핑:

| 우리 모델 | Airflow 3 |
|---|---|
| `Asset.urn` | `Asset("...")` URI |
| `Asset.inputs` | `inlets=[Asset(...)]` |
| `Producer` 실행 | task 의 `outlets=[Asset(...)]` |
| `AssetGroup` | DAG (또는 `@asset` group) |
| Profile | `AssetAlias` + env vars |
| Sensor producer | `AssetWatcher` |
| Inter-DAG 의존성 | `schedule=[Asset]` (ExternalTaskSensor 폐기) |

→ Airflow 3 Asset 위에 athlon Asset-Centric 디자인이 자연스럽게 올라탐.

## 한계 / 주의사항

| 한계 | 의미 |
|---|---|
| **AssetWatcher trigger 종류 한정적** | 모든 외부 이벤트 source 가 지원되는 건 아님. 커스텀 작성 필요할 수도 |
| **메타데이터 표현력 제한** | `extras` 는 dict 이지만 깊은 schema 정보는 DataHub 가 더 강력 |
| **UI 시각화 부실** | Airflow Datasets/Assets 페이지는 sparse. 진짜 카탈로그는 DataHub |
| **Asset 간 다양한 관계 표현 어려움** | "produced by", "consumed by" 외 다른 의미 표현 어려움 |
| **Column-level lineage X** | dbt 가 manifest 로 column lineage 제공. Airflow Asset 은 dataset 단위만 |

→ Asset 은 **dependency / trigger primitive** 로 강력. **카탈로그 / 풍부한 메타데이터** 는 DataHub 위임.

## Composer 에서의 활용

- **Composer 3** = Airflow 3 = Asset 사용 가능
- **Composer 2** = Airflow 2 = Dataset 만 가능 (이미 EOL 흐름)
- AssetWatcher 의 GCP Trigger (PubSub / GCS / BQ) 지원 확인 필요 — Composer 3 환경에서 PoC

## PoC 항목

- [ ] 로컬 Airflow 3 docker-compose 에서 `Asset` + `outlets`/`inlets`/`schedule=[Asset]` 동작 확인
- [ ] `AssetWatcher` 1개 작동 (FileTrigger 가 가장 쉬움)
- [ ] `AssetAlias` 로 환경 분리 패턴 검증
- [ ] Composer 3 sandbox 에서 PubSubMessageTrigger 동작
- [ ] DataHub URN ↔ Airflow Asset URI 자동 stitching 검증

상세 PoC: [[../애슬론/PoC/README]] Step 1

## 미확정 / 확인 필요

- AssetWatcher 의 production maturity (Airflow 3.0 GA 후 1년 시점, 안정성 검증 필요)
- 커스텀 Trigger 작성 부담 / 우리 시스템 (사내 API, Kafka 등) 지원 여부
- AssetAlias 의 Composer 3 환경 호환성

## 관련 문서

- [[1_개요]] — 스케줄러 메인 결정
- [[6_Airflow 2 vs 3 비교]] — 버전 결정 (Asset 도 신규 기능 중 하나로 언급됨)
- [[../애슬론/4_Asset-Centric 아키텍처 안]] — Asset 위에 올라타는 우리 디자인
- [[../애슬론/PoC/README]] — Asset 직접 만져보기
