# 3. backfill (dbt + Cloud Composer / Airflow 3)

> 백필 일반론은 생략. **dbt + Cosmos + Composer 3 / Airflow 3 통합 관점**에서 Neptune 의 BackfillService 와 무엇이 다른지, 실제 운영에서 무엇을 결정해야 하는지.
> 관련: [[2_schema 관리]], [[애슬론/5_Neptune SQL 변환의 dbt-BigQuery 대체 검토]]


---

## 1. Neptune 의 backfill 동작 (현재)

`api/.../service/neptune/BackfillService.kt` 기준.

### 1-1. 입력과 동작

| 필드                 | 의미                                                         |
| ------------------ | ---------------------------------------------------------- |
| `etlId`            | 대상 ETL                                                     |
| `startDt`, `endDt` | 재실행 날짜 범위 (KST 기준)                                         |
| `maxActiveRuns`    | 동시 실행 task 수 (default 1)                                   |
| `catchup`          | Airflow `catchup=True` 강제 — scheduler 가 missing dates 다 채움 |

처리:
1. 기존 production DAG 을 fork → 새 DAG `data_neptune_backfill_{id}_{uniqueTitle}` 생성
2. Git commit → Jenkins → Airflow 에 배포
3. Airflow scheduler 가 startDt~endDt 범위의 모든 logical_date 에 대해 DAG run 큐잉
4. `maxActiveRuns` 만큼 병렬 실행, 나머지 대기
5. 완료 후 backfill DAG 은 paused 상태 유지 (production DAG 과 분리됨)

### 1-2. 특징

- **별도 DAG fork** — production DAG 과 분리. 백필 진행 중 production 영향 X
- **Airflow 의 catchup 활용** — Airflow 가 scheduling 알아서 처리
- **ETL 단위** — 한 번에 한 ETL 만 backfill
- **데이터 정합** — production 과 동일 SQL 사용 (fork 한 시점의)

### 1-3. 한계

- DAG fork 가 Git/Jenkins 거치는 사이클 (수 분 소요)
- 다중 ETL 의 의존성 있는 backfill 시 운영자가 순서 관리
- 진행 상황은 Airflow UI 에서 봐야 함 (별도 모니터링 없음)
- 백필 후 fork DAG 정리도 수동

---

## 2. dbt + Cosmos + Composer 3 의 backfill 패턴

### 2-1. 핵심 매핑

Neptune 의 backfill 자동화에서 빠진 부분이 다음 셋으로 분해됨:

```
Neptune BackfillService
    ↓ 분해
┌─────────────────────────────────────────┐
│ 1. dbt model 자체 (코드)                │
│    - var('run_date') 등 backfill-able  │
│    - is_incremental() 처리              │
│    - insert_overwrite 로 idempotent    │
├─────────────────────────────────────────┤
│ 2. Airflow DAG (orchestration)          │
│    - data_interval_start → vars 주입    │
│    - schedule + catchup 설정            │
│    - max_active_runs (동시성)           │
├─────────────────────────────────────────┤
│ 3. 백필 trigger 방법                    │
│    - gcloud dags backfill, 또는         │
│    - Python script (vars 다양화)        │
└─────────────────────────────────────────┘
```

### 2-2. dbt model 측 준비

`insert_overwrite` partition 모델은 idempotent 가 보장됨 — 같은 `run_date` 로 여러 번 실행해도 같은 결과. backfill 의 전제 조건.

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partitions=["DATE('" ~ var('run_date', '2026-06-15') ~ "')"]
) }}
SELECT
    DATE('{{ var("run_date") }}') AS create_date,
    ...
FROM {{ source('raw', 'events') }}
{% if is_incremental() %}
WHERE event_ts >= TIMESTAMP('{{ var("run_date") }}')
  AND event_ts <  TIMESTAMP_ADD(TIMESTAMP('{{ var("run_date") }}'), INTERVAL 1 DAY)
{% endif %}
```

→ `is_incremental()` 블록 안에 윈도우 필터 둬야 source 전체 스캔 안 함. 비용 측면 핵심.

### 2-3. DAG 측 준비 (우리 POC DAG 패턴)

```python
DAILY_VARS = {
    "run_date": "{{ data_interval_start.in_timezone('Asia/Seoul').strftime('%Y-%m-%d') }}",
    ...
}

case2 = dbt_taskgroup(
    group_id="case2_plain_daily",
    select=["tag:case2"],
    operator_args_extra={"vars": DAILY_VARS},
)
```

**중요한 점**: vars 가 Jinja 템플릿이라 **각 dag_run 의 `data_interval_start` 가 다르게 렌더링됨**. backfill 시 자동으로 옛 날짜 vars 가 들어감. dbt 모델 코드 변경 없이 백필 가능.

### 2-4. Cosmos 의 backfill 동작

Cosmos `DbtTaskGroup` 은 logical_date 기반의 정상 dag_run 과 backfill dag_run 을 구분하지 않음 — 둘 다 같은 task 들 실행. 단지 Airflow 가 어떤 logical_date 로 trigger 하는지의 차이.

내부:
1. backfill dag_run 시작
2. Cosmos operator 가 `vars` Jinja 렌더 (각 dag_run 의 logical_date 사용)
3. `dbt run --select <model> --vars '<rendered_json>'` 실행
4. dbt 가 incremental partition 단위로 BQ 에 MERGE

→ **모델 변경 없이 backfill 가능**. backfill 자체에 dbt 가 무관심.

### 2-5. 일부 pipeline 만 백필하기

#### 전제 조건

- production DAG 은 여러 pipeline 을 묶어서 운용 (사용자가 어느 DAG 에 넣을지 선택). **DAG 분리 불가**.

#### Neptune 의 패턴 (정확한 동작, `BackfillService.kt` 기준)

production DAG 파일 복제가 아니라 **타겟 ETL 의 action chain 만 떼어서 새 워크플로우를 구성**하는 방식:

```
BackfillProcessor(etlId, startDt, endDt, maxActiveRuns, userId)
    ↓
1. backfill 엔티티 저장 (etlId 1개 지정)
2. 새 Workflow 엔티티 생성 — DAG name: `data_neptune_backfill_{padded_id}_{etl.uniqueTitle}`
   - 이 워크플로우는 타겟 ETL 한 개만 들고 있음
   - type = WorkflowType.BACKFILL
   - catchup = true, maxActiveRuns 적용
3. git.addDag(workflow)   ← 새 DAG 파일을 git neptune-dags 레포에 commit
4. jenkins.syncDags()      ← Jenkins 가 Airflow 환경에 배포
```

즉 production DAG (예: pipeline A + B + C 합본) 은 그대로 있고, backfill 은 **하나의 ETL (예: B) 의 action 만 담은 별개 Python DAG 파일을 새로 생성/배포**.

backfill 후 정리:
- `deleteBackfill` 호출 시 `git.deleteDag(workflow)` + DB 청소 + `jenkins.syncDags()` 로 backfill DAG 파일 삭제

→ Neptune 의 의도: production 영향 0 + 타겟 ETL 만 격리된 환경에서 catchup 으로 일괄 처리.

#### dbt 로 같은 패턴을 만들 때

dbt 에선 model 이 **DAG 와 무관한 자산**이라, "ETL 하나만 떼어 새 DAG 구성" 이 자연스러움.

| 자산 | 위치 | production / backfill 공유 방식 |
|---|---|---|
| 모델 SQL | `models/.../*.sql` | **그대로 공유** — 한 파일을 두 DAG 이 각자 `select=` 로 참조 |
| schema.yml / contract | `models/.../schema.yml` | **그대로 공유** |
| vars 매크로 | helper 모듈 (`common/dbt_presets.py` 등) | 공통 import |
| TaskGroup factory | `dbt_taskgroup()` helper | 공통 import |
| manifest.json | GCS 단일 파일 | **양쪽 DAG 이 같은 manifest 참조** |

→ **dbt 쪽의 진짜 reuse 단위는 model**. backfill DAG 은 모델 SQL/schema 를 복제하지 않고 `select=` 로 가리키기만 함.

#### 패턴: 타겟 pipeline 마다 별도 backfill DAG (Neptune-style)

각 backfill 가능한 pipeline 마다 작은 DAG 파일 하나:

```python
# dags/backfill/dag_backfill_case2.py
from datetime import datetime
from airflow import DAG
from common.dbt_presets import dbt_taskgroup

BACKFILL_VARS = {
    "run_date": "{{ data_interval_start.in_timezone('Asia/Seoul').strftime('%Y-%m-%d') }}",
    # ... production 과 동일
}

with DAG(
    dag_id="dag_backfill_case2",                    # ← Neptune 의 data_neptune_backfill_{id}_{title}
    schedule=None,                                   # 수동 trigger 만
    start_date=datetime(2026, 6, 1),
    catchup=False,                                   # backfill 명령 시 logical_date 범위로 dag_run 만들어짐
    max_active_runs=2,
    tags=["backfill", "neptune", "case2"],
) as dag:

    # production 과 동일 모델, 같은 select. DAG wrapper 만 다름
    dbt_taskgroup(
        group_id="case2_backfill",
        select=["tag:case2"],
        operator_args_extra={"vars": BACKFILL_VARS},
    )
```

호출:
```bash
gcloud composer ... dags backfill -- \
  dag_backfill_case2 \
  --start-date 2026-06-01 --end-date 2026-06-14 \
  --max-active-runs 2
```

→ Neptune 패턴과 1:1 매핑. DAG 파일은 작아서 (10~20 라인) Neptune 처럼 **자동 생성 스크립트**도 쓸 수 있음:

```python
# scripts/generate_backfill_dag.py
def generate_backfill_dag(pipeline_tag: str, output_path: str):
    template = """
    # auto-generated
    from datetime import datetime
    from airflow import DAG
    from common.dbt_presets import dbt_taskgroup
    from common.vars import BACKFILL_VARS

    with DAG(dag_id="dag_backfill_{tag}", schedule=None, ...) as dag:
        dbt_taskgroup(group_id="{tag}_backfill",
                      select=["tag:{tag}"],
                      operator_args_extra={{"vars": BACKFILL_VARS}})
    """
    # render + write + git commit + jenkins (또는 Composer GCS sync)
```

→ Neptune 의 `git.addDag()` + `jenkins.syncDags()` 가 그대로 Composer 의 `gcloud storage cp` + scheduler reparse 로 매핑.

#### 대안 패턴: 하나의 parameterized backfill DAG (conf-driven)

DAG 파일 폭증이 부담이면 단일 backfill DAG + conf 로 target 지정. 단 **Cosmos 의 `select=` 가 DAG parse 시점에 결정되므로 conf 로 동적 변경 어려움**. 대안 두 가지:

1. **BashOperator + dbt CLI** — Cosmos 우회. 가장 유연하지만 Cosmos 의 task 분해 / asset outlet 잃음
   ```python
   BashOperator(
       task_id="dbt_run",
       bash_command="dbt run --select {{ params.model }} --vars '{ run_date: \"{{ ds }}\" }'",
       params={"model": "case2_kp_stat_ticket_use_daily"},
   )
   ```
2. **DAG 안에 모든 pipeline 의 TaskGroup 미리 정의 + ShortCircuitOperator gating** — DAG 안에 case2/case3/... 다 정의되고 conf 로 어느 거 실행할지 결정. UI 의 skipped 표시가 많아짐

→ Neptune 의 "ETL 한 개만 든 깨끗한 DAG" 패턴과 다름. 운영 멘탈 모델 측면에서 **별도 DAG 파일 패턴이 더 Neptune-friendly**.

#### Neptune fork 와의 진짜 차이

|                 | Neptune                                                  | dbt + backfill DAG                                      |
| --------------- | -------------------------------------------------------- | ------------------------------------------------------- |
| 백필 단위           | ETL 한 개의 action chain                                    | model 한 개 (또는 select tag 로 묶인 그룹)                       |
| backfill DAG 생성 | `BackfillService` 가 워크플로우 + git commit + jenkins sync 자동 | 운영자가 DAG 파일 작성 (또는 자동 생성 스크립트)                          |
| backfill 후 정리   | `deleteBackfill` 호출 시 git 에서 DAG 삭제 + jenkins sync       | 수동 (GCS 에서 DAG 파일 삭제) 또는 영구 상주로 재사용                     |
| 코드 snapshot     | backfill DAG 안의 action 정의는 fork 시점 SQL 사본                | **공유** — production 코드 변경이 backfill 에도 영향 ⚠️            |
| pipeline 변경 시   | production + 진행 중 backfill 둘 다 수정 필요                     | production 만 수정. backfill DAG 은 selector 만 들고 있어서 자동 반영 |

⚠️ **코드 snapshot 차이**: Neptune backfill DAG 은 action 들의 SQL 을 자체 보유 (fork 시점 캡처). dbt 는 production 과 같은 model 파일 참조 → backfill 중 production 코드가 바뀌면 backfill 결과도 바뀜.

진짜로 격리가 필요하면:
- `dbt clone` (1.6+) 으로 모델 임시 복제 후 backfill DAG 이 그 복제본 참조
- manifest.json 을 backfill 전용 GCS 경로에 snapshot → backfill DAG 의 `ProjectConfig.manifest_path` 가 그걸 가리킴
- git tag/ref 로 시점 고정 후 별도 경로에 sync

대부분 운영에선 "백필 중 production 코드 변경 금지" 정책으로 충분. fork-level snapshot 은 over-engineering 인 경우가 많음.

### 2-6. 동시성 제어

| 레벨 | 어디서 | 권장 |
|---|---|---|
| DAG run | DAG 의 `max_active_runs` | 2~3 (BQ 슬롯 부담) |
| Task | TaskGroup 의 `max_active_tis_per_dag` | 모델 단위 동시성 |
| BQ | reservation slots | data team 정책 |
| Worker pod | Composer worker_concurrency | 기본값 (12 정도) |

⚠️ **K8s executor 와의 상호작용**: case3 처럼 K8s 로 보낸 task 는 pod 마다 새로 뜨므로 worker 풀 영향은 적지만 K8s 자원 영향 있음.

---

## 3. backfill 실행 방법 (Composer 3 기준)

### 3-1. 단일 날짜 (예외 케이스 수정)

Airflow UI:
```
Trigger DAG w/ config:
  Logical date: 2026-06-15 00:00:00
  Conf: {}    ← 우리 DAG 은 data_interval_start 에서 vars 유도하므로 conf 불필요
```

### 3-2. 범위 backfill (사고 복구 / 신규 모델 초기화)

```bash
gcloud composer environments run test-airflow3 \
  --location asia-northeast3 \
  dags backfill -- \
    dag_neptune_poc_daily \
    --start-date 2026-06-01 \
    --end-date 2026-06-14 \
    --max-active-runs 2
```

→ Airflow scheduler 가 14개 dag_run 큐잉, `max_active_runs=2` 만큼 병렬.

### 3-3. 커스텀 Python (vars 동적 조절 필요할 때)

```python
# 예: 매월 1~5일만 backfill, 또는 특정 vars 조합 다양화
from airflow.api.client import get_current_api_client
from datetime import datetime, timedelta

dates = [datetime(2026, m, 1) for m in range(1, 7)]
for d in dates:
    trigger_dag(
        dag_id='dag_neptune_poc_daily',
        logical_date=d,
        conf={'override_var': 'special_value'},
    )
```

### 3-4. dbt-only backfill (DAG 거치지 않고)

Composer 가 아닌 다른 환경에서 직접:
```bash
for date in 2026-06-01 2026-06-02 ... 2026-06-14; do
  dbt run --select case2_kp_stat_ticket_use_daily \
          --vars "{run_date: '$date'}"
done
```

→ 사고 복구 시 빠른 임시 처리. 운영 표준은 Airflow 경로.

---

## 4. 실측 — case2 옛 날짜 backfill (2026-06-18 PoC)

§ 2-5 의 패턴으로 case2 백필 DAG 작성 → Composer 배포 → UI 백필 실행 → BQ 확인까지.

### 4-1. 백필 DAG 작성 (변경 사항)

| 파일 | 변경 |
|---|---|
| `dags/dag_backfill_case2.py` | **신규 생성**, ~40 라인 |
| dbt 모델 / schema.yml / manifest / production DAG | **무변경** |

dbt 자산은 0 변경. backfill DAG 은 `select=["tag:case2"]` 로 production 과 같은 모델을 가리킬 뿐.

```python
# dags/dag_backfill_case2.py (요약)
RUN_DATE_KST = "{{ data_interval_start.in_timezone('Asia/Seoul').strftime('%Y-%m-%d') }}"
CASE2_VARS = {"run_date": RUN_DATE_KST}

with DAG(
    dag_id="dag_backfill_case2",
    schedule="@daily",                  # UI 백필 활성화에 필요 (interval 정의)
    start_date=datetime(2026, 6, 1),
    catchup=False,                      # 자동 실행 막음
    is_paused_upon_creation=True,       # 배포 즉시 paused
    max_active_runs=2,
    tags=["backfill", "neptune", "case2"],
) as dag:
    dbt_taskgroup(
        group_id="case2_backfill",
        select=["tag:case2"],
        operator_args_extra={"vars": CASE2_VARS},
    )
```

### 4-2. 실행

Airflow UI:
1. `dag_backfill_case2` paused 토글 해제
2. Backfill 버튼 클릭 → 날짜 범위 + max_active_runs 지정
3. Trigger

각 dag_run 의 vars:

| dag_run | data_interval_start (UTC) | 렌더된 run_date (KST) |
|---|---|---|
| 1 | 2026-06-10 00:00 | `"2026-06-10"` |
| 2 | 2026-06-11 00:00 | `"2026-06-11"` |
| ... | ... | ... |

Cosmos 가 각 dag_run 의 context 로 Jinja 새로 렌더 → 각각 다른 `run_date` 로 dbt 호출.

### 4-3. 결과

BQ 파티션 확인:
```sql
SELECT partition_id, total_rows, last_modified_time
FROM `dev-dp-project-354904.dbt_test.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'case2_kp_stat_ticket_use_daily'
ORDER BY partition_id;
```

→ 백필 범위의 partition_id 들이 신규 생성, 기존 파티션은 영향 없음. 각 파티션의 `revenue=5000` (SQL 의 하드코딩 그대로). pre_hook 의 `ALTER TABLE ADD COLUMN IF NOT EXISTS revenue` 는 매 dag_run 마다 실행되지만 idempotent 라 무해.

### 4-4. 학습 요약

| 항목 | 결과 |
|---|---|
| dbt 자산 변경 | **0** — model / schema.yml / manifest 모두 그대로 |
| 새 코드 | 백필 DAG 파일 1개 (~40 라인) |
| 운영자가 신경 쓸 vars 처리 | 0 — Jinja 매크로가 각 dag_run 마다 자동 렌더 |
| 백필 후 production 영향 | 0 — 별개 DAG 파일 |
| Neptune 식 격리 달성 | ✅ — production DAG 그대로, backfill 은 별개 DAG 에 case2 만 |

---

## 5. Neptune ↔ dbt+Composer 비교

| 항목 | Neptune | dbt + Composer 3 |
|---|---|---|
| 백필 발동 | UI 에서 BackfillService API 호출 | Airflow UI / gcloud / Python |
| DAG 격리 | 별도 fork DAG (`data_neptune_backfill_{id}`) | 같은 DAG 의 별도 dag_run (run_type 으로 구분) |
| 진행 추적 | Airflow UI 의 fork DAG | 같은 DAG 의 backfill dag_run 들 |
| 동시성 제어 | `maxActiveRuns` 필드 | `max_active_runs` + worker concurrency |
| 모델 idempotency | SQL 사용자 책임 | `insert_overwrite` 가 강제 보장 |
| 백필 후 정리 | fork DAG 수동 cleanup | 자동 — 추가 객체 없음 |
| 신규 컬럼 backfill | 새 컬럼은 옛 파티션에서 NULL (Neptune 이 옛 파티션 안 건드림) | `insert_overwrite` 가 통째로 재기록 → 옛 파티션도 새 SQL 결과로 채움 |
| 비용 가시성 | 별도 모니터링 | BQ INFORMATION_SCHEMA 로 backfill dag_run 의 slot/bytes 추적 |

**dbt 쪽이 우월한 부분**:
- DAG 격리 안 해도 됨 — 같은 DAG, 다른 dag_run 으로 자연스러움
- 모델 자체가 idempotent (Neptune 은 사용자 SQL 의 신뢰)
- 신규 컬럼 backfill 이 자동 (옛 파티션 통째로 재계산)
- 정리 작업 0

**Neptune 쪽이 우월한 부분**:
- 백필 추상화가 더 높음 — UI 한 번 클릭
- production DAG 영향 0 (fork 라서 격리)
