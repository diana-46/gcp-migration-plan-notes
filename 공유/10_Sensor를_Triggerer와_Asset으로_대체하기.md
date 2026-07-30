# 10. Sensor 를 Triggerer / Asset 으로 대체하기 (DE 실전 가이드)

> Airflow 3 로 오면서 sensor 를 그대로 두면 자원 낭비. 두 가지 표준 대체 방식을 알아두면 됨.
> - 시간·조건 대기 → **Deferrable** (`deferrable=True`)
> - 다른 DAG 완료 대기 → **Asset schedule** (`schedule=[Asset(...)]`)

## 왜 바꿔야 하나

Sync sensor 는 worker slot 을 계속 점유. Composer 3 은 DCU (vCPU + RAM 시간) 요금이라 sensor
가 오래 걸릴수록 요금 그대로 늘어남. Deferrable / Asset 은 worker slot 안 잡음.

---

## 시나리오 1: Poke sensor → Deferrable

### Before (Airflow 2 스타일 — worker slot 점유)

```python
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor

wait_source = BigQueryTableExistenceSensor(
    task_id="wait_source",
    project_id="dev-dp-project-354904",
    dataset_id="raw",
    table_id="user_action",
    poke_interval=60,        # 매 60초 worker 가 깨서 확인
    mode="reschedule",       # 대기 중 slot 놓지만, 재시작 오버헤드 있음
    timeout=3600,
)
```

문제:
- `poke_interval` 마다 worker 가 깸 → 60초에 한 번씩 slot 잡음
- `mode="reschedule"` 도 완전 free 는 아님 (재스케줄 오버헤드)
- 대기 1시간 = worker 시간 60분 소진 (요금에 그대로)

### After (Airflow 3 — Triggerer 위임)

```python
wait_source = BigQueryTableExistenceSensor(
    task_id="wait_source",
    project_id="dev-dp-project-354904",
    dataset_id="raw",
    table_id="user_action",
    deferrable=True,          # ← 이거 하나. Triggerer 로 위임
    timeout=3600,
)
```

동작:
1. Task 시작 → Triggerer 로 위임 → **worker slot 즉시 해제**
2. Triggerer 가 async 로 조건 감시 (event loop 하나가 수백 개 trigger 감시 가능)
3. 조건 만족 → worker 다시 잡아 후속 처리

이득:
- Worker slot 안 잡음 → 대기 1시간 = worker 시간 거의 0
- Composer 는 Triggerer 를 자동 provisioning → 별도 인프라 세팅 X
- Poke interval 튜닝 없음

## 지원 sensor 목록 (자주 쓰는 것)

| Sensor | Deferrable 지원 |
|---|---|
| `BigQueryTableExistenceSensor` | ✅ |
| `BigQueryTablePartitionExistenceSensor` | ✅ |
| `GCSObjectExistenceSensor` | ✅ |
| `GCSObjectsWithPrefixExistenceSensor` | ✅ |
| `TimeDeltaSensor` | ✅ (`TimeDeltaSensorAsync`) |
| `DateTimeSensor` | ✅ (`DateTimeSensorAsync`) |
| `ExternalTaskSensor` | ✅ (하지만 **Asset schedule 을 더 권장** — 아래 시나리오 2 참조) |
| `HttpSensor` | ✅ |

**`deferrable=True` 안 되는 sensor**: 사내 custom sensor (`AthlonQuerySensor` 등) 는 async 지원 필요.
Provider 팀에 요청 or `poke_interval` 을 최대한 크게 잡기.

---

## 시나리오 2: ExternalTaskSensor → Asset schedule

Sensor 로 다른 DAG 완료를 기다리는 패턴 자체를 없앰 (이벤트 기반).

### Before (polling)

```python
from airflow.sensors.external_task import ExternalTaskSensor

with DAG(
    dag_id="daily_report",
    schedule="0 8 * * *",
    ...
) as dag:
    wait_upstream = ExternalTaskSensor(
        task_id="wait_upstream",
        external_dag_id="upstream_etl",
        external_task_id="final_task",
        mode="reschedule",
        execution_delta=timedelta(hours=2),   # timing 맞춰야 함, 실수 여지
    )
    process = PythonOperator(task_id="process", ...)
    wait_upstream >> process
```

문제:
- Polling → worker slot 낭비
- `execution_delta` 계산 실수 시 영원히 대기
- 두 DAG 사이 timing 이 코드에 박힘 (upstream 스케줄 바뀌면 여기도 수정)
- 두 DAG 이 서로 참조 (upstream DAG 이름 하드코드)

### After (Asset 이벤트)

Upstream DAG 의 task 가 outlet 으로 asset 을 emit → Downstream DAG 이 그 asset 을 schedule 로 subscribe → upstream 완료 시 downstream 자동 트리거.

**Upstream** (producer):
```python
from airflow.sdk import Asset

USER_ACTION_TABLE = Asset(
    "bigquery/dev-dp-project-354904/marts/daily_user_summary"
)

with DAG(dag_id="upstream_etl", schedule="0 6 * * *", ...) as dag:
    build_mart = BigQueryInsertJobOperator(
        task_id="build_mart",
        configuration={...},
        outlets=[USER_ACTION_TABLE],   # ← 이 task 성공 시 asset emit
    )
```

**우리 cosmos 케이스**: dbt task 는 outlet 을 **자동 emit** (cosmos 가 처리). 별도 설정 X.
예: `dbt_userpost` task group 의 mart task 완료 시
`Asset("bigquery/.../bizberry_community_contents_userpost_integration")` 자동 등록.

**Downstream** (consumer):
```python
from airflow.sdk import DAG, Asset

USER_ACTION_TABLE = Asset(
    "bigquery/dev-dp-project-354904/marts/daily_user_summary"
)

with DAG(
    dag_id="daily_report",
    schedule=[USER_ACTION_TABLE],   # ← sensor 대신 asset schedule
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    process = PythonOperator(task_id="process", ...)
    # sensor task 아예 없음
```

이득:
- Sensor task 자체가 사라짐 (worker 자원 0)
- Timing 계산 (`execution_delta`) 필요 없음 — 이벤트 기반
- Upstream 스케줄 바뀌어도 downstream 코드 변경 X
- Cross-team / cross-repo 지원 — asset URI 만 공유

### 여러 asset AND 조건

두 upstream 다 완료돼야 실행:

```python
schedule=[
    Asset("bigquery/dev-dp-project-354904/datawarehouse_berriz/summary_integration"),
    Asset("bigquery/dev-dp-project-354904/datawarehouse_berriz/overview_trend_integration"),
]
```

---

## 전체 DAG Before / After

### Before (Airflow 2 + sensor)

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor


def process(**context):
    ...


with DAG(
    dag_id="daily_report",
    schedule_interval="0 8 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:
    wait_upstream = ExternalTaskSensor(
        task_id="wait_upstream",
        external_dag_id="upstream_etl",
        external_task_id="final_task",
        mode="reschedule",
        execution_delta=timedelta(hours=2),
    )
    wait_source = BigQueryTableExistenceSensor(
        task_id="wait_source",
        project_id="dev-dp-project-354904",
        dataset_id="raw",
        table_id="user_action",
        poke_interval=60,
        mode="reschedule",
    )
    do_report = PythonOperator(task_id="report", python_callable=process)

    [wait_upstream, wait_source] >> do_report
```

### After (Airflow 3 — Deferrable + Asset)

```python
import pendulum
from airflow.sdk import DAG, Asset
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor


UPSTREAM_ASSET = Asset(
    "bigquery/dev-dp-project-354904/marts/daily_user_summary"
)


def process(**context):
    ...


with DAG(
    dag_id="daily_report",
    schedule=[UPSTREAM_ASSET],       # ExternalTaskSensor 대체
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    wait_source = BigQueryTableExistenceSensor(
        task_id="wait_source",
        project_id="dev-dp-project-354904",
        dataset_id="raw",
        table_id="user_action",
        deferrable=True,             # BQ sensor 는 deferrable 로
    )
    do_report = PythonOperator(task_id="report", python_callable=process)

    wait_source >> do_report
```

차이 요약:

| | Before | After |
|---|---|---|
| `ExternalTaskSensor` | O (polling) | X (Asset schedule 로 대체) |
| BQ 존재 확인 | Poke sensor (worker slot 점유) | Deferrable sensor (Triggerer 위임) |
| Worker 자원 사용 | 대기 시간만큼 소진 | 대기 시간 거의 0 |
| Timing 계산 | `execution_delta` 실수 여지 | 필요 없음 (이벤트 기반) |
| 코드 라인 | 20+ | 15 |

---

## 이관 체크리스트

각 DAG 마다:

- [ ] 모든 sensor 확인 — Deferrable 지원되는지 (표 참조)
- [ ] `deferrable=True` 파라미터 추가 (또는 async 버전으로 교체)
- [ ] `ExternalTaskSensor` 있으면 → upstream Asset URI 확인 → `schedule=[Asset(...)]` 로 변경
- [ ] Custom sensor 는 provider 팀에 async 지원 요청 or `poke_interval` 조정
- [ ] `execution_delta`, `execution_date_fn` 같은 timing 계산 코드 제거

---

## 관련 문서

- [[9_Airflow3_Composer_이관_변경사항]] § 1-4 — Deferrable / Triggerer 개요
- [[스케줄러/9_Airflow Asset과 Dataset]] — Asset 개념 상세
- [[스케줄러/7_2_리소스 다이어트 포인트]] — 실측 리소스 절감 효과
- [[7_Lineage와_관측성]] — Asset 기반 cross-DAG dependency 실증
