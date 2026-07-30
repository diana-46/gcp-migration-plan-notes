# 11. Cosmos — dbt 를 Airflow 에 통합하는 라이브러리

> [`astronomer-cosmos`](https://github.com/astronomer/astronomer-cosmos) — dbt 프로젝트를
> Airflow DAG 안에서 **자동으로 task 로 렌더**해주는 오픈소스 라이브러리.

## 한 줄 요약

**dbt run 하나를 Airflow task 하나로** — 모델별 개별 task, 의존성 자동 wiring, retry / alert
/ SLA 등 Airflow 표준 관측성 그대로 활용.

## 왜 필요한가

dbt 프로젝트를 Airflow 에서 돌리는 3가지 방법:

| 방법 | 코드 | 단점 |
|---|---|---|
| **1. BashOperator + `dbt run`** | `BashOperator(bash_command="dbt run --select model_a")` | task 하나로 dbt 전체 실행 → 개별 모델 실패 파악 어려움, retry 시 전체 재실행 |
| **2. 모델마다 BashOperator 수동 작성** | 100개 모델 = 100개 BashOperator + 의존성 수동 wiring | 유지 부담 폭증, ref() 변경 시 DAG 도 수정 |
| **3. Cosmos** | `DbtTaskGroup(select=["+my_mart"])` 한 줄 | ✅ dbt manifest 로 자동 렌더 |

Cosmos = **dbt manifest 를 읽어서 Airflow task graph 로 자동 변환**.

## Cosmos 가 실제로 하는 일

```
storydata-dbt/target/manifest.json          ← dbt parse 산출물
        │
        │  (Cosmos 가 파싱)
        ▼
DbtTaskGroup(select=["+bizberry_community_overview_trend"])
        │
        │  (manifest 의 ref() 그래프 traverse)
        ▼
Airflow task graph:
    temp_bizberry_country_trend           ─┐
    temp_bizberry_fanclub_product_trend   ─┴─→ bizberry_community_overview_trend
```

- 각 dbt model → Airflow task 하나 (기본은 `.run`)
- `ref()` 로 걸린 dependency → Airflow task order 자동 wiring
- `dbt test` 도 model 마다 별도 test task 로 생성 (설정 가능)
- Outlet Asset URI 자동 부착 (Airflow 3 DatasetAlias)

## 우리 케이스에서 실제 사용

`storydata-airflow-dags/dags/storydata/berriz_0101_bizberry_hourly_integration.py`:

```python
from cosmos import DbtTaskGroup, ExecutionConfig, ExecutionMode, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import LoadMode

dbt_overview_trend = DbtTaskGroup(
    group_id="dbt_overview_trend",
    project_config=ProjectConfig(
        dbt_project_path="/home/airflow/gcs/data/dbt",
        manifest_path="/home/airflow/gcs/data/dbt/target/manifest.json",
    ),
    profile_config=ProfileConfig(
        profile_name="storydata",
        target_name="integration",
        profiles_yml_filepath="/home/airflow/gcs/data/dbt/profiles.yml",
    ),
    render_config=RenderConfig(
        load_method=LoadMode.DBT_MANIFEST,
        select=["+bizberry_community_overview_trend"],
    ),
    execution_config=ExecutionConfig(
        execution_mode=ExecutionMode.LOCAL,
    ),
    operator_args={"vars": dbt_vars_template},
)
```

이 6줄 짜리 config 로 Airflow UI 에 3개 task (2 temp + 1 mart) 자동 생성.

## 핵심 설정 2가지

### `LoadMode` — Cosmos 가 dbt 프로젝트를 읽는 방법

우리 선택: **`DBT_MANIFEST`** (사전 컴파일된 manifest.json 을 JSON 으로 파싱 → 초고속)

대안:
- `DBT_LS`: DAG 파싱마다 `dbt ls` subprocess. 스케줄러 부하 큼. 로컬 개발용.
- `AUTOMATIC`: manifest 있으면 DBT_MANIFEST, 없으면 DBT_LS fallback.
- 기타: `DBT_LS_FILE`, `DBT_LS_CACHE`, `CUSTOM`

**프로덕션 표준: `DBT_MANIFEST`**. CI 에서 `dbt parse` 로 manifest 만들고 GCS sync.

### `ExecutionMode` — dbt 를 어디서 실행하는지

우리 선택: **`LOCAL`** (Celery worker process 안에서 dbtRunner 를 Python 으로 invoke)

대안:
- `KUBERNETES`: 각 dbt task 를 K8s pod 로 실행 (image 관리 필요)
- `VIRTUALENV`: 별도 venv
- `DOCKER`, `AWS_ECS`, `GCP_CLOUD_RUN_JOB` 등

**대부분 케이스: `LOCAL`**. 무거운 모델만 `KUBERNETES` 로 분리 (Airflow 3 의
`executor="KubernetesExecutor"` 파라미터로도 가능).

## Cosmos 가 자동으로 해주는 것

- Model 별 task 생성
- ref() 기반 의존성 wiring
- 사용자 정의 `select` 로 부분 실행 (`+model` upstream 포함, `model+` downstream 포함)
- Airflow Asset outlet 자동 부착 (Airflow 3 DatasetAlias)
- Test task 생성 (설정 시)
- Airflow 표준 관측성 (로그, retry, SLA, Slack alert)

## DE 가 알아야 할 것

**DAG 파일에서 다루는 것**:
- `select` 문법 (dbt 와 동일: `+model`, `model+`, `tag:xxx`, `path:xxx` 등)
- `vars` 로 execution context 전달 (`{{ data_interval_start... }}`)
- `target_name` 으로 환경 스위치 (dev / integration / production)

**모르는 게 자연스러운 것**:
- Load mode / Execution mode 내부 동작 (플랫폼팀이 표준값 세팅)
- Manifest sync 흐름 (CI 자동)
- DatasetAlias emit 매커니즘 (Airflow 3 native)

DE 관점에선 **"dbt 프로젝트만 잘 관리하면 Airflow task 는 알아서 만들어진다"** 로 이해하면 충분.

## 배포 순서 주의

Cosmos 는 `LoadMode.DBT_MANIFEST` 로 GCS 마운트된 manifest 를 읽음 →
**dbt 저장소 (manifest 생성) → airflow-dags 저장소 (manifest 참조) 순서로 push** 해야 함.
역순이면 Cosmos 가 새 모델을 manifest 에서 못 찾아 DAG import error.

관련: [[애슬론/8_배포 시 유의할 점]] § 1

## Cosmos 대신 안 쓴다면

BashOperator + `dbt run` 만 쓴다면 잃는 것들:
- Model 별 개별 task (실패 파악 어려움)
- ref() 자동 dependency
- Airflow Asset lineage
- 개별 모델 재시도

시연에서 이 대비를 강조하면 Cosmos 채택 근거 명확해짐.

## 관련 문서

- [[2_결정A_dbt로_왜_가는가]] — dbt 이관 결정
- [[5_3layer_배포_아키텍처]] § Layer 2 — dbt project 배포 흐름
- [[dbt/0_dbt 기본 개념]] § 8 — manifest.json 설명
- [[dbt/1_materialization]] § 2-1 — Cosmos + ephemeral 상호작용
- [[애슬론/8_배포 시 유의할 점]] § 1 — 배포 순서
