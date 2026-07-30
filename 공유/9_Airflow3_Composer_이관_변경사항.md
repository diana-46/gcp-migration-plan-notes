# 9. Airflow 3 + Composer 이관에서 달라지는 것

> Airflow 2 → 3 + Self-managed → Cloud Composer 3 이관에서 **DE / 운영 관점에서 실제로
> 코드·워크플로우·운영이 어떻게 바뀌는지** 정리.
> 관련: [[스케줄러/6_Airflow 2 vs 3 비교]], [[스케줄러/1_개요]], [[애슬론/8_배포 시 유의할 점]]

---

## 0. 배경 — 왜 이 이관 방향인가

### 0-1. BQ 이관이 트리거

- 사내 데이터 저장소 이관 = **Hive / Presto → BigQuery** (별건 결정)
- Neptune ETL 이 붙어있던 Hive metastore · HDFS · Presto 는 다 사라짐
- 파이프라인이 새 스토리지 위에 다시 살아야 함 → 스케줄러 이관은 필연적 파생

### 0-2. 왜 Cloud Composer 3 인가 (다른 옵션 대비)

세 가지 대안 후보:

| 옵션 | 장점 | 단점 |
|---|---|---|
| **Cloud Composer 3** ← 채택 | 관리형 (GKE / Cloud SQL / Memorystore 자동), Airflow 3 번들, 서울 리전, BQ 네이티브 | Composer 3 이 self-managed 대비 20-35% 비쌈, executor / config 자유도 감소 |
| Self-managed Airflow on GKE | 요금 절감 (Spot / CUD), config 자유 | 인프라 관리 부담 (0.5+ FTE 필요), Airflow 업그레이드 · Cloud SQL 백업 · WI 세팅 등 |
| Composer 2 | 기존 팀 관성 | Airflow 2 종속 → 3 이관 필요 시 재작업 |

**채택 근거**: 이관 초기엔 관리 부담 축소가 이득 > 요금 프리미엄. 팀 규모 · 파이프라인 성숙도에 따라 Phase 2 후 자체 관리로 이동 옵션은 열려 있음.
관련: [[스케줄러/2_Cloud Composer vs Self-managed 비교]], [[스케줄러/0_결론]]

### 0-3. 왜 Airflow 3 (Composer 2 아니라 3)

- **Composer 3 은 Airflow 3.1.7 번들** — Composer 이관하는 순간 Airflow 3 강제
- Composer 2 (Airflow 2 번들) 로 갈 옵션 있으나 어차피 Airflow 3 이관이 예정 → 두 번 이관 회피
- Airflow 3 이 주는 이득 (Asset, Task SDK, Deferrable) 이 큼 (본문 § 2 참조)

### 0-4. GCP 이관에 딸려오는 것들 (전제 조건)

Composer 는 GCP 네이티브 → 데이터 · 인증 · 배포 도구가 **모두 GCP 것으로** 옮겨감. DE / 운영이 반드시 알아야 할 전제:

**GCS (Google Cloud Storage)** — 곳곳에서 사용:
- **DAG 배포**: Composer 는 `gs://COMPOSER_BUCKET/dags/` 에서 DAG 파일 읽음 → git push 로 직접 배포 불가, GCS sync 필요
- **dbt project · manifest**: `gs://COMPOSER_BUCKET/data/dbt/` 로 sync → 워커가 `/home/airflow/gcs/data/dbt/` 로 마운트해서 참조
- **PythonVirtualenvOperator dependency**: GCS 캐시
- **로그 export**: Cloud Logging → BQ / GCS 로 영구 보존
- **Airflow logs**: `gs://COMPOSER_BUCKET/logs/` 자동

**IAM + Workload Identity** — 인증 · 권한 통합:
- Composer 환경 SA 가 BQ / GCS / Pub/Sub 등 접근 → SA key JSON 관리 대신 WI 로 자동 인증
- DE 계정: Google Workspace SSO → GCP IAM → IAP → Airflow UI (LDAP 인증 사라짐)
- 관련: [[스케줄러/8_Composer 권한 및 인증]]

**Cloud Logging / Monitoring** — 관측성 자동:
- Task 로그 자동 수집 (별도 설정 X)
- `resource.type=cloud_composer_environment` 필터로 검색
- Metrics 대시보드 자동 제공

**Artifact Registry** — 사내 Python 패키지 저장소:
- `apache-airflow-providers-kakaoent-dataplatform` 등 사내 패키지 배포
- `pip install --extra-index-url` 로 소비
- SA + `keyrings.google-artifactregistry-auth` 인증
- 관련: [[스케줄러/7_3_공통 Custom Operator 제공 방안]]

**Secret Manager** (선택) — GH Actions WIF / DataHub token 등 secrets 관리 (아직 세팅 안 함, 이관 후 도입 예정)

이 GCP 전제들이 Composer + Airflow 3 이관에 자연스럽게 딸려옴. **GCP 요소 (GCS, IAM, WI, Cloud Logging) 학습이 이관의 일부**.

---

## 1. Airflow 2 → 3 Breaking Changes

### 1-1. DAG authoring API 격리

**Before (Airflow 2)**:
```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
```

**After (Airflow 3)**:
```python
from airflow.sdk import DAG, Variable
from airflow.providers.standard.operators.python import PythonOperator
```

**의미**: Airflow 3 은 **Task SDK 를 격리** — task 코드가 metadata DB 직접 접근 X, REST API 로만 통신. 보안·확장성 이득. DE 관점에선 **import 경로 대량 변경**.

### 1-2. Removed

| 기능 | 대체 |
|---|---|
| `SubDAG` | `TaskGroup` (이미 Airflow 2 에도 있었음) |
| `SLA` / `sla_miss_callback` | Deadline alerts / Slack callback 수동 |
| Smart Sensor | Deferrable operator (async) |
| Python 3.8 지원 | Python 3.11+ 강제 |

### 1-3. Dataset → Asset

**Before (Airflow 2.4+ Dataset)**:
```python
from airflow import Dataset
my_dataset = Dataset("s3://bucket/data")
with DAG(schedule=[my_dataset], ...):
    ...
```

**After (Airflow 3 Asset)**:
```python
from airflow.sdk import Asset
my_asset = Asset("bigquery/project/dataset/table")
with DAG(schedule=[my_asset], ...):
    ...
```

**URI 문법 변경**:
- Airflow 2: `bigquery/project.dataset.table` (dot 구분)
- Airflow 3: `bigquery/project/dataset/table` (slash 구분)
- Cosmos 는 자동으로 새 format emit — 경고 무시 원하면 `AIRFLOW__COSMOS__USE_DATASET_AIRFLOW3_URI_STANDARD=1`

**신규 개념**:
- **AssetAlias**: 런타임에 실제 URI 를 alias 에 붙임 (동적 asset 등록)
- **AssetWatcher**: 외부 이벤트 (FileTrigger, PubSubMessageTrigger) → asset 갱신 → DAG 자동 트리거

### 1-4. Deferrable / Triggerer 가 표준으로

Airflow 2 에서는 deferrable 이 옵션 · 추가 기능이었지만, Airflow 3 에서는 **표준 실행 모델의
일부**로 통합.

- 새 operator 들이 기본으로 `deferrable=True` 지원 (예: `BigQueryInsertJobOperator`)
- **Triggerer** 는 필수 인프라 컴포넌트 (Composer 3 이 자동 provisioning)
- Airflow 2 의 **Smart Sensor** (여러 sensor 를 한 프로세스에 배치 처리하던 실험적 기능)
  는 완전 삭제 → deferrable 이 더 깔끔한 대안이라 대체

**동작 차이**:

| Sync sensor (기존) | Deferrable (Airflow 3 표준) |
|---|---|
| Worker slot 점유하며 poke | Triggerer 로 위임 → worker slot 즉시 해제 |
| `poke_interval` 마다 worker 깨서 확인 | Triggerer 가 async event loop 로 감시 |
| 대기 시간 = worker cost | 대기 시간 ≈ 무료 |

**사용법**: 대부분 `deferrable=True` 파라미터 하나 추가하면 됨.

- 실측 예 ([[스케줄러/7_2_리소스 다이어트 포인트]]): sensor 하나가 **월 522 worker-hours**
  점유 → deferrable 로 옮기면 near-zero
- Composer DCU 요금 축소의 큰 지렛대

### 1-5. DAG Bundles

- 여러 저장소를 하나의 Airflow 에 nativley 로드 (`GitDagBundle`)
- 우리 케이스에선 **팀별 저장소를 서로 다른 bundle 로 관리 가능** (관련: [[3_결정B_팀별_DAG_저장소]])

### 1-6. Provider 패키지 완전 분리

- Airflow 2 시절 core 에 있던 operator 도 provider 로 이관
- `from airflow.operators.python` → `from airflow.providers.standard.operators.python`
- `pip install apache-airflow-providers-standard` 필요

---

## 2. Self-managed → Composer 3 이관에서 달라지는 것

### 2-1. 인프라 관리 위임

| 이전 | 이후 (Composer 관리) |
|---|---|
| GKE 클러스터 자체 관리 | Google 이 자동 관리 |
| Cloud SQL / PostgreSQL 자체 관리 | 자동 provisioning + 백업 |
| Memorystore (Redis) 자체 관리 | 자동 |
| Workload Identity Federation 세팅 | 자동 |
| 오토스케일링 정책 튜닝 | 자동 (worker 만 우리가 min/max 조절) |
| 로그 · 모니터링 파이프라인 | Cloud Logging / Monitoring 자동 |

**Trade-off**: 관리 부담 감소 vs 유연성 감소.

### 2-2. Executor 자유도 제약

- **CeleryKubernetesExecutor 강제** (Composer 3)
- `KubernetesExecutor` 단독 사용 불가
- Celery worker 큐 격리 제한
- 관련: [[스케줄러/3_Executor 종류 및 비교]]

### 2-3. DAG 배포 방식

- **Git push 로 직접 배포 안 됨** — GCS bucket 을 통한 sync
- 표준: GitHub Actions → `gsutil rsync -r -c -d dags/TEAM/ BUCKET/dags/TEAM/`
- git-sync sidecar 는 Composer 지원 제한적
- 관련: [[5_3layer_배포_아키텍처]] § Layer 3

### 2-4. Airflow 설정 관리 방식

**Before (Self-managed)**: `airflow.cfg` 직접 수정 or env var 자유롭게

**After (Composer)**:
- `airflow.cfg` 직접 수정 **불가**
- `AIRFLOW__` prefix env var 대부분 **거부** (Composer 가 보호)
- 대신 `--update-airflow-configs section-key=value` 로 세팅
- 예:
  ```bash
  gcloud composer environments update ENV_NAME \
      --update-airflow-configs cosmos-use_dataset_airflow3_uri_standard=1
  ```

### 2-5. PyPI 패키지 관리

**Before**: `pip install PACKAGE_NAME` 즉시 반영

**After**: Composer 환경 update 명령 필요
```bash
gcloud composer environments update ENV_NAME \
    --update-pypi-packages-from-file /tmp/req.txt
```
- 10-20분 소요, 상태 `UPDATING`
- Composer 3 은 `--constraint` / `--extra-index-url` 같은 pip directive 거부 (raw `pkg==ver` 만)
- 관련: [[애슬론/8_배포 시 유의할 점]] § 2

### 2-6. 인증 · 권한

**3-layer 모델** (관련: [[스케줄러/8_Composer 권한 및 인증]]):

1. **UI 접근**: GCP IAM (`composer.user` 등) → Google SSO 로그인 (IAP 통해)
2. **Airflow 액션**: FAB RBAC (Admin/Op/User/Viewer/Public) — 기존과 동일
3. **GCP 리소스 접근**: Workload Identity → 환경 SA → BQ/GCS/Pub/Sub

**변경 포인트**:
- LDAP 인증 → Google Workspace SSO
- SA key JSON 관리 → Workload Identity (권장)
- Username 이 `accounts.google.com:NUMERIC_ID` 형태

### 2-7. Logging · Monitoring

- 로그 자동 → Cloud Logging 으로 (별도 설정 없음)
- Task 로그: `resource.type=cloud_composer_environment` 필터로 검색
- Metrics: Cloud Monitoring 대시보드 자동 제공
- 로그 retention: `_Default` bucket 30일 (BQ export 로 영구 보존 가능)

### 2-8. 비용 모델

**Before (Self-managed GKE)**: 원가 = vCPU + RAM + Cloud SQL + 워커 노드 스팟

**After (Composer 3)**: 원가 = **DCU** (Data Compute Unit) — 24×7 상주 컴포넌트 + 사용량
- Scheduler / DAG processor / triggerer 는 DAG 0 개여도 floor cost (~$200-300/월)
- 관련: [[스케줄러/14_Composer 3 비용 구조]], [[4_Composer_조사_요약]] § 4

---

## 3. 우리 실전에서 만난 함정 (Story 팀 PoC 2026-06 ~ 07)

이 함정들은 **매뉴얼에 없거나 이름만 봐선 안 걸리는 실전 이슈** 들.

### 3-1. `AIRFLOW__` env var 대부분 거부

**증상**:
```
ERROR: Environment variables [AIRFLOW__COSMOS__USE_DATASET_AIRFLOW3_URI_STANDARD]
may not be overridden.
```

**원인**: Composer 3 은 `AIRFLOW__` prefix env var 를 보호 항목으로 취급.

**해결**: `--update-airflow-configs cosmos-use_dataset_airflow3_uri_standard=1` 로 세팅.

### 3-2. `apache-beam` backtracking 지옥

**증상**: PyPI update 30분 timeout. 로그에 apache-beam 여러 버전 시도 반복.

**원인**: `apache-airflow-core==3.1.7+composer` variant 가 `providers-apache-beam` 을 hard dep 로 가짐. 새 패키지 install 시 pip resolver 가 apache-beam (transitive deps 수십 개) 을 재검증.

**해결**: **raw pin** 을 requirements file 에 넣어 resolver 를 짧게 만듦:
```
apache-beam==2.74.0
asyncssh==2.20.0
astronomer-cosmos==1.10.1
dbt-core==1.11.12
dbt-bigquery==1.11.3
```
관련: [[애슬론/8_배포 시 유의할 점]] § 2-2

### 3-3. `PIP_NO_DEPS` 함정

**증상**: 새로 install 한 `dbt-bigquery` 는 있는데 필수 dep `dbt-core` 는 옛 버전. click 등 transitive 호환성 깨짐. dbt 실행 시 `ImportError: cannot import name '_OptionParser' from 'click.parser'` 등 크래시.

**원인**: `PIP_NO_DEPS=1` env var 가 pip 에 dep resolve 스킵 강제.

**해결**: Composer env var 에서 `PIP_NO_DEPS`, `PIP_USE_DEPRECATED` 제거. Raw pin + 정상 dep resolve 조합이 정답.

### 3-4. `>=` vs `==` 버전 pin

**증상**: `dbt-core>=1.11` 로 declare 했는데 옛 dbt-core 그대로 남음.

**원인**: Composer PyPI 의 declared 상태 vs 실제 install 상태 다를 수 있음. `>=X` 로 declare 하면 pip 이 "이미 만족" 판단해 upgrade 안 하는 경우.

**해결**: **exact pin (`==X.Y.Z`)** 사용:
```
✅ dbt-core==1.11.12
❌ dbt-core>=1.11
```

### 3-5. dbt 버전 mismatch (CI vs Composer)

**증상**: Composer 에서 `dbt run` 이 hang. Cosmos 가 manifest 파싱 못함.

**원인**: 로컬/CI 에서 dbt 1.11 로 manifest 생성 → Composer 의 1.9 가 manifest schema 버전 mismatch 로 못 읽음.

**해결**: CI 워크플로우의 dbt 버전을 Composer 와 정확히 매치. `pip install 'dbt-core==1.11.*'` 강제.

### 3-6. Composer state=UPDATING 중 DAG 실행 금지

**증상**: PyPI update 진행 중 실행된 task 가 transitional state (일부 옛 라이브러리, 일부 새) 볼 수 있음.

**해결**: 다음 명령으로 확인 후 trigger:
```bash
gcloud composer environments describe ENV_NAME --format="value(state)"
```
결과가 `RUNNING` 일 때만 안전.

### 3-7. gcloud ADC 만료

**증상**: `gcloud composer environments update` 명령이 조용히 실패, "If you have already logged in with a different account..." 안내만.

**해결**: 사전 확인:
```bash
gcloud auth application-default print-access-token > /dev/null \
    && echo "ADC OK" || echo "ADC 재로그인 필요"
```
필요 시 `gcloud auth application-default login`.

### 3-8. 배포 순서 (dbt → airflow-dags)

**증상**: DAG import error 배지. cosmos 가 새 모델을 manifest 에서 못 찾음.

**원인**: 역순 배포 (airflow-dags 먼저 → dbt 나중) 로 manifest 갱신 전에 DAG 파싱 시도.

**해결**: **표준 순서 준수**:
1. dbt 저장소 push → Actions 로 manifest sync 완료 대기
2. airflow-dags 저장소 push
관련: [[애슬론/8_배포 시 유의할 점]] § 1

---

## 4. DAG 코드 변경 예시

### Before (Airflow 2 self-managed)

```python
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.sensors.external_task import ExternalTaskSensor

def my_task(**context):
    ...

with DAG(
    dag_id="daily_report",
    schedule_interval="0 6 * * *",
    start_date=datetime(2025, 1, 1),
) as dag:
    wait = ExternalTaskSensor(
        task_id="wait_upstream",
        external_dag_id="upstream_dag",
        external_task_id="final_task",
        mode="reschedule",
    )
    process = PythonOperator(
        task_id="process",
        python_callable=my_task,
    )
    wait >> process
```

### After (Airflow 3 + Composer)

```python
import pendulum
from airflow.sdk import DAG, Asset, Variable
from airflow.providers.standard.operators.python import PythonOperator

UPSTREAM_ASSET = Asset(
    "bigquery/dev-dp-project-354904/datawarehouse_berriz/upstream_table"
)

def my_task(**context):
    ...

with DAG(
    dag_id="daily_report",
    schedule=[UPSTREAM_ASSET],   # ← ExternalTaskSensor 대체
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
) as dag:
    process = PythonOperator(
        task_id="process",
        python_callable=my_task,
    )
```

**주요 변경**:
- `from airflow import DAG` → `from airflow.sdk import DAG`
- `airflow.operators.python` → `airflow.providers.standard.operators.python`
- `schedule_interval` → `schedule` (문자열이든 Asset 리스트든 동일 파라미터)
- `datetime` → `pendulum` (timezone-aware)
- `ExternalTaskSensor` → `schedule=[Asset(...)]` (이벤트 기반)
- Task chain 자체가 단순해짐 (sensor task 소멸)

---

## 5. 이관 체크리스트 (DE 관점)

각 DAG 파일마다:

- [ ] `from airflow import DAG` → `from airflow.sdk import DAG`
- [ ] `airflow.operators.*` → `airflow.providers.standard.operators.*`
- [ ] `datetime` → `pendulum` (`tz` 명시)
- [ ] `schedule_interval` → `schedule`
- [ ] `SubDAG` → `TaskGroup`
- [ ] `sla_miss_callback` → Slack callback 수동
- [ ] `Dataset` → `Asset`, URI 문법 slash 구분
- [ ] `ExternalTaskSensor` → `schedule=[Asset(...)]` (해당 시)
- [ ] 사내 operator import 경로 → `airflow.providers.kakaoent.dataplatform.*`
- [ ] Poke sensor → deferrable (`deferrable=True` 또는 async operator)

각 프로젝트마다:

- [ ] `requirements.txt` 에 `apache-airflow-providers-*` 명시
- [ ] `apache-airflow-providers-kakaoent-dataplatform` 사내 provider 포함
- [ ] Composer 환경에 PyPI raw pin 반영
- [ ] CI 워크플로우 dbt / provider 버전을 Composer 와 동일 pin
- [ ] pytest / lint 통과 확인

---

## 6. 요약 — 한 눈에

| 축 | 이전 | 이후 |
|---|---|---|
| DAG import | `from airflow import DAG` | `from airflow.sdk import DAG` |
| Operator import | `airflow.operators.*` | `airflow.providers.standard.operators.*` |
| 스케줄 표현 | `schedule_interval=cron` | `schedule=cron` or `schedule=[Asset]` |
| Cross-DAG dep | `ExternalTaskSensor` (polling) | `schedule=[Asset]` (event) |
| Lineage | 파편 | dbt manifest + Airflow OpenLineage stitching |
| DAG 배포 | git push 즉시 | GitHub Actions → GCS sync |
| Airflow 설정 | `airflow.cfg` 자유 | `--update-airflow-configs` |
| PyPI 설치 | `pip install` 즉시 | `--update-pypi-packages-from-file` (10-20분) |
| 인증 | LDAP | Google SSO + IAP + FAB RBAC + WI |
| 로그 | 자체 관리 | Cloud Logging 자동 |
| Executor | 자유 선택 | CeleryKubernetesExecutor 강제 |
| 비용 모델 | vCPU+RAM+SQL | DCU + floor cost |
| Sensor | Poke / Reschedule | Deferrable (worker slot 절약) |

---

## 관련 문서

- [[스케줄러/6_Airflow 2 vs 3 비교]] — Airflow 버전 breaking change 상세
- [[스케줄러/1_개요]] — Composer 3 운영 방향
- [[스케줄러/2_Cloud Composer vs Self-managed 비교]] — Composer 채택 근거
- [[스케줄러/8_Composer 권한 및 인증]] — 3-layer 인증 모델
- [[스케줄러/9_Airflow Asset과 Dataset]] — Asset 개념 상세
- [[스케줄러/11_DAG Bundles와 배포 전략]] — DAG 배포 전략
- [[스케줄러/14_Composer 3 비용 구조]] — DCU 요금 모델
- [[애슬론/8_배포 시 유의할 점]] — 실전 배포 함정
- [[4_Composer_조사_요약]] — 조사 결과 총정리
