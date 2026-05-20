---
title: "04. Queue / Worker / Pool 패턴 검증"
status: in-progress
tags:
  - poc
  - queue
  - worker
  - celery
  - kubernetes-executor
  - composer3
created: 2026-05-20
updated: 2026-05-20
---

# 04. Queue / Worker / Pool 패턴 검증

> **검증 질문**: 사내 5종 worker queue 패턴 (`hadoop`/`cloud`/`http`/`sensor`/`doopey`) 이 Composer 3 에서 어떻게 매핑되나? 각 분리 메커니즘이 실제로 동작하는가?
>
> **답 (예상)**: Queue 분리는 ❌ (Composer 는 단일 `default` queue 강제). 대신 **executor + Pool + Triggerer** 3가지 메커니즘으로 분리. `sensor:40` 의 deferrable 전환이 가장 큰 작업.

## 환경

| 항목 | 값 |
|---|---|
| Composer | `test-airflow3` (asia-northeast3) |
| Airflow | composer-3-airflow-3.1.7-build.9 |
| 작업자 사양 | vCPU 0.5 / 메모리 2.5GB / 스토리지 10GB |
| 작업자 autoscale | 1~3 |

## 🔑 핵심 개념 발견

### Self-managed: queue = worker 분리 단위

기존 사내 패턴 (`~/WebstormProjects/data-platform-settings/playbooks/roles/airflow2`):

```
queue 5종              worker 5개 (systemd 별도)
─────                  ──────────────────────────
hadoop:6     ────►    airflow-worker-hadoop      (concurrency=6)
cloud:6      ────►    airflow-worker-cloud       (concurrency=6)
http:5       ────►    airflow-worker-http        (concurrency=5)
sensor:40    ────►    airflow-worker-sensor      (concurrency=40)
doopey:2 ×3  ────►    airflow-worker-doopey-*    (concurrency=2 × 3 variant)
```

→ queue 가 곧 worker 분리. worker 별 노드 사양/concurrency 자유.

### Composer: queue 1개 × worker N개 (autoscale)

```
queue 1개                worker Pod 1~N개 (autoscale)
─────────                ──────────────────────────────
default       ◄────      worker-pod-1   (concurrency=12)
              ◄────      worker-pod-2   (concurrency=12)
              ◄────      worker-pod-3   (concurrency=12)
                         ↑ 부하에 따라 1→3 autoscale
```

→ **queue 갯수는 항상 1**. autoscale 은 같은 queue 를 듣는 **worker Pod 수**.

→ `task.queue='foo'` 박아도 모든 task 가 같은 worker pool 로 흐름. Queue 가 분리 단위로 의미 없음.

## ⚠️ "Celery 도 Pod" 함정 — Pod ≠ Pod

검증 중 발견: Composer 의 **모든 Airflow 컴포넌트가 K8s Pod**. 단 lifecycle 이 완전히 다름.

| | Celery worker Pod | KubernetesExecutor Pod |
|---|---|---|
| Pod 수 | 1~N (autoscale) | task 1개당 1개 |
| Pod 수명 | 환경 수명 (장기) | task 1회 (단기) |
| Pod 생성 비용 | 환경 시작 시 1회 | 매번 10~60초 |
| task 처리 capacity | worker concurrency × Pod 수 | Pod 1개 = task 1개 |
| 사양 차별화 | 모든 task 동일 | task 별 자유 |
| 짧고 잦은 task | ⭐ 적합 | ❌ 비효율 |
| 무겁고 드문 task | △ 사양 한계 | ⭐ 적합 |
| 격리 | 약함 (process 공유) | 강함 (Pod 분리) |

→ **Celery 의 가치**: Pod 띄우는 오버헤드 회피. 1개 worker Pod 가 수천 개 task 처리.
→ **KubernetesExecutor 의 가치**: 격리 + 사양 자유. 단 짧은 task 에 쓰면 손해.

## Composer 의 4가지 분리 메커니즘

queue 가 막혔으니 다른 mechanism 으로 분리:

### 1. `executor="KubernetesExecutor"` — Pod 격리

```python
heavy = BashOperator(
    task_id='heavy',
    bash_command='dbt run --select huge_model',
    executor="KubernetesExecutor",
    executor_config={'pod_override': ...},   # Pod 사양 자유
)
```

→ heavy / 격리 / 특수 이미지 필요한 task 만.

### 2. Airflow Pool — 동시성 제한

```python
sensor = ExternalTaskSensor(
    task_id='wait',
    pool='sensor_pool',     # 동시 N개로 제한
    pool_slots=1,
)
```

→ queue 분리 못 해도 capacity 제어는 가능. Airflow UI → Admin → Pools.

### 3. Deferrable Sensor + Triggerer — sensor 분리 ⭐

```python
from airflow.providers.standard.sensors.time import TimeDeltaSensorAsync

wait = TimeDeltaSensorAsync(
    task_id='wait',
    delta=timedelta(hours=1),
)
```

→ Triggerer (lightweight component) 가 polling 처리. **worker slot 안 잡음**. 수천 sensor 도 worker 1~2개 분으로 처리.

### 4. Autoscale 상하한 — 단일 pool 의 capacity 조절

콘솔 → 환경 → **환경 구성** → **워크로드 구성** → "수정" → 작업자:

| 항목 | 의미 |
|---|---|
| 작업자 vCPU / 메모리 | Pod 1개당 사양 |
| **최소 작업자 수** | autoscale 하한 |
| **최대 작업자 수** | autoscale 상한 |

→ 전체 worker pool 의 capacity 만 조절. 사양 차별화 X.

## 사내 5종 queue → Composer 매핑

| 기존 queue | concurrency | Composer 대응 | 작업량 |
|---|---|---|---|
| `hadoop:6` | 6 | ❌ 폐기 | 0 (Hive 폐기 = dbt+BQ 이관 효과) |
| `doopey:2` ×3 | 6 | ❌ 폐기 | 0 |
| `cloud:6` | 6 | Celery worker (단일) | queue 이름 제거만 |
| `http:5` | 5 | Celery worker (단일) | queue 이름 제거만 |
| **`sensor:40`** | **40** | **deferrable Sensor + Triggerer** ⭐ | **코드 수정 필요** |
| (heavy task?) | — | `executor="KubernetesExecutor"` | 검토 필요 |

**핵심 작업**: `sensor:40` → deferrable 전환.

### Sensor 전환 효과 추정

| | 기존 (Self-managed) | Composer (deferrable) |
|---|---|---|
| Sensor 동시 처리 | worker 1개 + concurrency 40 | Triggerer 1개 + thousands |
| Worker slot 점유 | 40 (대기 중에도) | 0 |
| 비용 | worker 1대 상시 | Triggerer 1대 (lighter) |
| 코드 수정 | — | `XxxSensor` → `XxxSensorAsync` 또는 `deferrable=True` |

→ 실제 코드 수정 인벤토리: [[01_airflow3_compat_grep]] 의 sensor 패턴 grep 으로 추출 가능.

## 🔒 Composer 3 의 GKE 봉인

검증 방법론 자체에 영향 주는 사실. 회의 자료에 같이 박을 가치 있음.

| 항목 | Composer 3 |
|---|---|
| 시스템 Pod (worker / scheduler / triggerer / dag-processor / redis) | **kubectl 직접 접근 불가** |
| GKE 클러스터 위치 | Google tenant project (본인 프로젝트 아님) |
| `config.gkeCluster` 필드 | **빈 값** (의도적 미노출) |
| Composer 2 와의 차이 | 2: GKE 본인 프로젝트, kubectl OK / 3: 완전 봉인 |

**확인 명령** (모두 빈 출력 또는 정보 제한):
```bash
gcloud composer environments describe test-airflow3 \
  --location=asia-northeast3 \
  --format="value(config.gkeCluster)"
# → (빈 출력)
```

**검증 가능 경로**:

| 보고 싶은 것 | 우회 방법 |
|---|---|
| Worker Pod 갯수 | 모니터링 탭 "작업자" 그래프 |
| Scheduler / Triggerer 부하 | 모니터링 탭 컴포넌트별 그래프 |
| Pod 사양 / 노드 위치 | 환경 구성 탭 (설정값만) |
| Pod 로그 | Cloud Logging (`resource.type="cloud_composer_environment"`) |
| Task 실행 상태 | Airflow UI |
| Pod 생성 이벤트 | Cloud Logging (audit) |
| **본인 워크로드 Pod** | `gcloud composer environments user-workloads-pods` (시스템 Pod 는 X) |

**Trade-off**:
- **장점**: GKE 운영 책임 면제 (보안 패치 / 노드 풀 관리 / 클러스터 업그레이드 다 Google)
- **단점**: 디버깅 깊이 들어가야 할 때 보이는 정보 제한적

→ Self-managed 의 "kubectl 로 모든 것 확인" 패턴 X. 다음 검증 시나리오는 전부 **모니터링 탭 + Airflow UI + Cloud Logging** 으로 우회.

## 검증 시나리오

> kubectl 직접 접근 불가 (Composer 3 가 봉인). 모니터링 탭 + Airflow UI + task 동작으로 검증.

### A. Celery worker autoscale 관찰 ⭐

**목표**: 부하 시 worker Pod 가 1→3 으로 autoscale 되는지 + capacity 분포 확인.

**DAG**:
```python
# poc_celery_autoscale.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG("poc_celery_autoscale", schedule=None,
         start_date=datetime(2026, 5, 1), catchup=False, tags=["poc"]) as dag:
    for i in range(30):
        BashOperator(
            task_id=f"sleep_{i}",
            bash_command="sleep 60",
        )
```

**관찰**:
- 콘솔 → 환경 → **모니터링** 탭 → "작업자" 그래프
- trigger 직후 → 1~5분 후 작업자 수 증가 (1 → 3)
- task 분포: 30개가 worker × concurrency 안에서 어떻게 처리되는지

**결과** _(채울 예정)_:

| 시각 | 작업자 수 | running task | queued task |
|---|---|---|---|
| trigger 직후 | | | |
| 1분 후 | | | |
| 3분 후 | | | |
| 5분 후 (안정) | | | |

### B. `task.queue='foo'` 무시되는지

**목표**: 사용자 정의 queue 이름이 Composer 에선 의미 없음을 확인.

**DAG**:
```python
with DAG("poc_queue_ignored", ...) as dag:
    BashOperator(task_id="default_q", bash_command="hostname")
    BashOperator(task_id="hadoop_q",  bash_command="hostname", queue="hadoop")
    BashOperator(task_id="sensor_q",  bash_command="hostname", queue="sensor")
    BashOperator(task_id="fake_q",    bash_command="hostname", queue="nonexistent")
```

**관찰**:
- 4개 task 모두 정상 실행 ✅ (queue 이름이 막지 않음)
- task log 의 `hostname` 값 비교 → 같은 worker pool 이면 모두 같은/유사 호스트네임

**결과** _(채울 예정)_: queue 무시 확인 / 무시 X

### C. `executor="KubernetesExecutor"` 진짜 Pod 분리

**목표**: KubernetesExecutor 가 task 마다 별도 Pod 띄우는지, hostname 으로 확인.

**DAG**:
```python
with DAG("poc_k8s_executor", ...) as dag:
    BashOperator(
        task_id="celery_task",
        bash_command="hostname; sleep 5",
    )
    BashOperator(
        task_id="k8s_task",
        bash_command="hostname; sleep 5",
        executor="KubernetesExecutor",
    )
```

**관찰**:
- `celery_task` hostname → 일반 worker Pod (예: `airflow-worker-xxxx`)
- `k8s_task` hostname → task 전용 Pod (예: `xxxxxxxx-yyyyyyyy-...` 다른 형식)
- 두 hostname 이 다름 = Pod 분리 확인

**결과** _(채울 예정)_:

| task | hostname | Pod 형식 |
|---|---|---|
| celery_task | | |
| k8s_task | | |

### D. Deferrable Sensor 동작 ⭐

**목표**: deferrable sensor 가 worker slot 안 잡고 Triggerer 가 처리하는지.

**DAG**:
```python
from airflow.providers.standard.sensors.time import TimeDeltaSensorAsync

with DAG("poc_deferrable_sensor", ...) as dag:
    # 동시에 10개 sensor 시작 — 각 30분 wait
    for i in range(10):
        TimeDeltaSensorAsync(
            task_id=f"wait_{i}",
            delta=timedelta(minutes=30),
        )
```

**관찰**:
- trigger 후 → 10개 sensor 가 "deferred" 상태로 (running 아님)
- 모니터링 탭 → 작업자 수 변화 없음 (sensor 가 worker 안 잡음)
- 모니터링 탭 → Triggerer CPU/메모리 사용 증가
- 30분 후 → sensor 들이 일제히 success

**결과** _(채울 예정)_:

| 항목 | 측정 |
|---|---|
| Worker autoscale 증가? | |
| Triggerer 부하 증가? | |
| 10개 sensor 동시 처리 가능? | |
| 100개로 늘리면? | |

### E. Pool 동시성 제어

**목표**: Composer 에서 Pool 동작 확인 (queue 분리 못 하는 부분의 우회).

**작업**:
1. Airflow UI → Admin → Pools → "heavy_pool" 생성, slot=2
2. DAG:
```python
with DAG("poc_pool", ...) as dag:
    for i in range(10):
        BashOperator(
            task_id=f"slow_{i}",
            bash_command="sleep 30",
            pool="heavy_pool",
        )
```
3. trigger → 동시 2개만 running, 나머지 queued 확인

**결과** _(채울 예정)_: Pool 동작 OK / 이슈

## Capacity sizing 분석

기존 사내 총 capacity (Hive 영역 폐기 후):

```
cloud(6) + http(5) + sensor(40) = 51 task 동시
```

Composer 에서 동등 capacity:

### 옵션 A. 모두 Celery 로 흡수

```
worker Pod 당 concurrency = 12 (default)
51 / 12 ≈ 5개 → autoscale max = 5
```

→ 비용: worker Pod 5개 상시 (peak 시), 평소엔 1~2개

### 옵션 B. Sensor 만 deferrable 로 분리 ⭐

```
cloud(6) + http(5) = 11 task → worker Pod 1~2개
sensor(40) → Triggerer 1개 (lightweight)
```

→ 비용: **worker pool 대폭 축소** + Triggerer (이미 default 포함)

→ **옵션 B 권장**. 비용/성능 모두 유리.

## 회의에서 답할 메시지 _(검증 후 확정)_

> **사내 5종 queue 패턴이 Composer 에서 동작하는가?**
> Queue 분리는 ❌ (Composer 는 단일 `default` queue 강제). 분리는 **executor / Pool / Triggerer** 3가지로 분산.
>
> **5종 매핑 결과**:
> - `hadoop` / `doopey` → 폐기 (Hive 종료)
> - `cloud` / `http` → Celery worker autoscale 흡수
> - **`sensor:40` → deferrable Sensor + Triggerer (코드 수정 필요)** ⭐
> - heavy task (있다면) → `executor="KubernetesExecutor"`
>
> **마이그레이션 추정** (queue 영역):
> - sensor 코드 인벤토리 + deferrable 전환: 1~2주
> - DAG 의 queue 파라미터 제거 (cloud/http/hadoop/doopey 라벨): 0.5주
> - heavy task 인벤토리 + executor 지정 (있다면): 0.5주
>
> 총 1.5~3주.

## 관련 노트

- [[README]] — 본 PoC 의 전체 흐름 (Step 4)
- [[../3_Executor 종류 및 비교]] — Celery vs KubernetesExecutor 상세
- [[../4_Queue 라우팅과 Pod 스펙 설정]] — Pod 사양 / executor_config 패턴
- [[../9_Airflow Asset과 Dataset]] — Asset-based scheduling (sensor 대체 가능성)
- [[../2_Cloud Composer vs Self-managed 비교]] — 표 B-5 "Worker queue 5종 → 패턴 전환"
- [[01_airflow3_compat_grep]] — 코드 인벤토리 (sensor 패턴 grep)

## 시연용 자료 (회의)

- 작업자 autoscale 그래프 (시나리오 A) — Composer 모니터링 탭 캡쳐
- 같은 worker pool 로 흡수되는 hostname 비교 (시나리오 B)
- KubernetesExecutor Pod 분리 hostname (시나리오 C)
- deferrable sensor 다량 처리 → worker 부하 없음 (시나리오 D) ⭐
- Pool 동시성 제한 동작 (시나리오 E)
