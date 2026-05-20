---
title: "04. Queue / Worker / Pool 패턴 검증"
status: done
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
> **답 (확정)**: Queue 분리는 ❌ **단순 무시가 아니라 묵음 실패** — `default` 외 queue 는 영원히 `queued` 상태로 stuck. 분리는 **executor + Pool + Triggerer** 3가지로 분산. 단 **KubernetesExecutor 는 cold start 7~10분 + idle 즉시 deprovision → 실용성 매우 제한적**. 사내 케이스는 사실상 Celery + Triggerer 만으로 충분.

## 핵심 발견 요약 (회의 1슬라이드)

1. ⚠️ **`task.queue='foo'` 가 묵음 실패** — `default` 외 queue 의 task 는 에러 없이 영원히 queued. **모든 `queue=` 파라미터 제거 필수**.
2. ⚠️ **KubernetesExecutor cold start 7분 46초 측정** — Autopilot 노드 provisioning + image pull. Idle 시 노드 즉시 deprovision → warm start 불가. **분 단위 이하 task 에 사실상 사용 불가**.
3. ✅ **Celery worker autoscale 정상** (1→3 까지 관찰), 모니터링 탭에서 Pod 단위 상태 확인 가능
4. ✅ **Deferrable sensor + Triggerer 정상** — worker 안 잡고 처리. `sensor:40` 패턴의 정답.
5. ✅ **Airflow Pool 정상** — slot=2 정확히 강제. queue 분리 못 하는 부분의 capacity 우회.

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
| `cloud:6` | 6 | Celery worker (단일) | **`queue=` 파라미터 제거 필수** ⚠️ |
| `http:5` | 5 | Celery worker (단일) | **`queue=` 파라미터 제거 필수** ⚠️ |
| **`sensor:40`** | **40** | **deferrable Sensor + Triggerer** ⭐ | **코드 수정 필요** |
| (heavy task?) | — | KubernetesExecutor 비추 (cold start 7~10분), Celery worker 사양 상향 권장 | 검토 필요 |

**핵심 작업**:
1. **`queue=` 파라미터 일괄 제거** — 안 그러면 task 가 묵음으로 stuck (시나리오 B 확정)
2. **`sensor:40` → deferrable 전환** — 코드 한 줄 (`deferrable=True`) 또는 글로벌 `default_deferrable=True`

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

### A. Celery worker autoscale 관찰 ⭐ ✅

**목표**: 부하 시 worker Pod 가 1→3 으로 autoscale 되는지 + capacity 분포 확인.

**DAG**: `~/PycharmProjects/composer-poc-pkg/dags/poc_celery_autoscale.py` (30개 task × `sleep 60`)

**결과** ✅:

| 항목 | 측정값 |
|---|---|
| Peak Pod 갯수 | **4** (max=3 설정인데 일시 초과 — rolling/graceful replacement) |
| Pod 상태 분포 (스냅샷) | OK 1 + Pending 1 + 완료 2 |
| Pod 이름 형식 | `airflow-worker-<random5char>` |
| Pod 사양 | 0.5 CPU / 2.5GB |
| Active Pod CPU 사용률 | 8.87% (= ~0.04 vCPU 실사용) |
| Active Pod 메모리 사용률 | 21.94% (= ~550MB 실사용) |
| Autoscale up 트리거 시간 | trigger 후 1~3분 이내 |
| Cool-down (1로 복귀) | task 완료 후 ~5분 |

**관찰**:
- 콘솔 → 환경 → **모니터링** 탭 → "**Celery Executor 작업자**" 화면에서 Pod 단위 상태 확인 가능 ⭐
- Composer 3 가 GKE kubectl 막아놨지만 이 화면이 **kubectl 의 실용적 대체**
- CPU/메모리 여유 매우 큼 → 더 큰 부하 줘도 OK, 또는 worker 사양 줄여서 비용 절감 가능

**시연 자료**: "Celery Executor 작업자" 화면 스크린샷 + autoscale 시계열 그래프.

### B. `task.queue='foo'` 무시되는지 — 🚨 결정적 발견

**목표**: 사용자 정의 queue 이름이 Composer 에선 의미 없음을 확인.

**DAG**: `~/PycharmProjects/composer-poc-pkg/dags/poc_queue_ignored.py` (4개 task, 각각 다른 queue 이름)

**결과** ❌ — **단순 "무시" 가 아니라 "묵음 실패"**:

| task | queue 설정 | 결과 |
|---|---|---|
| `default_q` | `default` | ✅ **success** (즉시 실행) |
| `hadoop_q` | `hadoop` | ⏸ **queued 영원히 stuck** |
| `sensor_q` | `sensor` | ⏸ **queued 영원히 stuck** |
| `fake_q` | `nonexistent_queue` | ⏸ **queued 영원히 stuck** |

→ **Composer 의 worker 는 `default` queue 만 listen**. 다른 queue 로 보낸 task 는:
- Redis broker 에 적재됨 ✅
- 아무도 가져가지 않음 → `queued` 상태 영원히 ❌
- **에러 X / 경고 X / 모니터링 알람 X** (묵음)
- DAG run 은 timeout 까지 running 상태 유지

### ⚠️ 위험 — 마이그레이션 시 가장 함정

사내 코드에 `queue=` 파라미터가 남아있으면 **이관 직후 묵음으로 stuck**. 운영자가 늦게 발견.

**필수 작업**:
```bash
cd ~/PycharmProjects/airflow-dags

# 모든 queue= 사용 위치 추출
grep -rn "queue=" . --include="*.py" | grep -v "task_queue"

# queue 별 분포
grep -rno "queue=['\"]\\w\\+['\"]" . --include="*.py" | \
  awk -F: '{print $NF}' | sort | uniq -c | sort -rn
```

→ 결과 기반으로 모든 task 의 `queue=` 파라미터 제거. CI lint 로 재발 방지 권장.

**시연 자료**: Graph view 의 1 success + 3 queued 패턴.

### C. `executor="KubernetesExecutor"` Pod 분리 ✅ + ⚠️ cold start

**목표**: KubernetesExecutor 가 task 마다 별도 Pod 띄우는지 + 실 startup 시간 측정.

**DAG**: `~/PycharmProjects/composer-poc-pkg/dags/poc_k8s_executor.py` (celery_task + k8s_task)

**결과** ✅ Pod 분리 확인, ⚠️ cold start 매우 큼:

| task | trigger → 시작까지 | 실제 작업 | Pod 종류 |
|---|---|---|---|
| `celery_task` | **23초** | 5.2초 | 일반 worker pod (이미 떠있음) |
| `k8s_task` (cold) | **7분 46초 (466초)** ⚠️ | 6.9초 | 신규 Pod `airflow-k8s-worker-poc-k8s-executor-k8s-task-g3zlp2ri` |

**K8s Pod 사양** (모니터링 탭 → "Kubernetes 실행자 작업자"):
- 0.25 CPU / 2GB (Celery worker 의 0.5 CPU / 2.5GB 와 다른 default)
- Pod 이름에 DAG ID + task ID 박혀있음 (1:1 추적 가능)

### ⚠️ 결정적 함정 — Idle 시 노드 즉시 deprovisioning

작업 종료 후 잠시 후 노드 deprovisioned. **warm start 실현 거의 안 됨** → 다음 K8s task 도 cold start 7~10분.

```
첫 K8s task    cold 7~10분  → 작업 → 노드 사라짐
다음 K8s task  또 cold 7~10분 ⚠️
```

### 사내 케이스 의미

```
task 실제 작업 시간 ≪ Pod 생성 오버헤드 (466초)
  → 5분 task: 손해 (오버헤드 1.5배)
  → 30분 task: ignorable (오버헤드 25%)
  → 1시간+ task: K8s 가 격리/사양 측면에서 의미

사내 task 유형 (dbt+BQ 이관 후):
  ├─ BigQuery query: BQ 가 실제 작업 → Airflow task 는 dispatch/wait (가벼움)
  ├─ Sensor: deferrable 로 worker 안 잡음
  ├─ HTTP/Slack API: 즉시 (가벼움)
  └─ heavy task: 거의 없음

→ K8s executor 거의 안 쓸 가능성. 필요시 Celery worker 사양 상향이 나은 선택.
```

**시연 자료**: "Kubernetes 실행자 작업자" 화면 (Pending 상태 K8s pod) + DAG run timeline (Duration 7분 53초).

### D. Deferrable Sensor 동작 ⭐ ✅

**목표**: deferrable sensor 가 worker slot 안 잡고 Triggerer 가 처리하는지.

**DAG**: `~/PycharmProjects/composer-poc-pkg/dags/poc_deferrable_sensor.py` (10개 sensor × 5분 wait)

**참고 — 첫 시도 import 에러**:
```
ImportError: cannot import name 'TimeDeltaSensorAsync' from 'airflow.providers.standard.sensors.time'
```
→ Airflow 3 에선 `*Async` 클래스 deprecated. **`TimeDeltaSensor(deferrable=True)`** 패턴이 표준.

수정된 import:
```python
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor

TimeDeltaSensor(
    task_id="wait",
    delta=timedelta(minutes=5),
    deferrable=True,
)
```

**결과** ✅:

| 항목 | 측정 |
|---|---|
| Task 상태 | **`deferred`** (running 아님) ⭐ |
| Worker autoscale | **변화 없음** (sensor 가 worker 안 잡음) ⭐ |
| Triggerer 부하 | 약간 증가 (10개 처리에도 미미) |
| 10개 sensor 동시 처리 | ✅ 무리 없이 (Triggerer 1개로) |
| 5분 후 일제 success | ✅ |

### Airflow 3 의 DEFERRED 상태 — 핵심 메커니즘

| 상태 | Worker slot? | 의미 |
|---|---|---|
| `queued` | 대기 중 | worker slot 기다림 |
| `running` | ✅ 점유 | 실제 실행 중 |
| **`deferred`** | ❌ 안 점유 | **Triggerer 에게 위임, worker 자유** |

→ 일반 sensor: 5분 wait 동안 worker 슬롯 점유 (낭비)
→ deferrable sensor: 즉시 슬롯 반납하고 Triggerer 에 위임 → 100개 wait 도 worker 0 점유

### 사내 `sensor:40` 의 답

```
기존: sensor worker 1대 (concurrency 40) — 40개 sensor 가 worker capacity 다 점유
Composer: deferrable + Triggerer — worker 0 점유, Triggerer 1대로 수천 처리
```

**전환 방법 3가지**:
1. **글로벌 설정**: `default_deferrable = True` (Composer "Airflow 구성 재정의" 탭) — 가장 빠름
2. **sensor 별 `deferrable=True` 명시** — 안전하지만 작업량
3. **사내 자체 sensor 마이그레이션** — `defer()` 메서드 구현 (가장 큰 작업)

→ 추천: **글로벌 설정 + 사내 자체 sensor 점진 마이그레이션**.

**시연 자료**: Graph view 의 10개 task 가 `deferred` 상태 + 모니터링 탭 작업자 수 변화 없음.

### E. Pool 동시성 제어 ✅

**목표**: Composer 에서 Pool 동작 확인 (queue 분리 못 하는 부분의 capacity 우회).

**DAG**: `~/PycharmProjects/composer-poc-pkg/dags/poc_pool.py` (10개 task × `sleep 30`, pool="heavy_pool" slot=2)

**선행**: Airflow UI → Admin → Pools → `heavy_pool` (slot=2) 생성.

**결과** ✅:

| 항목 | 측정값 |
|---|---|
| 10개 task 모두 success | ✅ |
| 총 Duration | **2분 57초** (= 30초 × 5 batch + 약간의 scheduler overhead) |
| 동시 running task peak | **2** (= slot 수와 정확히 일치 ⭐) |
| Batch 패턴 | slow_00+01 → slow_02+03 → ... → slow_08+09 |
| 모니터링 "실행 중인 태스크" 그래프 | 2 로 plateau |

→ Pool 동시성 제한 **정확히 동작**. Composer 의 단일 worker pool 환경에서도 task 그룹별 capacity 제어 가능.

**시연 자료**: 모니터링 탭 "Airflow 태스크" 그래프 (실행 중 = 2 plateau) + DAG run 결과 (10개 success, 2:57).

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

## 회의에서 답할 메시지 (검증 완료)

> **Q1: 사내 5종 queue 패턴이 Composer 에서 동작하는가?**
> ❌ 동작 안 함 — Composer worker 는 `default` queue 만 listen. 다른 queue 이름 박힌 task 는 **묵음으로 stuck** (에러 X). 마이그레이션 시 모든 `queue=` 파라미터 제거 필수.
>
> **Q2: 5종을 Composer 에서 어떻게 매핑?**
> - `hadoop` / `doopey` → 폐기 (Hive 종료)
> - `cloud` / `http` → Celery worker autoscale 흡수, queue 파라미터 제거
> - **`sensor:40` → deferrable Sensor + Triggerer** ⭐ (worker 0 점유)
> - heavy task → KubernetesExecutor 비추 (cold start 7~10분), Celery worker 사양 상향이 나음
>
> **Q3: Composer 의 KubernetesExecutor 가 실용적인가?**
> ❌ 매우 제한적. 측정값: cold start **7분 46초** (Autopilot 노드 provisioning + image pull). Idle 시 노드 즉시 deprovision → warm start 거의 불가. 분 단위 task 에 사실상 사용 불가. **사내 케이스에선 거의 필요 없을 가능성** (BQ 가 진짜 작업 함).
>
> **Q4: Composer 의 동시성 제어는?**
> ✅ Airflow Pool 정상 동작 (slot 수만큼 정확히 enforced). queue 분리 못 하는 부분의 capacity 우회 가능.
>
> **Q5: Composer 3 의 디버깅 가시성?**
> - kubectl 직접 접근 ❌ (Composer 2 와의 큰 차이)
> - 단 모니터링 탭의 **"Celery Executor 작업자"** / **"Kubernetes 실행자 작업자"** 화면이 Pod 단위 상태 노출 → kubectl 의 실용적 대체.

### 마이그레이션 작업량 추정

| 작업 | 추정 |
|---|---|
| 모든 DAG 의 `queue=` 파라미터 제거 (CI lint 포함) | 0.5~1주 |
| sensor 코드 인벤토리 + `deferrable=True` 일괄 적용 (또는 `default_deferrable=True` 글로벌) | 1~2주 |
| 사내 자체 sensor 클래스 마이그레이션 (`defer()` 구현) | 0.5~1주 |
| heavy task 인벤토리 + worker 사양 조절 검토 | 0.5주 |
| **총 (queue 영역만)** | **2.5~4.5주** |

## 관련 노트

- [[README]] — 본 PoC 의 전체 흐름 (Step 4)
- [[../3_Executor 종류 및 비교]] — Celery vs KubernetesExecutor 상세
- [[../4_Queue 라우팅과 Pod 스펙 설정]] — Pod 사양 / executor_config 패턴
- [[../9_Airflow Asset과 Dataset]] — Asset-based scheduling (sensor 대체 가능성)
- [[../2_Cloud Composer vs Self-managed 비교]] — 표 B-5 "Worker queue 5종 → 패턴 전환"
- [[01_airflow3_compat_grep]] — 코드 인벤토리 (sensor 패턴 grep)

## 시연용 자료 (회의)

| # | 자료 | 메시지 |
|---|---|---|
| 1 | 모니터링 탭 "Celery Executor 작업자" 화면 (A) | "Composer 에서도 Pod 상태 확인 가능 — kubectl 봉인이지만 UI 가 대체" |
| 2 | Graph view: 1 success + 3 queued stuck (B) | "queue 이름 박힌 task 는 묵음으로 stuck — 마이그레이션 함정" |
| 3 | "Kubernetes 실행자 작업자" + Duration 7:53 (C) | "K8sExecutor cold start 7~10분 — 실용성 매우 제한적" |
| 4 | Graph view 10개 deferred + autoscale 변화 없음 (D) | "sensor:40 의 답 — worker 0 점유로 처리 가능" ⭐ |
| 5 | "실행 중인 태스크 = 2 plateau" 그래프 (E) | "Pool 로 동시성 제어 가능" |
