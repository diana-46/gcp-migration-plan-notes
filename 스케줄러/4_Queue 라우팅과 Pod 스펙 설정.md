---
title: "Queue 라우팅과 Pod 스펙 설정"
status: draft
tags:
  - confluence
  - airflow
  - 스케줄러
created: 2026-05-11
updated: 2026-05-11
source: https://kakaoent.atlassian.net/wiki/spaces/DP/pages/5068882427/Airflow+Queue+Pod
confluence_id: 5068882427
space_id: 365101058
space_key: DP
version: 1
---

# Queue 라우팅과 Pod 스펙 설정

> CeleryKubernetesExecutor에서 task가 Celery로 갈지 K8s Pod로 갈지 어떻게 결정하나, Pod 스펙은 어떻게 차별화하나.

## 1. Celery vs Pod 라우팅: `queue` 파라미터

Airflow가 task의 "무거움"을 자동 판단하지 않는다. **작성자가 명시적으로** `queue`를 지정.

task의 queue 값
├─ queue == 'kubernetes' (airflow.cfg에서 지정한 이름)
│ → K8s Pod 생성
│
└─ 그 외 queue (default, sensor 등)
→ Celery 워커가 처리

### airflow.cfg

ini[core]
executor = CeleryKubernetesExecutor
[celery\_kubernetes\_executor]
# K8s로 보낼 queue 이름 (단일 string만, 콤마로 여러 개 X)
kubernetes\_queue = kubernetes

K8s queue는 **하나만**. 여러 K8s queue 보내고 싶으면 같은 queue 안에서 Pod 스펙으로 차별화. (아래 참고)

### 코드 예시

py# Celery 워커가 실행 (기본)
quick\_task = BashOperator(
bash\_command='echo hello',
# queue 안 쓰면 default → Celery
)
# K8s Pod로 실행
heavy\_task = BashOperator(
bash\_command='dbt run --select large\_model',
queue='kubernetes', # ← 이게 trigger
)

### 어떤 task를 어디로 보낼지

| 상황 | Celery | Pod |
| --- | --- | --- |
| 실행 시간 < 1분 | ✅ | ❌ (오버헤드 큼) |
| 실행 시간 > 10분 | ⚠️ | ✅ |
| 메모리 > 4GB | ❌ | ✅ |
| Sensor (대기) | ✅ | ❌ |
| 특수 라이브러리 | ❌ | ✅ (커스텀 이미지) |
| 격리 중요 | ❌ | ✅ |
| 빈도 높음 (분당 N개) | ✅ | ❌ |

## 2. Pool / Queue 관리 위치

### Pool

**Airflow UI에서 관리** (Composer/Self-managed 동일).

Airflow UI → Admin → Pools
- Pool 이름
- Slot 수 (동시 실행 제한)

GCP Console에는 없음. Airflow concept.

### Queue (Celery worker 분리)

**Self-managed**: worker마다 다른 queue listen 가능

bashairflow celery worker --queues=default --concurrency=8
airflow celery worker --queues=sensor --concurrency=32
airflow celery worker --queues=high\_priority --concurrency=4

→ queue별로 worker 수, 노드 사양 다르게 가능.

**Cloud Composer 2**: worker 단일 deployment → queue별 분리 어려움. `queue='kubernetes'`로만 Pod 분리 가능.

## 3. Pod 스펙 설정 방법

3단계: **Default → Override → Factory**

### 3-1. Default Pod Template

모든 K8s task에 적용되는 기본.

`pod_template.yaml`:

yamlapiVersion: v1
kind: Pod
metadata:
name: airflow-task-default
spec:
containers:
- name: base
image: airflow:2.x
resources:
requests:
memory: "512Mi"
cpu: "500m"
limits:
memory: "2Gi"
cpu: "1"
serviceAccountName: airflow-worker
restartPolicy: Never

airflow.cfg:

ini[kubernetes\_executor]
pod\_template\_file = /opt/airflow/pod\_templates/pod\_template.yaml

### 3-2. Task별 Override (executor\_config)

pyfrom kubernetes.client import models as k8s
heavy\_task = BashOperator(
queue='kubernetes',
bash\_command='dbt run --select huge\_model',
executor\_config={
'pod\_override': k8s.V1Pod(
spec=k8s.V1PodSpec(
containers=[k8s.V1Container(
name='base',
resources=k8s.V1ResourceRequirements(
requests={'memory': '8Gi', 'cpu': '4'},
),
)]
)
)
},
)

Override 가능: resources, image, node\_selector, tolerations, affinity, env, volumes, service\_account 등 거의 모든 K8s Pod 옵션.

### 3-3. 재사용 패턴 (Python Factory) — 권장

각 task마다 V1Pod 작성하면 코드 폭증. 자주 쓰는 스펙을 함수/상수로.

py# common/pod\_configs.py
from kubernetes.client import models as k8s
def pod\_config(memory='2Gi', cpu='1', node\_selector=None, image=None):
spec = k8s.V1PodSpec(
containers=[k8s.V1Container(
name='base',
image=image,
resources=k8s.V1ResourceRequirements(
requests={'memory': memory, 'cpu': cpu},
limits={'memory': memory, 'cpu': cpu},
),
)],
node\_selector=node\_selector or {},
)
return {'pod\_override': k8s.V1Pod(spec=spec)}
# 프리셋
SMALL = pod\_config(memory='1Gi', cpu='0.5')
MEDIUM = pod\_config(memory='4Gi', cpu='2')
LARGE = pod\_config(memory='16Gi', cpu='8')
HEAVY = pod\_config(memory='32Gi', cpu='16', node\_selector={'workload': 'heavy'})
GPU = pod\_config(memory='16Gi', cpu='4', node\_selector={'gpu': 'true'})

DAG에서:

pyfrom common.pod\_configs import SMALL, MEDIUM, LARGE, HEAVY
light\_task = BashOperator(
queue='kubernetes',
executor\_config=SMALL,
bash\_command='echo hello',
)
heavy\_task = BashOperator(
queue='kubernetes',
executor\_config=HEAVY,
bash\_command='process\_huge.py',
)

→ DAG 작성자는 어떤 사이즈를 쓸지만 고민.

## 4. Pod 많을 때의 부담

| 부담 받는 곳 | 영향 |
| --- | --- |
| Airflow Scheduler | Pod 생성/모니터링 latency 증가 |
| K8s API Server | Pod CRUD 폭주, etcd 부담 |
| K8s Scheduler | Pod placement 지연 |
| Node Provisioning | 노드 부족 시 autoscaler 발동 (30초~2분) |
| Container Registry | 이미지 pull throttling |
| Airflow DB | task\_instance write 폭증 |

특히 **짧은 task가 매번 Pod로 생성되면 손해**. Hybrid 전략의 가치.

## 5. 부담 완화 방법

### 1) 동시 실행 제한

ini# airflow.cfg
[core]
parallelism = 200 # 전체 동시 task
max\_active\_tasks\_per\_dag = 50
max\_active\_runs\_per\_dag = 5
[kubernetes\_executor]
worker\_pods\_creation\_batch\_size = 10

Pool로도:

pyheavy\_task = BashOperator(
pool='heavy\_pool', # 동시 5개만
queue='kubernetes',
)

### 2) 이미지 Pre-pull (DaemonSet)

yamlapiVersion: apps/v1
kind: DaemonSet
metadata:
name: image-prepull
spec:
template:
spec:
initContainers:
- name: prepull-dbt
image: myorg/dbt:latest
command: ['echo', 'pulled']
containers:
- name: pause
image: k8s.gcr.io/pause:3.9

→ Pod 시작 시간 30초 → 5초.

### 3) Node Pool 분리

default-pool: n2-standard-4 (일반 task)
heavy-pool: n2-highmem-8 (taint: workload=heavy)
spot-pool: n2-standard-4 (spot, 짧은 task)

executor\_config의 `node_selector`/`tolerations`로 라우팅.

### 4) Hybrid 전략 (가장 중요)

짧은 task → Celery (Pod 안 만듦)
무거운 task만 → K8s Pod
→ Pod 수 자체를 줄이는 가장 큰 방법

### 5) Resource Quota (폭주 방지)

yamlapiVersion: v1
kind: ResourceQuota
metadata:
name: airflow-quota
spec:
hard:
pods: "500"
requests.cpu: "200"
requests.memory: "400Gi"

## 6. 권장 운영 패턴

1. **Hybrid**: 대부분 Celery, 진짜 무거운 것만 Pod
2. **Pod 스펙 factory**: SMALL/MEDIUM/LARGE/HEAVY 프리셋
3. **Pool로 동시 실행 제한**
4. **Node pool 분리** (일반/heavy/spot)
5. **이미지 pre-pull** (DaemonSet)
6. **모니터링**: Pod 생성 latency, API server requests, scheduler heartbeat

## 7. 모니터링 메트릭

필수:
- Pod 생성 → ready 시간 (P50, P95, P99)
- K8s API server requests/sec
- Airflow scheduler heartbeat
- 노드 utilization
- Pending Pod 수
알람:
- Pod 생성 P95 > 60초
- Pending Pod > 50개
- Scheduler heartbeat 지연

## 관련 문서

- [[3_Executor 종류 및 비교]]
- [[2_Cloud Composer 2 vs Self-managed 비교]]
- [[5_Metadata DB 운영]]
