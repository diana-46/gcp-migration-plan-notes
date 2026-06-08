---
title: "08. Deferrable Sensor 동작 검증 — Traditional vs Triggerer 비교"
status: in_progress
tags:
  - poc
  - sensor
  - trigger
  - deferrable
  - triggerer
  - composer3
created: 2026-06-04
updated: 2026-06-04
---

# 08. Deferrable Sensor 동작 검증 — Traditional vs Triggerer 비교

> **검증 질문**: 같은 sensor 를 **Poke 모드(기존)** 와 **Deferrable 모드** 로 동시에 띄웠을 때, 워커 슬롯 점유 / 상태 표시 / 리소스 사용 차이가 실제로 발생하는가?
>
> **답 (예상)**: ✅ Traditional 은 **Running (초록색)** 으로 워커 슬롯 점유, Deferrable 은 **Deferred (보라색)** 로 즉시 반납 → Triggerer 가 비동기 처리.
>
> [[04_worker_pool_queue]] 에서 "Deferrable sensor + Triggerer 정상" 발견(4번)을 시각적으로 1:1 비교 검증하는 후속 PoC. [[../7_2_리소스 다이어트 포인트]] 의 전제 검증.

## 검증 의도

[[../7_2_리소스 다이어트 포인트]] 에서 정리한 sensor 다이어트의 효과를 **눈으로 확인**:

- Traditional Sensor (Poke 모드): 20분 대기 = Worker 슬롯 20분 점유 → 메모리 4 GB 점유
- Deferrable Sensor: Triggerer 에 위임 → Worker 슬롯 즉시 반납 → 메모리 점유 거의 0

사내 sensor 들이 `wait_45_min` 같이 긴 대기를 가지므로, 이 차이가 워커 다이어트의 핵심 lever.

## 환경

| 항목 | 값 |
|---|---|
| Composer | 3.1.7-build |
| Airflow | 3.1.7 |
| DAG 파일 위치 | `~/dev/airflow/poc_trigger.py` |
| GCS bucket | _(채울 자리)_ |

## 테스트 DAG

```python
import pendulum
from datetime import timedelta
from airflow import DAG
# Airflow 3 경고 메시지에 나온 최신 표준 임포트 경로
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor

with DAG(
    dag_id='sensor_vs_triggerer_v3_final',
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,                                       # Airflow 3 표준 (schedule_interval 대신)
    catchup=False,
    tags=['poc', 'airflow3', 'perfect_test'],
) as dag:

    # 1. [AS-IS] 기존 방식: 워커 슬롯을 20분 동안 꽉 쥐고 대기 (Running - 연두색)
    traditional_sensor = TimeDeltaSensor(
        task_id='1_traditional_sensor',
        delta=timedelta(minutes=20)
    )

    # 2. [TO-BE] Triggerer 방식: 워커 자원 즉시 반납 후 비동기 대기 (Deferred - 보라색)
    # 대기 시간을 20분으로 늘렸기 때문에, 무조건 트리거러로 토스됩니다!
    deferrable_sensor = TimeDeltaSensor(
        task_id='2_deferrable_sensor',
        delta=timedelta(minutes=20),
        deferrable=True
    )

    # 두 태스크를 동시에 실행하여 비교
    [traditional_sensor, deferrable_sensor]
```

### 핵심 설계

| 변수                    | 선택 / 의도                                                             |
| --------------------- | ------------------------------------------------------------------- |
| `TimeDeltaSensor`     | 가장 단순한 sensor — 외부 의존성 없이 시간만 대기                                    |
| `delta=20분`           | **충분히 긴 대기 시간** — Triggerer 토스를 확실히 유도 (짧으면 trigger 진입 전에 끝남)       |
| 같은 DAG 안 2 task 병렬    | UI 에서 동시 비교 가능, 환경 변수 동일                                            |
| Airflow 3 import path | `airflow.providers.standard.sensors.time_delta` (deprecation 경고 우회) |
| `schedule=None`       | 수동 trigger 로 테스트                                                    |

## 실행 방법

```bash
# 1. DAG 파일 GCS bucket 에 업로드
gsutil cp ~/dev/airflow/poc_trigger.py gs://<composer-bucket>/dags/

# 2. Airflow UI 에서 1~2분 대기 후 DAG 보이는지 확인
#    → dag_id: sensor_vs_triggerer_v3_final

# 3. 수동 trigger
#    UI 의 ▶️ 버튼 또는:
gcloud composer environments run <env-name> --location asia-northeast3 \
    dags trigger -- sensor_vs_triggerer_v3_final
```

## 관찰 포인트

### 1. DAG UI 의 task 상태 색깔

| Task | 예상 상태 | 색깔 | 의미 |
|---|---|---|---|
| `1_traditional_sensor` | **Running** | 🟢 연두색 | Worker slot 점유 중 |
| `2_deferrable_sensor` | **Deferred** | 🟣 보라색 | Triggerer 가 비동기 대기 중 |

→ 두 task 가 같은 시간 같은 조건이지만 **상태 색깔이 다르면** 동작 차이 실증.

### 2. Worker pod 의 메모리/슬롯

Composer 의 **모니터링 → Celery Executor 작업자** 화면에서:

- traditional_sensor 실행 중: Celery worker 의 **active task = 1** (슬롯 점유)
- deferrable_sensor 실행 중: Celery worker 의 **active task = 0** (즉시 반납)

20분 동안 worker 메모리 점유 차이 → 점유한 4 GB 가 즉시 해제되는지.

### 3. Triggerer 로그

```bash
gcloud composer environments storage logs read <env-name> \
    --location asia-northeast3 \
    --filter "resource.labels.task_id=triggerer"
```

→ `Triggering job N (sensor_vs_triggerer_v3_final.2_deferrable_sensor)` 류 로그가 떠야 함.
→ traditional_sensor 는 Triggerer 로그에 안 나옴 (워커에서 직접 처리).

### 4. Task duration / log

- 두 task 모두 **약 20분 후 동시 완료** 되어야 함 (delta 동일)
- 결과는 같지만 **자원 점유 패턴이 달라야** PoC 성공

## 기대 결과

| 항목 | Traditional (Poke) | Deferrable (Trigger) |
|---|---|---|
| 상태 표시 | Running (🟢) | Deferred (🟣) |
| Worker 슬롯 점유 | 20분 내내 | 0 |
| Worker 메모리 점유 | ~4 GB × 20분 | 거의 0 |
| Triggerer 로그 | 없음 | 등장 |
| 완료 시간 | ~20분 | ~20분 |

→ **결과는 같은데 자원 비용이 완전히 다름** = sensor 다이어트의 효과 실증.

## 실측 결과 (테스트 중)


![[Pasted image 20260604171427.png]]
![[Pasted image 20260604171603.png]]
직접 워커에서 실행하는 구조가 됨

![[Pasted image 20260604171728.png]]
trigger 쪽으로 넘기고 defered 상태로 변경

![[Pasted image 20260604171946.png]]
trigger 쪽에서는 1분에 한번씩 검사하는 것으로 보임
![[Pasted image 20260604173429.png]]
성공하면 성공, 실패하면 똑같이 airflow 의 retry 로직에 의해 재실행 된다고 함.

### Worker pod 점유

![[Pasted image 20260604173652.png]]

![[Pasted image 20260604173710.png]]



## 함의 — 사내 적용 시

[[../7_2_리소스 다이어트 포인트]] 와 연결:

| Sensor 타입 | 적용 방법 |
|---|---|
| `TimeDeltaSensor` | `deferrable=True` 한 줄 — ✅ 본 PoC 로 검증 |
| `ExternalTaskSensor` | `deferrable=True` 한 줄 — 별도 PoC 권장 (외부 DAG 의존 패턴 검증) |
| `S3KeySensor` / `GCSObjectExistenceSensor` | `deferrable=True` 한 줄 |
| `AthlonQuerySensor` (사내 커스텀) | trigger 자체 작성 필요 — 별도 검증 (Phase 2) |
| `BigqueryQuerySensor` (사내 커스텀) | 표준 `BigQueryTablePartitionExistenceSensor` 로 갈음 또는 자체 trigger |

본 PoC 성공 시 **표준 sensor 들은 즉시 일괄 전환 가능** 결론.

## 잔여 검증 (회의 이후 진행 가능)

- [ ] `ExternalTaskSensor` deferrable 동작 (외부 DAG 의존)
- [ ] **Triggerer HA** — replicas ≥ 2 구성 시 동작
- [ ] **Triggerer `default_capacity` 튜닝** — 수백 ~ 수천 trigger 동시 부하 시
- [ ] 사내 커스텀 sensor (`AthlonQuerySensor`) 의 async 마이그레이션 설계

## 관련 문서

- [[../7_1_실제 스펙 산정]] — 옵션 B (sensor 다이어트로 ₩200만/월 절감) 의 전제
- [[../7_2_리소스 다이어트 포인트]] — sensor → trigger 전환 전체 가이드
- [[04_worker_pool_queue]] — Triggerer 기본 동작 검증 (선행 PoC)
- [[../6_Airflow 2 vs 3 비교]] — Airflow 3 의 Triggerer / Deferrable 변화

## 참고

- [Airflow Deferrable Operators 공식 문서](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/deferring.html)
- [Triggerer 컴포넌트 가이드](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/triggerer.html)
