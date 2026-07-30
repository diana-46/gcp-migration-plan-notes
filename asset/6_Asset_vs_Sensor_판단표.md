---
title: "6. Asset vs Sensor 판단표 — 언제 무엇을 쓰나"
status: draft
tags:
  - airflow
  - asset
  - sensor
  - decision
created: 2026-07-24
updated: 2026-07-24
---

# 6. Asset vs Sensor 판단표 — 언제 무엇을 쓰나

> Asset scheduling 은 **sensor 의 상위호환이 아니라 부분 대체**. 그것도 특정 pattern 만. 팀에서 판단할 수 있게 정리.

## 핵심 재프레이밍

기존 오해:
> "Asset 은 sensor 를 대체하는 상위 개념이다"

정확한 프레임:
> **Asset 은 DAG 을 event-driven 으로 재설계할 때 쓰는 도구고, sensor 는 여전히 time-driven DAG 안에서 조건을 기다릴 때 필요한 도구다.**

두 도구는 겹치지 않는 사용 사례가 각각 있다.

## Time-driven vs Event-driven

| 축 | Time-driven DAG | Event-driven DAG |
|---|---|---|
| 언제 시작 | Cron / interval 로 스스로 | 외부 이벤트 도착 시 |
| Prerequisite 대기 | DAG 내부에서 sensor/check | DAG 시작 자체가 "왔음" 의미 |
| 실패 시 재실행 | Backfill 로 시간대 clear | 이벤트 재발생 or 수동 trigger |
| Sensor 위치 | 어디에든 (시작/중간/끝) | 항상 DAG 시작점 |
| 사용 케이스 | 정기 리포트, 배치 집계, 자기 페이스 있는 파이프라인 | Reactive 트리거, upstream 완료 대기, 외부 이벤트 대응 |

**Asset scheduling 은 event-driven 쪽 도구**. Time-driven DAG 안의 sensor 는 대체 불가.

## Asset 이 sensor 를 자연스럽게 대체하는 케이스

**"DAG 시작점에 있는, 오직 upstream 완료를 기다리기 위해 존재하는" sensor**

```
Before (time-driven with sensor):

DAG (schedule="@daily"):
    ExternalTaskSensor("upstream_dag", execution_delta=1h)  ← 시작점, 오직 이거만
    processing_1
    processing_2

After (event-driven with asset):

DAG (schedule=[UPSTREAM_ASSET]):
    processing_1
    processing_2
```

**이 pattern 만 자연스럽게 asset 으로 넘어감**. 그리고 이건 downstream DAG 의 흔한 pattern 이라 asset 이 유용한 케이스가 많다.

## Sensor 가 대체 불가능한 3가지 케이스

### 케이스 1 — Sensor 가 DAG 중간에 있음

```
DAG (schedule="0 0 * * *"):   # 자기 스케줄
    extract_own_data          # 자기 pre-work
    transform_own_data
    ExternalTaskSensor("upstream_dag")   ← 여기서만 대기
    join_with_upstream
    publish
```

이 DAG 은:
- 자기 cron 스케줄이 있음 (daily)
- Pre-work 를 먼저 실행
- **중간에서** upstream 을 기다림

**Asset scheduling 은 "DAG 시작" 이 아니라 "DAG 중간" 에 걸 수 없다**. 이 케이스는 asset 으로 표현 불가.

**대안**:
- **Deferrable ExternalTaskSensor** — worker slot 안 잡고 trigger 로 대기. Sensor 의 자원 비용 문제는 대부분 여기서 해소
- **DAG 분할** — pre-work DAG + asset-scheduled post-work DAG (억지 분할)
- **AssetWatcher** (Airflow 3, 일부 버전) — DAG 실행 중 asset 관찰. 아직 실무 성숙도 낮음

### 케이스 2 — Sensor 가 "task 완료" 가 아니라 "데이터 조건" 을 검사

```python
BigqueryQuerySensor(
    sql="SELECT COUNT(*) FROM ... WHERE dt = '{{ ds }}'",
    condition_expression="x >= 1000",  # 데이터 조건
)
```

Asset event 는 "producer 가 완료됨" 만 알림. **"row 가 1000개 이상 쌓였을 때"** 같은 데이터 조건은 표현 불가.

**대안**:
- Producer 쪽에 조건 검사 task 를 추가하고 **그 task 에 stamp outlet** 을 붙임
- 즉 "조건 검사를 sensor (consumer) 에서 stamp (producer) 로 옮김"
- 가능하지만 producer 소유가 다른 팀이면 정치적 이슈

### 케이스 3 — Sensor 의 `execution_delta` 가 런타임 값에 의존

```python
ExternalTaskSensor(
    external_dag_id="upstream",
    execution_delta=timedelta(hours=lambda ctx: compute_offset(ctx)),  # 동적
)
```

Sensor 는 각 dag_run 별로 자기 execution_date 기준 매칭 가능. Asset 스케줄러는 이 개념 없음.

**대안**:
- Consumer 에서 코드로 매칭 (payload 로 producer 시간 받아서 뺄셈) — 가능하지만 스케줄러 수준의 매칭은 못 얻음
- DAG 시작 조건이 아닌 처리 조건이면 stamp+consumer 매칭으로 커버 가능

## 판단표

| 니즈 / 상황 | Asset scheduling | ExternalTaskSensor |
|---|:---:|:---:|
| DAG 전체가 upstream 완료로만 시작 | ✅ | ⭕ (deferrable) |
| DAG 이 자기 스케줄로 시작 후 중간에 대기 | ❌ | ✅ |
| Upstream 완료 시점 정보를 payload 로 받고 싶음 | ✅ (stamp `extra`) | ⭕ (execution_date 매칭만) |
| 데이터 조건 (count / freshness / null 없음) 검사 | ❌ | ✅ (BQ query sensor 등) |
| Cross-team, upstream 을 못 건드림 | ⭕ (producer 협조 필요) | ✅ (자립적) |
| 자원 절약 (worker slot / poke) | ✅ | ⭕ (deferrable 이면 대부분 해소) |
| Partition-specific 구독 필요 (예: EOD 만) | ✅ (AssetAlias) | ⭕ (execution_delta 로 표현) |
| Backfill 시 downstream 억제 가능 | ✅ (stamp + skip) | ⭕ (기본은 자기 백필 안 됨) |
| Cadence 가 다른 producer/consumer | ✅ (stamp + dynamic mapping) | ⭕ (execution_delta 정적) |
| 하이브리드 (cron + upstream 완료 둘 다) | ✅ `AssetOrTimeSchedule` | ⭕ (sensor + own cron) |

**범례**: ✅ 자연스럽게 지원 / ⭕ 가능하지만 제약 / ❌ 표현 불가

## Deferrable sensor 이 답인 경우

Sensor 의 자원 비용 (worker slot 점유, poke 오버헤드) 이 asset scheduling 을 도입하려는 주된 동기라면, **deferrable sensor 로 대부분 해소**된다.

```python
ExternalTaskSensor(
    external_dag_id="upstream",
    execution_delta=timedelta(hours=1),
    deferrable=True,   # ← 이거
    poke_interval=60,
)
```

Deferrable sensor 는:
- Trigger 로 이관되어 worker slot 미점유
- Poll 은 trigger 프로세스에서 asyncio 로 효율적으로
- **Sensor 의 표현력 (조건 검사, 중간 위치 등) 은 그대로 유지**

즉 "sensor 자원 문제만 걸린 경우" 라면 asset 으로 재구조화하지 말고 deferrable 을 붙이는 게 훨씬 저렴한 개선.

**Asset 으로 넘어갈 이유가 되는 케이스**:
- DAG 이 정말 event-driven 이 자연스러운 경우
- Payload 로 producer 정보를 전달받아야 하는 경우
- Partition-specific 구독이 필요한 경우 (AssetAlias)
- Cross-team producer 가 이미 asset 을 emit 하고 있는 경우

## Story 팀 DAG 이 어디에 해당하는가

### `berriz_bizberry_downstream_demo_integration`

```
schedule = [BIZBERRY_SUMMARY_ASSET, BIZBERRY_OVERVIEW_TREND_ASSET]
```

- DAG 시작점에 있는, upstream 완료만 기다리는 케이스
- **Asset scheduling 이 잘 맞는 케이스** ✅
- 다만 payload 부재 → stamp task 도입으로 개선

### `berriz_0101_bizberry_hourly_integration` (producer)

- Hourly cron 스케줄로 자기 시작
- Downstream 을 위한 outlet 발신
- Sensor 없음 (자기 페이스)
- **Time-driven 이지만 downstream 을 위해 event emit 하는 hybrid 역할**

### 만약 앞으로...

> "매일 자정에 시작해서 여러 팀 mart 를 join 하는 DAG"

- 자기 스케줄 (daily) + 여러 upstream mart 대기
- **Time-driven with sensors in the middle**
- Asset 으로 옮기지 말고 **deferrable sensor 유지** 가 자연스러움
- 또는 `AssetOrTimeSchedule` 로 hybrid 표현 시도

## 팀 가이드라인 (제안)

**Asset scheduling 을 도입하는 조건**:
1. DAG 이 오직 upstream 이벤트 대응 용도임 (자기 스케줄 필요 없음)
2. Producer 가 stamp task 로 payload 를 제공할 의지가 있음 (또는 이미 있음)
3. Partition-specific 구독이 필요하거나, 장차 필요해질 여지가 있음

**Sensor 유지가 나은 조건**:
1. DAG 이 자기 스케줄로 시작해야 함 (정기 리포트, 배치)
2. Sensor 가 DAG 중간에 있음
3. 데이터 조건 (count / freshness) 검사 필요
4. Cross-team, upstream 협조 어려움

**둘 다 고려하는 조건**:
1. Cron 트리거 + upstream 완료 둘 다 원함 → `AssetOrTimeSchedule` 검토
2. 지금은 sensor 로 시작하고, upstream 이 stamp 를 제공하면 asset 으로 전환

## 시연에서 이걸 어떻게 전달할까

시연에서 asset scheduling 을 **"sensor 완전 대체"** 로 팔지 말자. 나중에 팀들이 실패 케이스를 만나면 신뢰 잃음.

대신 이 프레임:

> "Asset 은 사용해야 할 자리가 있고, sensor 도 그대로 유효하다. 판단 기준은 위 표와 같다. 우리 팀은 downstream_demo 케이스가 asset 에 딱 맞아서 이걸로 시작했고, deferrable sensor 는 여전히 다른 DAG 에서 쓴다."

이렇게 하면 시연이 **"어느 걸 언제 써야 하는지 팀이 판단할 수 있게 되는 자료"** 가 된다. 단순 홍보 자료가 아니라.

## 관련 문서

- [[0_결론]] — 이 판단표의 요약
- [[4_해결_패턴]] — Asset 을 쓸 때의 실전 패턴
- [[../공유/10_Sensor를_Triggerer와_Asset으로_대체하기]] — 초기 sensor 대체 논의
- [[8_시연_스토리라인]] — 이 프레임을 시연에 녹이는 방법
