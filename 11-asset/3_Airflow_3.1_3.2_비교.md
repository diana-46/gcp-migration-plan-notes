---
title: "3. Airflow 3.1 vs 3.2/3.3 Asset scheduling 비교"
status: draft
tags:
  - airflow
  - asset
  - version-compare
  - partitioned-assets
  - roadmap
created: 2026-07-24
updated: 2026-07-24
---

# 3. Airflow 3.1 vs 3.2/3.3 Asset scheduling 비교

> **문서 목적**: 지금 우리 (Composer 3.1.7) 에서 asset 이 못 하는 것과, 3.2/3.3 에서 어떻게 해결되는지를 한 문서에 정리. 팀 내부 baseline + MDL 조사자와의 대화 자료.

**선행 자료**: [[1_동작_원리]] (mechanics), [[2_문제_정의]] (3.1 gap)

## 한 줄 요약

> **3.1 은 asset 이 "이벤트 신호" 수준**. 3.2 부터 **partition 이 first-class** 가 되고, 3.3 에서 **cadence 변환 매퍼** 가 붙어 진짜 데이터-centric orchestration 도구가 된다. Cosmos 지원 여부와 UI 성숙도가 남은 관건.

## 버전별 진화 (핵심 API 만)

| 축 | 3.0 (2025-04) | 3.1 (현재 Composer) | 3.2 | 3.3 |
|---|:---:|:---:|:---:|:---:|
| Asset URI + outlets | ✅ | ✅ | ✅ | ✅ |
| `outlet_events[...].extra` | ✅ | ✅ | ✅ | ✅ |
| `AssetAlias` (dynamic concrete) | ✅ | ✅ | ✅ | ✅ |
| `AssetWatcher` (외부 trigger) | ✅ | ✅ (Kafka 확대) | ✅ | ✅ |
| `@asset` decorator | ✅ | ✅ | ✅ | ✅ |
| `AssetOrTimeSchedule` (hybrid) | ✅ | ✅ | ✅ | ✅ |
| `KafkaMessageQueueTrigger` | ❌ | ✅ | ✅ | ✅ |
| **`partition_key` first-class** | ❌ | ❌ | ✅ | ✅ |
| `CronPartitionTimetable` | ❌ | ❌ | ✅ | ✅ |
| `PartitionedAssetTimetable` | ❌ | ❌ | ✅ | ✅ |
| `PartitionedAtRuntime` | ❌ | ❌ | ✅ | ✅ |
| `IdentityMapper`, `StartOfDayMapper`, ... | ❌ | ❌ | ✅ | ✅ |
| **`RollupMapper`** (many-to-one) | ❌ | ❌ | ❌ | ✅ |
| **`FanOutMapper`** (one-to-many) | ❌ | ❌ | ❌ | ✅ |
| `DayWindow`, `WeekWindow`, `SegmentWindow` | ❌ | ❌ | ❌ | ✅ |
| Manual trigger with `partition_key` | ❌ | ❌ | ✅ | ✅ |

**핵심 진화 지점**:
- 3.0 → 3.1: `KafkaMessageQueueTrigger` 등 외부 이벤트 트리거 확대
- **3.1 → 3.2: partition_key 를 first-class 로 승격.** Extra 아닌 스케줄러 매칭 필드
- **3.2 → 3.3: cadence 변환 매퍼.** Hourly → daily 같은 시간축 변환이 native

## Part A — Airflow 3.1 에서 안 되는 것 (재확인)

[[2_문제_정의]] 요약 + 실측 결과.

### Gap 1 — Payload 없음

Asset event 에 `extra` 필드는 있지만:
- Cosmos 자동 outlet 은 `extra` 를 안 채움
- Producer 의 execution_date / partition 정보를 downstream 이 알려면 `source_run_id` 파싱 (fragile) 또는 DB 조회

### Gap 2 — 조건부 emit 없음

Task 성공 = 무조건 event emit. Backfill / manual trigger 를 downstream 이 구분 못 함. 억제하려면 stamp task 로 우회.

### Gap 3 — Partition 매칭 없음

`schedule=[A, B]` AND 는 "둘 다 update 됐음" 만 판단. "같은 hour 짝" 이라는 시간축 매칭은 스케줄러가 안 해줌.

### Gap 4 — 파티션 UI 부재

Airflow UI 에 파티션 단위 진행 상황 뷰가 없음. "어느 파티션이 처리됐고 어느 파티션이 누락됐는지" 를 UI 로 못 봄.

### Gap 5 — Freshness SLA 선언 불가

"Youtube 리포트가 4일 지연되면 알림" 같은 선언적 freshness 없음. 별도 sensor DAG 로 구현해야 함.

### 3.1 실측 진행 상황

- ✅ 과거 dag_run clear+rerun 시 downstream 자동 트리거 확인 (2026-07-24)
- ⏳ Batching 실측 (같은 partition_key 재emit dedup 여부)
- ⏳ Backfill 시 downstream storm 실제 관측

상세: [[9_Past_DAG_재실행과_Downstream]]

## Part B — Airflow 3.2 개선 (Partitioned Assets)

### 개념 승격: partition_key 가 first-class 필드

3.1 까지의 asset event 필드:
```
(uri, timestamp, source_dag_id, source_task_id, source_run_id, extra)
```

3.2 부터:
```
(uri, timestamp, source_dag_id, source_task_id, source_run_id, extra, partition_key)
                                                                    ^^^^^^^^^^^^^^^
                                                                    first-class 매칭 필드
```

**의미**: 스케줄러가 partition_key 를 직접 사용해서 매칭 판단. Extra 에 담긴 문자열이 아니라 native concept.

### Producer 쪽 (partition 선언)

**시간 기반 정기 스케줄**:
```python
@asset(schedule=CronPartitionTimetable("0 * * * *", timezone="UTC"))
def hourly_sales():
    pass
# → 매 시간 dag_run 이 partition_key = "2026-07-24T05:00:00" 자동 할당
```

**런타임 결정 (multi-partition 지원)**:
```python
@asset(schedule=PartitionedAtRuntime())
def multi_region(self, outlet_events):
    outlet_events[self].add_partitions(["us", "eu", "apac"])
# → 한 task 실행이 여러 partition 을 명시적으로 emit
```

### Consumer 쪽 (partition-aware 스케줄)

```python
with DAG(
    schedule=PartitionedAssetTimetable(
        assets=hourly_sales & daily_targets,       # AND, partition-aware
        default_partition_mapper=IdentityMapper(), # 파티션 그대로 매칭
    ),
) as dag:
    @task
    def process(dag_run=None):
        print(dag_run.partition_key)  # "2026-07-24T05:00:00" 자동 접근
```

### AND 매칭이 partition-aware

문서 인용:
> "If transformed partition keys from all required upstream assets do not align, the downstream Dag will not be triggered for that partition."

즉:
- `A_h1`, `A_h2`, `B_h1` 만 도착 → `h1` downstream 만 발화. `h2` 는 `B_h2` 도착 대기
- **3.1 gap 3 (partition 매칭 없음) 이 native 로 해결됨**

### 매퍼 종류 (3.2 부터 시작)

| 매퍼 | 용도 |
|---|---|
| `IdentityMapper` | 파티션 그대로 매칭 (같은 hour, 같은 date 등) |
| `StartOfHourMapper` | 임의 datetime → 그 시각 정각 |
| `StartOfDayMapper` | 임의 datetime → 그 날짜 자정 |
| `StartOfWeekMapper` | 임의 datetime → 그 주 시작 |
| `StartOfYearMapper` | 임의 datetime → 그 해 1월 1일 |
| `AllowedKeyMapper` | 허용 리스트 검증 |
| `FixedKeyMapper` | 여러 key 를 하나로 접기 |
| `ProductMapper` | Composite key 를 segment 별로 다른 매퍼 |

### Manual trigger by partition_key

```bash
curl -X POST "http://<airflow-host>/api/v2/dags/<dag>/dagRuns" \
  -d '{"logical_date": "...", "partition_key": "us|2026-03-10T09:00:00"}'
```

**의미**: 특정 partition 을 직접 재실행 가능. UI 백필도 파티션 단위로 발전할 것으로 예상 (문서화 정도 실측 필요).

## Part C — Airflow 3.3 개선 (Rollup / FanOut Mapper)

**Cadence 변환이 native**. 이게 특히 MDL / Story 우리 팀 케이스에 관련.

### RollupMapper (many-to-one)

**Hourly → daily 등 상위 집계**:

```python
schedule=PartitionedAssetTimetable(
    assets=hourly_sales,
    default_partition_mapper=RollupMapper(
        upstream_mapper=StartOfDayMapper(),   # hourly key → daily key 정규화
        window=DayWindow(),                    # 하루가 예상 window
        wait_policy=WaitForAll,                # 24개 다 모여야 발화 (또는)
        # wait_policy=MinimumCount(20),        # 20개 이상이면 발화
    ),
)
```

**동작**:
1. Producer h1 완료 → `"2026-07-24T01:00:00"` event
2. Producer h2 완료 → `"2026-07-24T02:00:00"` event
3. ... 24개 event 축적
4. `StartOfDayMapper` 가 모두 `"2026-07-24"` 로 정규화
5. `DayWindow + WaitForAll` 이 "24개 다 왔음" 판정
6. Consumer 발화, `dag_run.partition_key = "2026-07-24"`

### FanOutMapper (one-to-many)

**Weekly → daily 등 하위 배포**:

```python
schedule=PartitionedAssetTimetable(
    assets=weekly_model,
    default_partition_mapper=FanOutMapper(
        upstream_mapper=StartOfWeekMapper(),
        window=WeekWindow(),
        max_downstream_keys=7,
    ),
)
```

**동작**: Producer 월요일 실행 → 1 event → 7개 daily key 로 fan-out → consumer 7번 발화.

### SegmentWindow (카테고리컬 rollup)

```python
default_partition_mapper=RollupMapper(
    upstream_mapper=FixedKeyMapper("all_regions"),
    window=SegmentWindow(["us", "eu", "apac"]),
)
```

**동작**: `us`, `eu`, `apac` 3개 event 다 모여야 `"all_regions"` downstream 발화.

## Part D — 3.1 gap 이 3.2/3.3 로 어떻게 해결되는가

**핵심 매핑 표**:

| 3.1 Gap | 3.2 해결 | 3.3 추가 | 여전히 필요한 것 |
|---|---|---|---|
| Payload 없음 | `partition_key` first-class, `dag_run.partition_key` 접근 | — | 조건부 emit 은 여전히 stamp 필요 |
| 조건부 emit 없음 | — | — | Stamp task + `AirflowSkipException` 유지 |
| Partition 매칭 없음 | `PartitionedAssetTimetable` + 매퍼 | Rollup / FanOut | — |
| 파티션 UI 부재 | 개선? UI 성숙도 실측 필요 | — | UI 가 여전히 약하면 커스텀 대시보드 |
| Freshness SLA | ❌ 여전히 없음 | ❌ | 별도 sensor DAG 유지 |
| Cadence 다름 | Manual (매퍼 조합) | ✅ Rollup / FanOut native | — |
| Cross-DAG dbt 의존성 | ❌ Cosmos 한계 | ❌ | Cosmos 개선 필요 |
| Backfill dedup | 실측 필요 | ✅ Partition 별 dag_run 예상 | 실측 확정 필요 |

## Part E — MDL 팀 요구와의 커버리지

MDL 팀 (dataset_test.md) 의 pain point 를 각 도구로 커버 가능한지:

| MDL 요구 | 3.1 | 3.2/3.3 | Dagster | 우리 판단 |
|---|:---:|:---:|:---:|---|
| DSP 갭/지연 감지 | ⚠️ 커스텀 | ✅ AssetWatcher + Kafka | ✅ | **3.2 커버** |
| Sequenced N-1 → N | ❌ | ⚠️ **self-dep 명시 예시 없음** | ✅ | **Airflow 약점** |
| 재수집 자동 재실행 | ❌ | ⚠️ run_key 개념 다름 | ✅ | **Airflow 약점** |
| 파티션 관측성 UI | ❌ | ⚠️ 성숙도 미지수 | ✅ | **Airflow 약점 가능성** |
| Cross-DAG dbt 의존성 | ❌ | ❌ Cosmos 한계 | ✅ dagster-dbt | **Cosmos 개선 필요** |
| 1 파티션 = 1 DagRun | ❌ | ⚠️ 실측 확인 필요 | ✅ | **PoC 목표** |
| Freshness SLA 선언 | ❌ | ❌ | ✅ | **Airflow 명확한 약점** |
| Kafka 이벤트 드리븐 | ⚠️ | ✅ AssetWatcher | ✅ | **3.1 부터 가능** |
| UI 파티션 백필 | ❌ | ⚠️ API 있음, UX 미지수 | ✅ | **Airflow 약할 것** |

**정직한 판단**:
- Airflow 3.2/3.3 이 **커버**: DSP 갭 감지, Kafka 이벤트, 파티션 매칭, cadence 변환
- Airflow **여전히 약함**: Sequenced self-dep, 재수집 자동, 파티션 UI, Freshness SLA
- Cosmos **개선 필요** (upstream 기여 or fork): Cross-DAG dbt 의존성

## Part F — 남은 미지수 (PoC 필요)

3.2/3.3 문서로도 확답 안 되는 것들. Docker 로 실측.

1. **Cosmos + partitioned assets 호환** — Cosmos 가 dbt run 의 outlet 에 partition_key 를 어떻게 실는지 (또는 안 실는지)
2. **`StartOfDayMapper` 의 timezone** — KST 기준 그루핑이 되는지, UTC 만인지
3. **1 파티션 = 1 DagRun 보장** — MDL 문서가 3.x 에서 "안 됨" 이라 지적한 부분. 3.2/3.3 개선 여부
4. **같은 partition_key 로 여러 번 emit 시 동작** — Dedup 여부, downstream 재트리거 여부
5. **`RollupMapper WaitForAll` 실패 시** — 일부 upstream 실패 시 downstream 이 timeout / skip 되는지
6. **파티션 UI 성숙도** — Airflow UI 에서 파티션 별 뷰가 얼마나 잘 되는지
7. **`AssetWatcher + KafkaMessageQueueTrigger`** — 실제 운영 안정성, 재해복구
8. **API 로 partition 백필** — UX 가 Dagster 수준인지, 원시 API 만인지

**우선순위 1**: Cosmos 호환 — 이거 결과에 따라 나머지 계획이 완전 바뀜.

## Part G — 이관 로드맵 관점

### 현재 (Composer 3.1.7)

- Asset scheduling 기본 사용 가능 (Cosmos 자동 outlet + `schedule=[Asset]`)
- **Story 팀 케이스**: stamp task 로 payload / 조건부 emit / alias 로 partition 채널
- 한계 명확: partition 매칭 없음, 조건부 emit 우회 필요

### 근접 (Composer 3.2 이관 ~1개월)

- **partition_key first-class** → stamp task 의 대부분 로직 native 로 대체
- **`PartitionedAssetTimetable`** → AssetAlias 로 fan-out 하던 것 partition 매퍼로 대체
- Stamp task 는 **조건부 emit (게이트)** 역할만 남음

### 중기 (Composer 3.3 이관 시점)

- **RollupMapper / FanOutMapper** → cadence 변환이 코드 몇 줄
- 카테고리컬 rollup (`SegmentWindow`) 로 multi-DSP 매칭 표현 가능
- Story 팀 hourly → daily rollup 이 스케줄러 native 로

### 장기 (Airflow 이후 버전)

- 파티션 UI 성숙도 개선 예상 (Airflow 팀 로드맵)
- Cosmos 의 partitioned assets 지원 (커뮤니티 or 자체 기여)
- `AssetWatcher` 의 커스텀 trigger 생태계 확장

## 관련 문서

- [[1_동작_원리]] — 3.1 mechanics (선행 자료)
- [[2_문제_정의]] — 3.1 gap 3가지 (이 문서 Part A 요약)
- [[4_해결_패턴]] — 3.1 bridge patterns
- [[9_Past_DAG_재실행과_Downstream]] — 실측 진행 중
- [[10_MDL_aligning_노트]] — MDL 조사자 대응 자료
- [[dataset_test]] — MDL 조사자의 Dagster PoC 리포트
- [[../스케줄러/9_Airflow Asset과 Dataset]] — Asset/Dataset 개념 소개

## TODO

- [ ] Composer 3.2 이관 시점 확정 후 로드맵 업데이트
- [ ] Docker PoC 진행 후 Part F 항목별 결과 반영
- [ ] Part E 커버리지 표를 PoC 실측 결과로 업데이트
- [ ] Cosmos partitioned assets 지원 상태 모니터링
