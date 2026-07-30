---
title: "10. 월요일 MDL 조사자 aligning 노트"
status: draft
tags:
  - airflow
  - asset
  - meeting-prep
  - dagster
  - mdl
  - internal
created: 2026-07-24
updated: 2026-07-24
---

# 10. 월요일 MDL 조사자 aligning 노트

> **일시**: 2026-07-27 (월) 예정
> **성격**: 간단한 1:1 aligning. Formal 결정 자리 아님
> **참석**: Diana (데이터플랫폼팀, 조직 표준 담당) ↔ MDL 팀 플랫폼 조사자
> **목적**: 서로의 조사 결과 공유, 조직 방향 대략 aligning, 후속 액션 잡기

## 내 포지션 (미팅 임하는 프레임)

**나의 역할**:
- 조직 차원 오케스트레이터 표준을 결정하는 위치
- Provider 개발/유지, PoC 실행 주체
- MDL / Story 등 소비자 팀의 요구를 통합 판단

**상대의 역할**:
- MDL 팀에서 플랫폼 관점으로 조사한 사람
- Dagster PoC 를 이미 진행함 (dataset_test.md)
- 자기 팀 문제 해결 관점에서 접근

**월요일 목표**:
1. 상대의 발견 상세 청취 (문서에 없는 뉘앙스, 확신도, 진짜 원하는 것)
2. 우리 팀 관점 (Airflow 3.2/3.3 이론 + 3.1 실측) 공유
3. 조직 방향 옵션 3가지 놓고 감 잡기
4. 후속 액션 (재미팅 시점, 각자 할 것) 확정

**하지 말 것**:
- 상대를 반박하거나 Dagster 를 폄하 (그쪽 조사가 실제 문제에 기반함)
- 조직 결정을 이 자리에서 확정 (나 혼자 결정할 사안 아님)
- "우리 팀에서 다 커버해줄게" 식 과약속

## Chapter 1 — 상대의 발견 요약 (dataset_test.md 압축)

먼저 상대가 뭘 발견했는지 정확히 이해하고 있음을 보여주기.

### MDL 팀의 5가지 pain point (요약)

1. **다중 DSP 랜덤 지연** — Spotify 2일, YouTube 3일, Chartmetric 랜덤. Cron 스케줄로 갭 보정 어려움
2. **전일 파티션 의존 (sequenced)** — chart/playlist 는 N-1 → N 순차. 재수집 시 max 까지 cascade
3. **재수집이 일상적** — updated_dt 없음. Kafka 이벤트로 감지 필요
4. **파티션 관측성 부족** — 어느 파티션에 어느 DSP 가 빠졌는지 UI 로 못 봄
5. **DAG 쪼갤 때 dbt 의존성 끊김** — Cosmos 가 cross-DAG 배선 안 함

### 시도한 것들의 실패

- **Dataset v2.10.2** — partition 인식 안 됨, 정적 그래프
- **Kafka + TriggerDagRunOperator** — DAG 단위 트리거라 fan-out 심함
- **Range mode 5일치** — 무거운 모델 불가, diff 계산 틀어짐

### 현재 임시 운영

- DSP × Silver / Gold DAG 분리 + SCHEDULED_TIME 하드코딩
- data_interval_start - Ndays 로 갭 보정
- dbt test + Redash + Slack alert

### Dagster PoC 발견 (3-1 ~ 3-6)

- ✅ 파티션 가시성 UI (**가장 큰 pain 해결**)
- ✅ UI 파티션 백필
- ✅ DAG 쪼개도 dbt 의존성 유지
- ✅ Sequenced self-dep (`TimeWindowPartitionMapping(-1, -1)`)
- ✅ Kafka sensor + run_key dedup
- ✅ Freshness SLA 선언적

### 상대의 결론 (내가 읽은)

- MDL 자체는 **Dagster 로 이관** 이 자연스러움
- 데이터 레이어는 Dagster, Loupe 전송 같은 orchestration 은 Airflow (하이브리드)
- Nifi 수집 로직도 Dagster 로 통합할 수 있음

**청취할 것**: 이 결론이 얼마나 확정적인지, 아니면 조직 판단 대기 상태인지.

## Chapter 2 — Airflow 3.1 상세 (우리 실측)

우리 팀이 Airflow 3.1 실측한 결과를 이론 + 관측치로 정리.

### Asset 이벤트의 3층 구조

```
Producer task (outlets=[Asset(...)]) ──emit──→
  Layer 1: DB (asset_event 테이블, append-only)
  Layer 2: Scheduler batching (unconsumed → dag_run)
  Layer 3: Consumer (triggering_asset_events)
```

- 상세는 [[1_동작_원리]]

### 3.1 의 발견된 3가지 gap

1. **Payload 없음** — Cosmos 자동 outlet 은 URI 만. `extra` 비어있음
2. **조건부 emit 없음** — task 성공 시 무조건 emit
3. **Partition 매칭 없음** — `schedule=[A, B]` AND 는 이벤트 존재 판정, 시간축 매칭 아님

- 상세는 [[2_문제_정의]]

### 실측 검증 (진행 중, 2026-07-24)

- ✅ 과거 dag_run clear+rerun 시 downstream 자동 트리거 확인
- ⏳ 여러 producer run 뭉침 시 downstream dag_run 개수 (batching 실측)
- ⏳ 같은 dag_run 두 번 clear+rerun 시 dedup 여부
- ⏳ source_run_id 형식 (scheduled/manual/backfill 접두어)

- 상세는 [[9_Past_DAG_재실행과_Downstream]]

### 3.1 에서 우회 (Bridge Pattern)

Cosmos 뒤에 **stamp task** 를 붙여서:
- `outlet_events[asset].extra` 에 partition/run 정보 stamp
- `AirflowSkipException` 으로 조건부 emit
- `AssetAlias` 로 partition-specific fan-out (별도 채널)
- Consumer 에서 dynamic task mapping 으로 매칭

**한계**:
- 임시방편. 3.2/3.3 partitioned assets 로 상당 부분 대체 예정
- URI dedup 이슈 (아직 실측 미완료)

- 상세는 [[4_해결_패턴]], [[5_계층_분리_원칙]]

## Chapter 3 — Airflow 3.2/3.3 상세 (이론)

**여기가 미팅에서 우리가 대등하게 대화하기 위한 핵심 이론 파트**. MDL 조사자의 dataset_test.md 는 Airflow 3.x 를 정리했지만 partitioned assets (3.2) 이후 진화는 반영 안 됐을 가능성 큼.

### 3.2 의 핵심: Partitioned Assets

**개념**: Asset event 에 `partition_key` 가 **first-class 필드** 로 붙음. Extra 에 담긴 문자열이 아니라 스케줄러가 직접 사용하는 매칭 필드.

**Producer 쪽**:

```python
# 시간 기반 (정기 스케줄)
@asset(schedule=CronPartitionTimetable("0 * * * *", timezone="UTC"))
def my_asset():
    ...
# → 매 시간 dag_run 이 partition_key = "2026-07-24T05:00:00" 자동 할당

# 런타임 결정
@asset(schedule=PartitionedAtRuntime())
def multi_region(self, outlet_events):
    outlet_events[self].add_partitions(["us", "eu", "apac"])
# → 한 task 가 여러 partition 을 명시적으로 emit
```

**Consumer 쪽**:

```python
with DAG(
    schedule=PartitionedAssetTimetable(
        assets=A & B,                       # AND, partition-aware
        default_partition_mapper=IdentityMapper(),
    ),
) as dag:
    @task
    def process(dag_run=None):
        print(dag_run.partition_key)       # "2026-07-24T05:00:00" 자동 접근
```

**AND 매칭이 partition-aware**:
> "If transformed partition keys from all required upstream assets do not align, the downstream Dag will not be triggered for that partition."

즉 A_h1, A_h2, B_h1 만 왔으면 → h1 downstream 만 발화. h2 는 B 도착 대기.

### 3.3 의 핵심: Rollup / FanOut Mapper

**Cadence 변환이 native**. 이게 MDL 케이스에 특히 관련.

**RollupMapper (many-to-one)** — hourly → daily 등:

```python
schedule=PartitionedAssetTimetable(
    assets=hourly_sales,
    default_partition_mapper=RollupMapper(
        upstream_mapper=StartOfDayMapper(),   # hourly key → daily key
        window=DayWindow(),                    # 하루 24개가 예상 집합
        wait_policy=WaitForAll,                # 24개 다 오면 발화
    ),
)
```

**FanOutMapper (one-to-many)** — weekly → daily 등:

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

**SegmentWindow** — 카테고리컬 rollup ("us|eu|apac" → "all_regions").

### Manual trigger 로 특정 partition 실행

```bash
curl -X POST "http://<airflow-host>/api/v2/dags/aggregate_regional_sales/dagRuns" \
  -d '{"logical_date": "...", "partition_key": "us|2026-03-10T09:00:00"}'
```

파티션 백필도 이 API 로 가능해질 것으로 보임.

### AssetWatcher + KafkaMessageQueueTrigger (3.x)

MDL 문서에도 언급됨. Producer DAG 이 emit 안 하고 **스케줄러가 외부 큐를 직접 watch**:

```python
user_signup = Asset(
    "pubsub://project/topic",
    watchers=[
        AssetWatcher(
            name="signup_arrival",
            trigger=PubSubMessageTrigger(...),
        ),
    ],
)
```

- **Kafka**: `KafkaMessageQueueTrigger` (3.1 부터 메시지 큐 확대)
- **GCS**: `GCSFileTrigger`
- **PubSub**: `PubSubMessageTrigger`
- 커스텀 Trigger 작성 가능

MDL 이 Nifi/Kafka 로 데이터 도착을 알리는 파이프라인이라, **이걸 잘 조합하면 sensor 없이 이벤트-드리븐 트리거 가능**.

## Chapter 4 — MDL 요구와 Airflow 3.2/3.3 커버리지

**이 표가 미팅의 핵심**. MDL pain point 각각에 대해 정직하게 커버율 판단.

| MDL 요구 | 3.1 상태 | 3.2/3.3 예상 | Dagster | 판단 |
|---|:---:|:---:|:---:|---|
| DSP 갭/지연 자동 감지 | ❌ | ✅ AssetWatcher + Kafka | ✅ sensor | **3.2 로 커버 가능** |
| Sequenced (N-1 → N) | ⚠️ 커스텀 | ⚠️ **문서에 self-dep 예시 없음** | ✅ `TimeWindowPartitionMapping(-1,-1)` | **Airflow 약점** |
| 재수집 자동 재실행 | ❌ | ⚠️ 커스텀 (run_key 개념 다름) | ✅ run_key dedup | **Airflow 약점** |
| 파티션 관측성 UI | ❌ | ⚠️ UI 성숙도 미지수 | ✅ | **Airflow 약점 가능성 큼** |
| DAG 쪼갤 때 dbt 의존성 | ❌ Cosmos 한계 | ❌ 여전히 Cosmos 한계 | ✅ dagster-dbt | **Cosmos 개선 필요** |
| 1 파티션 = 1 DagRun | ❌ | ⚠️ 실측 필요 (우리 PoC 목표) | ✅ | **PoC 확인 후 판단** |
| Freshness SLA 선언 | ❌ | ❌ | ✅ | **Airflow 명확한 약점** |
| Kafka 이벤트 드리븐 | ⚠️ | ✅ AssetWatcher | ✅ sensor | **3.x 로 커버** |
| UI 파티션 백필 | ❌ | ⚠️ API 는 있음, UX 미지수 | ✅ | **Airflow 약할 것** |

**정직한 판단**:
- **Airflow 3.2/3.3 이 커버하는 것**: DSP 갭 감지, Kafka 이벤트, 파티션 매칭
- **Airflow 여전히 약한 것**: Sequenced self-dep, 재수집 자동, 파티션 UI, Freshness SLA, cross-DAG dbt 의존성
- **어느 도구로도 어려운 것**: Cosmos 개선 필요 (조직이 upstream 기여? fork?)

즉 **MDL 케이스는 정말 Dagster 가 유리한 지점이 남음**. 우리가 Airflow 로 다 커버 가능하다고 주장하기는 무리.

## Chapter 5 — 조직 방향 3가지 옵션

미팅에서 놓고 대화할 프레임.

### Option A — 전체 Airflow (Dagster 안 씀)

**전제**: MDL 도 Airflow 로 남되, gap 을 조직이 커스텀 개발로 메꿈.

**필요 개발**:
- 파티션 UI → DataHub 스티칭 or 자체 대시보드
- Sequenced self-dep → 커스텀 operator / provider 확장
- Freshness SLA → 자체 sensor 라이브러리
- Cross-DAG dbt 의존성 → Cosmos upstream 기여

**비용**: 데이터플랫폼팀의 지속적 커스텀 개발. 반년~1년 규모.

**리스크**: 커스텀 툴이 Dagster 만큼 우아하지 못함. MDL 팀 생산성 저하.

**적합**: 조직이 Airflow 생태계 투자를 강력히 유지하고 싶고, 인력 여유 있을 때.

### Option B — 전체 Dagster (Airflow 폐기)

**전제**: 조직 전체가 Dagster 로 이동.

**필요 이동**:
- `apache-airflow-providers-kakaoent-dataplatform` 폐기 or Dagster 로 포팅
- Composer 3 이관 (진행 중) 폐기, 자체 Dagster 클러스터
- 모든 팀 재교육
- Story 등 단순 케이스도 Dagster 로

**비용**: 조직 대규모 이동. 매우 큼.

**리스크**: 이미 한 Airflow 투자 손실. 단순 케이스에 오버킬.

**적합**: 조직이 data-asset-centric 워크플로가 압도적이고, 다른 orchestration 니즈가 적을 때.

### Option C — 하이브리드 (계층별 표준화)

**전제**: 워크로드 성격에 따라 도구 분리.

- **데이터 레이어** (dbt heavy, partition heavy, sequenced): Dagster
- **Orchestration 레이어** (Loupe 전송, 인프라 잡, 정기 배치): Airflow

**MDL 문서에서도 이 방향 제안**:
> `[NiFi 수집, Dagster로 통합] → Kafka`
> `├─→ [Dagster] 데이터 레이어`
> `└─ [Airflow] cron consume → Loupe 전송`

**비용**: 두 도구 유지. 하지만 **경계가 명확** 해서 관리 부담 예측 가능.

**필요한 것**:
- 공통 인프라 통일 (모니터링, 알림, 인증)
- Provider 인터페이스 통일 (Loupe / Kafka / BQ 접근을 두 프레임워크에 동일 API)
- 팀 간 handoff 지점 문서화

**적합**: 워크로드 성격이 정말 다르고, 각 도구를 강점 영역에 배치할 수 있을 때. **MDL 이 이미 제안한 방향.**

### 개인적 감 (미팅 전 정리)

- **Option A** 는 조직 리소스 감안 시 realistic 하지 않음
- **Option B** 는 이미 한 투자 감안 시 낭비
- **Option C** 가 가장 지속가능 — 다만 이중 유지 부담을 어떻게 관리할지 실행 계획 필요

**하지만 이건 미팅에서 확정할 게 아니라 aligning 하는 것**. 상대의 의견이 어느 옵션에 가까운지 청취.

## Chapter 6 — 미팅 아젠다 (실제 대화 순서)

**15~30분 미팅으로 가정**:

1. **오프닝 (2분)**
   - "간단히 이야기하기로 했지만, 우리 쪽에서도 조사와 실측을 좀 했어서 공유"
   - 문서 (dataset_test.md, 이 노트) 공유

2. **상대 발견 청취 (5~10분)**
   - Dagster PoC 결과에서 가장 강력했던 부분?
   - Dagster 이관 확신도는 어느 정도?
   - 상대 팀 (MDL) 내부 컨센서스 상태?

3. **우리 관점 공유 (5~10분)**
   - Airflow 3.1 실측 진행 상황
   - 3.2/3.3 이론 정리 결과
   - MDL 요구와의 커버리지 매트릭스 (Chapter 4)

4. **조직 옵션 대화 (5분)**
   - 3가지 옵션 중 감이 어느 쪽인지 서로 짚기
   - Option C (하이브리드) 가 실제로 가능한지 논의

5. **후속 액션 (3분)**
   - 데이터플랫폼팀 PoC 스코프 (Docker Airflow 3.2/3.3)
   - MDL 팀 다음 스텝 (Dagster 이관 추진 vs 대기)
   - 재미팅 시점

## Chapter 7 — 데이터플랫폼팀 PoC 계획 (미팅에서 발표)

우리가 뭘 검증할지 명확히 하고 미팅에 임함.

**대상**: Airflow 3.2/3.3 Docker (Composer 미리 검증)

**핵심 검증 항목**:

1. **Cosmos + partitioned assets 호환** — dbt task 의 outlet event 가 partition_key 를 어떻게 실는지
2. **`CronPartitionTimetable` + timezone (KST)** — 우리 케이스 정합
3. **`RollupMapper + StartOfDayMapper + WaitForAll`** — hourly → daily rollup 실측
4. **1 파티션 = 1 DagRun 보장** — MDL 이 3.x 에서 "안 됨" 이라 지적. 3.2/3.3 에서 진짜 되는지
5. **AND 매칭 partition 정합** — 짝 안 맞으면 안 뜨는 게 자동인지
6. **AssetWatcher + KafkaMessageQueueTrigger** — MDL 스타일 이벤트 트리거 실측
7. **파티션 UI 성숙도** — Airflow UI 에서 파티션 별 진행 상황 얼마나 잘 보이는지
8. **Backfill 시 partition 별 개별 dag_run** — 백필 API 로 특정 partition 지정

**예상 기간**: 1~2주.

**결과물**:
- 각 검증 항목별 ✅/⚠️/❌ 판정
- 스크린샷 / 코드 예시
- MDL 요구와의 최종 커버리지 (Chapter 4 표 업데이트)

## Chapter 8 — 미팅 후 예상 후속 액션

**시나리오 A — 상대가 Dagster 로 이관 강하게 원함**
- 우리는 PoC 로 3.2/3.3 커버리지 확인 (약속 시점)
- PoC 결과가 "생각보다 많이 커버" 면 재고 요청
- 그래도 Dagster 로 갈 결정이면 Option C 로 조직 방향 세팅
- 데이터플랫폼팀 로드맵: Dagster 지원도 시작 (인력 배정 필요)

**시나리오 B — 상대가 대기 상태**
- 우리 PoC 완료까지 기다림
- PoC 후 재미팅으로 결정
- 이 사이 우리는 provider 확장 로컬 프로토타입 유지

**시나리오 C — 상대가 조직 결정 요구**
- 매니저 / 리드 미팅 세팅
- 이 노트를 확장한 조직 방향 결정 문서 작성
- 3주~1개월 스팬으로 결정

## 관련 문서

### 우리 팀 자체 자료 (내가 만들었거나 참여)
- [[README]] — asset 디렉토리 인덱스
- [[0_결론]] — Executive summary
- [[2_문제_정의]] — 3.1 gap 3가지
- [[1_동작_원리]] — Asset event 3층 mechanics
- [[4_해결_패턴]] — Stamp / Alias / Mapping bridge patterns
- [[5_계층_분리_원칙]] — Cosmos vs Stamp 계층 분리
- [[6_Asset_vs_Sensor_판단표]] — 조직 규칙 참고 자료
- [[7_Provider_확장_제안]] — Provider 확장 로드맵 (우리 팀 내부 결정)
- [[8_시연_스토리라인]] — 시연 chapter 1~5
- [[9_Past_DAG_재실행과_Downstream]] — 실측 진행 중

### 상대 자료
- [[dataset_test]] — MDL 조사자의 Dagster PoC 리포트

## 미팅 준비 최종 체크리스트

미팅 전 완료해야 할 것:

- [ ] 이 문서 최종 리뷰
- [ ] [[dataset_test]] 상세 재독 (놓친 뉘앙스 없는지)
- [ ] 진행 중 실측 결과 반영 ([[9_Past_DAG_재실행과_Downstream]])
- [ ] [[7_Provider_확장_제안]] 톤 조정 (외부 제안 → 우리 팀 로드맵)
- [ ] Chapter 4 매트릭스 최종 확인 (정직성)
- [ ] Chapter 5 옵션 3가지 각각의 tradeoff 재검토
- [ ] Chapter 6 아젠다 시간 배분 확인
- [ ] PoC 계획 (Chapter 7) 인력 / 일정 감 잡기

## TODO (미팅 후)

- [ ] 대화 요약 이 문서에 추가
- [ ] 상대의 실제 확신도 반영
- [ ] 조직 방향 옵션 3가지 중 어디로 좁혀졌는지 기록
- [ ] 후속 액션 확정 (재미팅 시점 등)
- [ ] 우리 팀 내부 공유 (필요 시 별도 문서)
