---
title: "7. Provider 확장 제안 — 조직 표준 컴포넌트 3종"
status: draft
tags:
  - airflow
  - asset
  - provider
  - proposal
  - kakaoent-dataplatform
created: 2026-07-24
updated: 2026-07-24
---

# 7. Provider 확장 제안 — 조직 표준 컴포넌트 3종

> `airflow-provider-kakaoent-dataplatform` 에 asset 관련 표준 컴포넌트 3종을 추가하자는 제안. 데이터플랫폼팀과의 논의 자료.

## 왜 provider 로?

Provider 에는 이미 팀 공통 인프라 접근 도구들이 있음:
- `LoupeKafkaBatchOperator` — Kafka 배치 export
- `LoupeSignalHttpOperator` — Loupe API 시그널
- `BigqueryQuerySensor` — BQ 데이터 조건 sensor

Asset 관련 표준 도구를 provider 에 추가하는 건 **자연스러운 확장 방향**. 이유:

1. **팀별 재발명 방지** — Story 팀이 stamp task 를 만들고 나면 다른 팀도 비슷한 걸 만들 것. Provider 에 넣으면 한 번만 만듦
2. **URI 규약 일관성** — 팀마다 URI 스킴이 다르면 lineage / catalog 스티칭이 깨짐. Provider builder 로 강제
3. **Payload 스키마 표준화** — Downstream 이 "producer 마다 payload 가 달라서 매번 다른 파싱" 을 안 해도 되도록
4. **Airflow 3 asset API 진화 흡수** — API 가 minor 버전 사이에 바뀌면 provider 만 업데이트하고 팀 DAG 은 무관하게

## 컴포넌트 3종

### 컴포넌트 1: `KakaoAsset` — URI 컨벤션 빌더

**목적**: 팀마다 URI 스킴 재발명하지 않도록 canonical builder 제공.

**API 스케치**:
```python
from airflow.providers.kakaoent.dataplatform.assets import KakaoAsset

# 물리 자산 좌표 (Cosmos outlet 과 매칭용)
BIZBERRY_TABLE = KakaoAsset.bigquery(
    project="dev-dp-project-354904",
    dataset="datawarehouse_berriz",
    table="bizberry_community_contents_summary_integration",
)
# -> Asset("bigquery://dev-dp-project-354904/datawarehouse_berriz/bizberry_community_contents_summary_integration")

# 논리 자산 좌표 (팀이 명시하는 readiness signal)
BIZBERRY_READY = KakaoAsset.logical(
    team="story",
    domain="berriz.bizberry",
    name="summary_hourly_ready",
)
# -> Asset("kakaoent://story/berriz.bizberry/summary_hourly_ready")

# GCS 객체
GCS_ASSET = KakaoAsset.gcs(bucket="my-bucket", path="reports/daily")
# -> Asset("gs://my-bucket/reports/daily")

# Kafka 토픽
KAFKA_ASSET = KakaoAsset.kafka(broker_cluster="berriz", topic="berriz.bizberry.user_post")
# -> Asset("kafka://berriz/berriz.bizberry.user_post")
```

**얻는 것**:
- 오타 방지 (URI 문자열 조합 실수)
- 조직 차원 URI namespace 관리 가능 (`kakaoent://team/domain/...`)
- 미래에 URI 스킴 변경 시 provider 만 수정
- 팀별 asset 필터링 (`kakaoent://story/*`) 용이

**Airflow 표준 스키마 호환**:
- `bigquery://`, `gs://`, `s3://`, `mysql://`, `kafka://` 등은 Airflow 3 권고 스키마 그대로
- `kakaoent://` 는 조직 커스텀 스키마 (논리 자산용)

### 컴포넌트 2: `KakaoAssetStampOperator` — Stamp task 표준

**목적**: [[4_해결_패턴]] Pattern A 를 operator 로 pack.

**API 스케치**:
```python
from airflow.providers.kakaoent.dataplatform.assets import KakaoAssetStampOperator

stamp = KakaoAssetStampOperator(
    task_id="stamp_summary_ready",
    asset=BIZBERRY_READY,               # 또는 aliases=[...]
    payload={
        "partition_kst": "{{ data_interval_start.in_timezone('Asia/Seoul').strftime('%Y%m%d%H') }}",
        "kst_date":      "{{ data_interval_start.in_timezone('Asia/Seoul').strftime('%Y%m%d') }}",
        "custom_field":  lambda ctx: compute_something(ctx),  # 콜러블도 지원
    },
    skip_on_run_type=["backfill"],
    skip_when=lambda ctx: ctx["data_interval_start"].hour not in [0, 12],
)
```

**Operator 가 자동으로 추가하는 표준 필드** (팀이 놓쳐도 항상 들어감):

| 필드 | 값 |
|---|---|
| `producer_dag_id` | `ctx["dag_run"].dag_id` |
| `producer_task_id` | `ctx["ti"].task_id` |
| `producer_run_id` | `ctx["dag_run"].run_id` |
| `producer_run_type` | `ctx["dag_run"].run_type` |
| `emitted_at` | UTC ISO 8601 |
| `stamp_operator_version` | provider 버전 (파서가 스키마 진화 대응) |

즉 팀은 도메인 payload 만 세팅하고, meta 는 operator 가 담당.

**AssetAlias fan-out 도 지원**:
```python
stamp = KakaoAssetStampOperator(
    task_id="stamp_summary_ready",
    aliases=[SUMMARY_HOURLY, SUMMARY_EOD, SUMMARY_EOW],
    concrete_asset_builder=lambda ctx: KakaoAsset.logical(
        team="story",
        domain="berriz.bizberry",
        name=f"summary/{ctx['data_interval_start'].in_timezone('Asia/Seoul').strftime('%Y%m%d%H')}",
    ),
    payload={...},
    emit_conditions={
        SUMMARY_EOD: lambda ctx: ctx["data_interval_start"].in_timezone("Asia/Seoul").hour == 23,
        SUMMARY_EOW: lambda ctx: (
            ctx["data_interval_start"].in_timezone("Asia/Seoul").hour == 23
            and ctx["data_interval_start"].in_timezone("Asia/Seoul").isoweekday() == 7
        ),
    },
    skip_on_run_type=["backfill"],
)
```

**얻는 것**:
- Stamp 패턴이 팀마다 동일한 형태로 표현됨
- 표준 payload 필드 강제 → downstream 파싱 일관성
- Skip 조건이 well-known 파라미터 (`skip_on_run_type`, `skip_when`)
- Alias fan-out 표준화

### 컴포넌트 3: `AssetEventUnpacker` — Consumer 헬퍼

**목적**: [[4_해결_패턴]] Pattern C 를 헬퍼로 pack. Downstream 의 `triggering_asset_events` 파싱 반복 흡수.

**API 스케치**:
```python
from airflow.providers.kakaoent.dataplatform.assets import AssetEventUnpacker

@task
def gather(**context):
    unpacker = AssetEventUnpacker(context)
    
    # partition_kst 별로 group, 모든 subscribed asset 이 다 온 partition 만 반환
    return unpacker.grouped_by(
        "partition_kst",
        require_all_assets=True,
    )

@task
def process(payload):
    ...

process.expand(payload=gather())
```

**Unpacker 가 흡수하는 것들**:
- `triggering_asset_events` 딕셔너리 순회
- Extra field 안전한 접근 (없으면 default, 있으면 파싱)
- Grouping / filtering / matching
- Producer run_type 별 분기 (backfill / scheduled)
- 특정 asset 만 필터 (`only=[SUMMARY_READY]`)
- Orphan event 처리 정책

**주요 메서드 예시**:
```python
unpacker.all_events()                              # 전체 flat list
unpacker.by_asset()                                # 자산별 dict
unpacker.grouped_by("partition_kst")               # 필드별 group
unpacker.filtered(lambda ev: ev.extra["kst_hour"] == 23)  # 조건 필터
unpacker.stats()                                   # 카운트 / 시간 범위 등 진단
```

**얻는 것**:
- Consumer 파싱 로직 표준화
- Payload 필드 없을 때 fail-fast (하드코딩된 파싱보다 낫음)
- 팀 간 downstream 처리 로직 공유

## Provider 에 넣지 말아야 할 것 (범위 관리)

**Cosmos wrapper**
- 팀마다 dbt 프로젝트 구조가 다름
- `DbtTaskGroup(...)` 감싸는 순간 cosmos 업그레이드가 provider 릴리스에 발이 묶임
- → 팀 util 로 남기는 게 좋음

**비즈니스 로직 (mart 별 config)**
- Bizberry 특화 mart 목록 같은 건 팀 레포에 유지

**DAG 팩토리**
- "이 config 주면 전체 DAG 를 생성" 같은 magic
- 초기엔 편해도 나중에 커스터마이징이 지옥
- 계약 (operator, helper) 만 제공하고 조립은 팀이

## 롤아웃 단계 (4단계)

Provider 변경은 blast radius 가 커서 신중해야 함. 순서:

### 1단계 (즉시) — `KakaoAsset` URI builder

- 부작용 zero, 순수 helper
- 팀들이 자연스럽게 쓰기 시작
- 리스크 낮음
- **의사결정 필요**: URI 스킴 (`kakaoent://` 등) 표준 확정

### 2단계 (~2주) — Story 팀 로컬 프로토타입

- Story 팀이 stamp operator + unpacker 를 팀 util 로 프로토타입
- 실제 DAG 하나 (`berriz_bizberry_downstream_demo`) 에서 실전 검증
- 발견되는 이슈 / 필요 파라미터 목록화

### 3단계 (~1개월) — Provider 로 승격

- 프로토타입 안정화 후 provider PR
- 다른 팀에 공지, 문서화
- 마이그레이션 가이드 (URI 규약, payload 스키마)

### 4단계 (~2개월+) — 확장

- Unpacker 고급 기능 (매칭 정책, 상태 저장 헬퍼)
- Metrics / logging 표준 (asset event 카운터 등)
- Airflow 3 minor 업데이트 대응 (`AssetWatcher` 등 붙는 대로)

## 안티패턴 / 조심할 점

### AP-1. Payload 스키마를 팀 자유에 완전 맡기기

**증상**: 팀마다 `execution_date` / `logical_date` / `date_str` 다르게 씀. Downstream 이 팀마다 다르게 파싱.

**대응**: Provider 가 **표준 필드는 강제**. 자유 필드는 `custom` 서브 딕셔너리로 격리:
```python
payload = {
    "partition_kst": "...",       # 표준
    "kst_date": "...",            # 표준
    "custom": {                   # 팀 자유 영역
        "our_special_field": "..."
    }
}
```

### AP-2. Escape hatch 없이 완전 대체

**증상**: Provider 로 못 표현하는 케이스가 나올 때 팀이 raw Airflow API 로 우회 못 하도록 막힘.

**대응**: Operator 로 감싸도 원본 API 는 항상 열려있음을 명시. "필요하면 팀이 커스텀 stamp 를 짜도 됨" 을 문서화.

### AP-3. Airflow API 진화 무시하고 촘촘히 잡기

**증상**: Airflow 3 마이너 버전에서 `outlet_events` API 가 바뀌면 provider 가 통째로 깨짐.

**대응**:
- Provider API 를 얇게 유지 (파라미터 최소화)
- Airflow 3 minor 마다 회귀 테스트
- `AssetWatcher`, partitioned assets 같은 새 API 는 붙는 걸 확인 후 지원

### AP-4. 버전 매트릭스 불명확

**증상**: Provider 가 어떤 Airflow / cosmos 버전을 지원하는지 애매. 팀이 업그레이드하면서 provider 가 깨짐.

**대응**: Provider README 에 지원 매트릭스 명시. 예: "provider vX.Y 는 airflow 3.1.x, cosmos 1.5.x+ 지원"

## 논의 필요 사항 (월요일 미팅 아젠다 후보)

1. **URI 스킴 확정** — `kakaoent://` 로 갈지, 다른 이름 (`k11e://` 등)?
2. **표준 payload 필드 목록** — 위 표에서 추가/삭제할 필드?
3. **네이밍 규약** — Alias 이름 컨벤션 (`{team}.{domain}.{mart}.{condition}_ready`?)
4. **Cosmos outlet 정책** — 조직 차원에서 켜둘 지침?
5. **1단계 (`KakaoAsset` builder) 를 언제 릴리스** — Story 팀 프로토타입 전에 먼저 배포?
6. **책임 분담** — Provider 개발은 데이터플랫폼팀? Story 팀 contributor 로?

## 관련 문서

- [[0_결론]] — 이 제안의 executive 요약
- [[4_해결_패턴]] — 이 제안이 표준화하려는 패턴들
- [[5_계층_분리_원칙]] — Provider 컴포넌트가 어느 계층에 속하는지
- [[8_시연_스토리라인]] — 시연 chapter 4 에 해당
