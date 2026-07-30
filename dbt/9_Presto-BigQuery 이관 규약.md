# Presto → BigQuery 이관 규약 v0.1 (Draft)

**대상**: `mlb-dbt`, `musicdata-lab-dbt` 그 외 이관 예정 dbt 프로젝트
**작성 근거**: `storydata-dbt` 파일럿 실측 + 두 프로젝트 인벤토리 스캔
**상태**: 초안 — 미확정 항목은 Part VI 에 별도 표기

---

## 0. 요약 (한 페이지 규약)

| 항목 | 표준 |
|---|---|
| Adapter | `dbt-bigquery` (dbt-core 1.9+) |
| Incremental strategy | **`insert_overwrite`** (기본) / `merge` (CDC 예외 승인제) |
| 파티션 타입 | **`DATE`** (일별) / **`DATETIME`** (시간별, KST 벽시계) / **`INT64`** (range) |
| 파티션 컬럼명 | `_kst` 또는 `_utc` suffix 강제. suffix 없는 이름 금지 |
| 원본 타임스탬프 | `event_ts_utc TIMESTAMP` 로 별도 보존 |
| `require_partition_filter` | 모든 파티션 테이블 `true` |
| `maximum_bytes_billed` | profile 필수 |
| `labels` | `{team, layer, domain, owner}` 필수 |
| 세션 `SET @@time_zone` | 아드혹 분석에만 사용. 프로덕션 SQL은 명시적 인자 |
| 커스텀 어댑터 | 폐기 (`dbt-custom-adapter`, `dbt-custom-trino`) |

---

# Part I. 표준

## 1. Adapter / Profile

### 1.1 표준 `profiles.yml` 템플릿

```yaml
<project_name>:
  target: production
  outputs:
    production:
      type: bigquery
      method: service-account          # 또는 oauth (ADC)
      project: <gcp-project-id>
      dataset: <default-dataset>
      location: asia-northeast3
      threads: 8

      # 비용·성능 거버넌스 (필수)
      priority: batch                  # 백필용 (개발: interactive)
      maximum_bytes_billed: 5000000000000   # 5TB, 팀별 조정

      # 타임아웃
      timeout_seconds: 3600
      job_creation_timeout_seconds: 60
      job_execution_timeout_seconds: 3600
      job_retries: 3
```

### 1.2 금지

- `session_properties`, `X-Presto-*` HTTP 헤더 관련 옵션 전부
- `maximum_bytes_billed` 없는 profile (안전핀 없이 배포 금지)

---

## 2. Materialization

| 유형 | 사용 |
|---|---|
| `table` | 일반 마트 |
| `view` | 뷰. `grant_access_to`로 다운스트림 노출 |
| `incremental` | 파티션 기반 재빌드 (기본) |
| `ephemeral` | CTE 인라인 (재사용 필터) |
| `materialized_view` | Silver→Gold hot path 자동 갱신 (신중히) |
| `snapshot` | 히스토리 추적 필요 시 (Presto Hive에선 못 썼음, 이제 사용 가능) |
| Python 모델 | 필요 시 BigFrames 우선, Dataproc은 예외 승인 |

---

## 3. Incremental Strategy ⭐

### 3.1 표준

**`insert_overwrite` 를 프로젝트 전역 표준**으로.

```yaml
# dbt_project.yml
models:
  <project>:
    +incremental_strategy: insert_overwrite
```

**이유**: 두 프로젝트의 기존 관행 `delete+insert` + Hive 세션 트릭은 "파티션 통째 덮어쓰기" 시맨틱을 만들기 위한 우회로였음. BQ의 `insert_overwrite`가 동일 시맨틱을 네이티브로 제공.

### 3.2 예외 (승인제)

| 전략 | 사용 조건 |
|---|---|
| `merge` | CDC/upsert 모델 신설 시. `unique_key` 명시 + `incremental_predicates`로 파티션 프루닝 강제 필수. `tag: 'merge-approved'` 부여 |
| `microbatch` | 지연 도착 데이터 처리 시. Bronze 원본 로그 계층 한정 |
| `append` | 감사 로그성. 중복 제거 없이 누적만 |
| `insert_only` (커스텀) | Hive block-replace 시맨틱이 필요한 특수 케이스. `music-dbt-utils`에서 관리 |

### 3.3 금지

- 개별 모델 `config()`에서 `incremental_strategy` 오버라이드 (예외 승인 없이)
- `unique_key` 없이 `merge` 사용

---

## 4. 파티셔닝 & 타임존 ⭐⭐⭐ (가장 중요)

### 4.1 파티션 타입 선택 매트릭스

| 케이스 | 파티션 컬럼 타입 | granularity | 컬럼 이름 |
|---|---|---|---|
| KST 비즈니스 **일별** 스냅샷 | `DATE` | `day` | `partition_date` (예외적으로 suffix 생략 허용) |
| KST 비즈니스 **시간별** 스냅샷 | `DATETIME` (KST 벽시계) | `hour` | `partition_dt_kst` |
| UTC 이벤트 로그 **일별** | `DATE` | `day` | `partition_date_utc` |
| UTC 이벤트 로그 **시간별** | `DATETIME` (UTC 벽시계) | `hour` | `partition_dt_utc` |
| 정수 range | `INT64` | `range` | `partition_id` |

### 4.2 금지 사항

| 금지 | 이유 |
|---|---|
| **STRING 파티션 (`VARCHAR(8)` YYYYMMDD)** | BQ가 STRING 파티션 불허 |
| **`TIMESTAMP` 를 파티션 컬럼으로 사용** | UTC boundary 강제 → KST 시각과 어긋남 (실측 44% 데이터 손실 사례) |
| **다중 파티션 컬럼 (`partition_date` + `partition_hour`)** | BQ 단일 컬럼만 지원. hourly는 `DATETIME`으로 통합 |
| **`_dt` 등 suffix 없는 컬럼명** | 타임존 규약 모호 |
| **`require_partition_filter=false`** | 예외 승인제. 기본은 `true` |

### 4.3 원본 타임스탬프 보존 (필수)

파티션 컬럼과 별도로 **원본 절대 순간 컬럼을 반드시 보존**:

```sql
SELECT
  DATETIME(event_ts, 'Asia/Seoul')  AS partition_dt_kst,  -- 파티션 키
  event_ts                          AS event_ts_utc       -- 원본 절대 순간 보존
```

**이유**: 파티션 컬럼만 있으면 재변환·다른 tz 리포팅·감사 시 정보 손실.

### 4.4 조회 규약

**KST 비즈니스 일자 조회**:

```sql
-- ✅ DATETIME 파티션
WHERE DATE(partition_dt_kst) = DATE '2026-07-20'

-- ✅ DATE 파티션
WHERE partition_date = DATE '2026-07-20'
```

**금지 패턴**:

```sql
WHERE DATE(event_ts_utc) = DATE '2026-07-20'   -- ⛔ UTC 기준 판정, 44% 손실
WHERE partition_date = '20260720'              -- ⛔ 문자열 비교 (타입 안 맞음)
```

### 4.5 타임존 함수 규약

**프로덕션 dbt SQL에는 세션 tz 의존 금지**. 모든 timezone-sensitive 함수는 명시적 인자:

```sql
DATE(ts, 'Asia/Seoul')
DATETIME(ts, 'Asia/Seoul')
FORMAT_TIMESTAMP('%Y-%m-%d', ts, 'Asia/Seoul')
EXTRACT(HOUR FROM ts AT TIME ZONE 'Asia/Seoul')
```

또는 프로젝트 매크로로 감싸기:

```sql
-- macros/tz.sql
{% macro to_kst_date(col) %}DATE({{ col }}, 'Asia/Seoul'){% endmacro %}
{% macro to_kst_datetime(col) %}DATETIME({{ col }}, 'Asia/Seoul'){% endmacro %}
```

### 4.6 세션 tz 활용 (아드혹만)

**BQ 콘솔·노트북 분석 세션에만 사용**:

```sql
-- 세션 부트스트랩 (팀 관행)
SET @@time_zone = 'Asia/Seoul';
SET @@dataset_project_id = 'dev-dp-project-354904';
SET @@dataset_id = 'datawarehouse_berriz';
```

**금지**: `on-run-start`, `pre_hook`에 `SET @@time_zone` — dbt 세션 격리 때문에 안 먹음. 오히려 혼란만 유발.

---

## 5. 클러스터링 & 스토리지

### 5.1 파티션 vs 클러스터링

| 축 | Partitioning | Clustering |
|---|---|---|
| 물리 구조 | 별도 세그먼트/파일 | 같은 파일 내 정렬 순서 |
| 프루닝 정밀도 | 파티션 단위 (완전 스킵) | 블록 단위 (부분 스킵) |
| 컬럼 수 | 1개 | 최대 4개 |
| 컬럼 타입 | `DATE`/`TIMESTAMP`/`DATETIME`/`INT64` | `STRING`/정수/시간/`BOOL`/`NUMERIC` 등 광범위 |
| 상한 | 4,000 파티션/테이블 | 없음 |
| 재정렬 | 수동 (파티션 재빌드) | **BQ 백그라운드 자동** |

**원칙**: **파티션 = 시간축**, **클러스터 = 조회 축**. 함께 써야 진짜 효과.

**Presto/Hive 대비**: Hive에도 `bucketed_by`/`sorted_by` 개념 있으나 관리 부담 커서 두 프로젝트 실사용 0회. BQ는 auto-managed·저비용이라 실질 신기능.

### 5.2 `cluster_by` 규약

#### 컬럼 선정 원칙

- **조회 필터·JOIN 키로 자주 쓰이는 컬럼** 선정
- **최대 4개**, 순서가 성능에 결정적
- **파티션 컬럼은 중복 포함 금지** (이미 물리 분할됐음)

#### 컬럼 순서 규칙 ⭐

`cluster_by=[a, b, c]`는 **a로 정렬 → 같은 a 안에서 b → 같은 (a,b) 안에서 c** 계층 정렬.

| 쿼리 필터 | 이득 |
|---|---|
| `WHERE a = ?` | ✅ 최대 (1차 축) |
| `WHERE a = ? AND b = ?` | ✅ 최대 |
| `WHERE b = ?` (a 없이) | ❌ 이득 거의 없음 |
| `WHERE c = ?` (a, b 없이) | ❌ 이득 없음 |

**앞쪽 배치 원칙**: 자주 필터되고 카디널리티 낮은 축 → 앞. 카디널리티 높은 상세 축 → 뒤.

| 카디널리티 | 예시 | 위치 |
|---|---|---|
| 낮음 (수십~수백) | `platform_code`, `country_code`, `status` | 앞쪽 |
| 중간 (수천) | `chart_id`, `community_id`, `board_id` | 중간 |
| 높음 (수백만+) | `isrc`, `user_id`, `post_id`, `track_id` | 뒤쪽 |

#### 레이어별 표준 클러스터 축 (권장)

| 레이어 | 표준 `cluster_by` |
|---|---|
| Bronze | `[source_platform]` 또는 `[ingest_batch]` |
| Silver track | `[platform_code, country_code, isrc]` |
| Silver artist | `[platform_code, artist_id]` |
| Silver chart | `[chart_id, country_code]` |
| Silver playlist | `[platform_code, playlist_id]` |
| Gold mart | 조회 API의 주 필터 축 우선 |
| API v2 | `[platform_code, primary_key]` |

레이어·도메인별 구체 결정은 팀별 협의. 위는 시작점 가이드.

### 5.3 스토리지 옵션

| 옵션 | 표준 |
|---|---|
| `partition_expiration_days` | Bronze 730일, Silver 1095일, Gold 무기한 (팀 협의) |
| `hours_to_expiration` | temp/실험 테이블 2~24시간 |
| `format` | **삭제** (BQ 관리형 기본) |
| `cache.enabled` | **삭제** (BQ 자동 캐시) |
| `location` | `asia-northeast3` (서울) |
| `kms_key_name` | 민감 데이터 dataset만 |

---

## 6. Contracts & Data Type

### 6.1 표준

**모든 모델에 `contract.enforced: true`** (musicdata-lab-dbt 관행을 mlb-dbt에도 확장).

### 6.2 Data Type 매핑표

| Presto | BigQuery |
|---|---|
| `varchar`, `varchar(N)` | `STRING` |
| `bigint` | `INT64` |
| `integer` | `INT64` (BQ는 32-bit 없음) |
| `double` | `FLOAT64` |
| `decimal(38,0)` | `BIGNUMERIC` |
| `decimal(p,s)` (s≤9, p≤38) | `NUMERIC` |
| `decimal` (그 외) | `BIGNUMERIC` |
| `date` | `DATE` |
| `timestamp` | **`TIMESTAMP`** (UTC 절대 시각) 또는 **`DATETIME`** (벽시계) — 컨텍스트에 따라 |
| `boolean` | `BOOL` |
| `array(varchar)` | `ARRAY<STRING>` |
| `map(varchar, varchar)` | `ARRAY<STRUCT<key STRING, value STRING>>` 또는 `JSON` |
| `row(...)` | `STRUCT<...>` |
| `varbinary` | `BYTES` |

### 6.3 YML `data_type` 규약

- `varchar(N)` 길이 명시 **금지** (BQ에 개념 없음)
- `type:` 키 사용 **금지** — 반드시 `data_type:`

---

## 7. 라벨 · 비용 거버넌스

### 7.1 필수 라벨

모든 모델 config에:

```yaml
+labels:
  team: mdl                          # 팀
  layer: bronze                      # bronze/silver/gold/api
  domain: melon                      # 도메인 (선택)
  owner: <email prefix>              # 개인 오너
```

### 7.2 비용 상한

- **profile**: `maximum_bytes_billed` 필수 (스캔 초과 시 실패)
- **백필**: 별도 계정/프로젝트로 분리 검토 (실수로 프로덕션 예산 소진 방지)
- **개별 모델 오버라이드**: 대용량 GROUP BY 마트는 별도 상한

### 7.3 우선순위

| 시나리오 | `priority` |
|---|---|
| 일일 배치 | `batch` (슬롯 절약) |
| 개발·재시도 | `interactive` |
| 백필 | `batch` 필수 |

### 7.4 Reservation (슬롯 예약) 라우팅

BQ 요금제 결정에 따라 두 방식:

| 요금제 | 특징 |
|---|---|
| **On-demand** (기본) | 스캔 바이트당 과금. 소규모엔 유리, 백필 폭탄 위험 |
| **Capacity (Reservation)** | 슬롯 예약 → 예측 가능한 고정 비용, 워크로드 격리 |

#### 7.4.1 BQ 側 Reservation 계층

```
Capacity Commitment (슬롯 구매 단위, 연/월/flex)
    ↓
Reservation (슬롯 pool에 이름)
    ↓
Assignment (어떤 대상이 어떤 reservation 쓸지)
```

**Assignment 대상**: Organization / Folder / Project / (계층적 sub-reservation)
**Assignment 잡 유형**: `QUERY` (대화형) / `PIPELINE` (배치) / `ML_EXTERNAL` / `BACKGROUND` / `CONTINUOUS`

#### 7.4.2 dbt에서 Reservation 설정 계층 ⭐

**4단계** (아래로 갈수록 우선):

| 단계 | 위치 | 적용 범위 |
|---|---|---|
| 1. **Target** | `profiles.yml` | 그 target(dev/prod)의 모든 잡 |
| 2. **Project 전역** | `dbt_project.yml` 최상위 `models:` | dbt 프로젝트 전체 |
| 3. **폴더/서브폴더** | `dbt_project.yml` 트리 하위 | 해당 경로 이하 모델만 (실무 주력) |
| 4. **개별 모델** | 모델 SQL의 `{{ config() }}` | 그 한 모델만 |

**형식**: `projects/{PROJECT_ID}/locations/{LOCATION}/reservations/{RESERVATION_NAME}`

#### 7.4.3 예시 (musicdata-lab-dbt 가정)

```yaml
# dbt_project.yml
models:
  # (2) 프로젝트 전역 기본
  +reservation: 'projects/mdl/locations/asia-northeast3/reservations/mdl-default'

  musicdata_lab_dbt:
    # (3) 레이어별 오버라이드
    bronze:
      +reservation: 'projects/.../reservations/bronze-ingest'
    silver:
      +reservation: 'projects/.../reservations/silver-transform'
      # (3) 서브폴더까지 더 잘게
      transform:
        track:
          +reservation: 'projects/.../reservations/heavy-track-mart'
    gold:
      +reservation: 'projects/.../reservations/gold-mart'
    api:
      +reservation: 'projects/.../reservations/api-serve'
```

```sql
-- (4) 개별 모델 오버라이드 (특수 헤비 케이스, 승인제)
{{ config(
    reservation='projects/.../reservations/one-off-heavy-backfill'
) }}
```

#### 7.4.4 Dataset / Tag 단위는?

- **Dataset 단위**: dbt config에 직접 축 없음. **하지만 관행상 폴더 = dataset 매칭**이므로 폴더 레벨 config로 사실상 커버.
- **Tag 단위**: 직접 config 다르게 걸긴 어려움. `--select tag:xxx` CLI 실행으로 워크로드 분리는 가능. 편법 우회 있으나 권장 안 함.

#### 7.4.5 실무 권장 조합

**musicdata-lab-dbt (629 모델, 규모 큼)**:
- 레이어별 4개 reservation (bronze / silver / gold / api)
- silver 내부 헤비 폴더(예: `transform/track`)는 별도 reservation 검토
- 개별 모델은 예외적으로만

**mlb-dbt (39 모델, 규모 작음)**:
- 프로젝트 전체 1개 reservation으로 충분
- 백필 시엔 profiles.yml의 backfill target에서 flex reservation 라우팅

**공통 원칙**:
- **프로덕션과 백필은 반드시 별도 reservation** (백필 폭탄 격리)
- **Idle slot sharing ON** 유지 (경제성) — reservation 유휴시 다른 워크로드 borrow 허용
- **Autoscaling reservation** (`baseline_slots` + `max_slots`) 활용 — 백필 기간 자동 증설

#### 7.4.6 실행 단 라우팅 예시

```yaml
# profiles.yml
outputs:
  production:
    priority: batch
    # 프로덕션 배치용 (기본 assignment 사용)
  backfill:
    priority: batch
    # 백필용 target — dbt_project.yml에서 +reservation으로 flex reservation 지정
```

---

## 8. 매크로 & 표준 라이브러리

### 8.1 `music-dbt-utils` BQ 브랜치에 승격 대상

| 매크로 | 출처 | 용도 |
|---|---|---|
| `to_kst_date(col)`, `to_kst_datetime(col)` | 신규 | tz 함수 wrapper |
| `insert_only` incremental strategy | storydata-dbt | Hive block-replace 시맨틱 |
| `generate_alias_name` | storydata-dbt | dev/integration/prod 접미사 |
| `partition_date_range_filter` (BQ 버전) | musicdata-lab-dbt에서 이식 | 백필 range 필터 |
| `previous_partition_date_filter` (BQ 버전) | 이식 | sequenced 모델 어제 참조 |

### 8.2 폐기 대상

| 대상 | 대체 |
|---|---|
| `dbt-custom-adapter` (dbt-trino 래퍼) | 기본 `dbt-bigquery` |
| `dbt-custom-trino` (dbt-trino 포크) | 기본 `dbt-bigquery` |
| `trino__list_relations_without_caching` | BQ 기본 |
| `trino__load_csv_rows` | BQ 로드 잡 (기본) |
| `trino__get_catalog_*` 6종 | BQ `INFORMATION_SCHEMA` |

---

## 9. SQL 방언 매핑 (Reference)

| Presto | BigQuery | 주의 |
|---|---|---|
| `date_format(dt, '%Y%m%d')` | `FORMAT_DATE('%Y%m%d', dt)` | 인자 순서 반대 |
| `date_parse(s, fmt)` | `PARSE_DATE(fmt, s)` | 인자 순서 반대 |
| `date_diff('day', a, b)` | `DATE_DIFF(b, a, DAY)` | 인자·단위 위치 다름 |
| `date_add('day', n, dt)` | `DATE_ADD(dt, INTERVAL n DAY)` | |
| `date_trunc('day', dt)` | `DATE_TRUNC(dt, DAY)` | |
| `try_cast(x AS T)` | `SAFE_CAST(x AS T)` | |
| `try(expr)` | `SAFE.expr` 또는 CASE | 케이스별 |
| `cast(x AS varchar)` | `CAST(x AS STRING)` | |
| `cast(x AS bigint)` | `CAST(x AS INT64)` | |
| `x \|\| y` | `CONCAT(x, y)` | `\|\|`도 지원되지만 표준 `CONCAT` |
| `map_agg(k, v)` | `ARRAY_AGG(STRUCT(k AS key, v AS value))` | 매크로 wrapper 권장 |
| `array_join(arr, ',')` | `ARRAY_TO_STRING(arr, ',')` | |
| `array_agg(x)` | `ARRAY_AGG(x)` | 호환 |
| `regexp_replace(...)` | `REGEXP_REPLACE(...)` | re2 방언 검증 |
| `AT TIME ZONE 'Asia/Seoul'` | 함수 인자로 `'Asia/Seoul'` | 시맨틱 재확인 |
| `current_timestamp` (괄호 없음) | `CURRENT_TIMESTAMP()` | 괄호 필수 |
| `now()` | `CURRENT_TIMESTAMP()` | |
| `$partitions` 시스템 테이블 | `INFORMATION_SCHEMA.PARTITIONS` | |

---

# Part II. 금지 목록

플랫폼 팀 리뷰에서 자동 리젝트 대상:

| 금지 | 이유 |
|---|---|
| **파티션 컬럼에 `STRING`** | BQ 미지원 |
| **`partition_date VARCHAR(8)`** | STRING 파티션 불가, DATE로 승격 |
| **다중 컬럼 파티션 (`partition_date` + `partition_hour`)** | BQ 단일 컬럼만 |
| **파티션 컬럼에 `TIMESTAMP`** (특수 승인 없이) | KST/UTC 함정. `DATETIME` 사용 |
| **파티션 컬럼명 suffix (`_kst`/`_utc`) 누락** | 규약 위반 |
| **`DATE(ts)` (기본 tz)** | 실측 손실 확인됨. 명시 인자 필수 |
| **`\|\|` 문자열 concat** | `CONCAT` 표준 |
| **`current_timestamp` 괄호 없이** | BQ 문법 오류 |
| **개별 모델에서 `incremental_strategy` 오버라이드** | 예외 승인 없이 금지 |
| **`unique_key` 없는 `merge`** | 논리 오류 |
| **`require_partition_filter=false`** | 예외 승인 없이 금지 |
| **`maximum_bytes_billed` 없는 profile** | 비용 안전핀 없음 |
| **`labels` 누락** | 비용 추적 불가 |
| **`format:'parquet'`, `cache.enabled: true`** | Presto 잔재, 삭제 |
| **`pre_hook: "SET @@time_zone ..."`** | dbt 세션 격리로 안 먹음, 혼란 유발 |
| **`type:` 키 사용** (YML에서) | `data_type:` 표준 |
| **`varchar(N)` 길이 명시** (YML에서) | STRING 통일 |

---

# Part III. 검증 방법

## 검증 축 5개

| # | 축 | 방법 |
|---|---|---|
| 1 | Neptune Presto ↔ BQ Parity | 같은 실행 시각 결과 diff (`SUM`, `COUNT`, key-level) |
| 2 | Timezone 규약 (KST 정확성) | 골든존(0-8시 KST) 데이터로 `partition_date == kst_date` 확인 |
| 3 | 파티션 무결성 | `INFORMATION_SCHEMA.PARTITIONS`로 파티션별 row 균등성 |
| 4 | 멱등성 | 같은 vars로 두 번 실행 후 결과 diff |
| 5 | 데이터 무결성 | NOT NULL, unique, 범위, 포맷 dbt tests |

## Timezone 감사 쿼리 (모든 파티션 테이블 대상)

```sql
WITH sample AS (
  SELECT
    partition_date,
    date_format(event_ts AT TIME ZONE 'Asia/Seoul', '%Y%m%d') AS kst_date,
    date_format(event_ts AT TIME ZONE 'UTC',        '%Y%m%d') AS utc_date
  FROM <schema>.<table>
  WHERE partition_date BETWEEN '20260710' AND '20260720'
)
SELECT
  CASE
    WHEN partition_date = kst_date AND partition_date = utc_date THEN 'AMBIGUOUS'
    WHEN partition_date = kst_date THEN 'KST_MATCH'
    WHEN partition_date = utc_date THEN 'UTC_MATCH'
    ELSE 'MISMATCH'
  END AS verdict,
  COUNT(*) AS row_count
FROM sample GROUP BY 1 ORDER BY 2 DESC;
```

## 필수 dbt tests

```yaml
models:
  - name: <mart>
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns: [<pk_columns>, partition_date_or_dt]
    columns:
      - name: partition_dt_kst
        tests: [not_null]
      - name: event_ts_utc
        tests: [not_null]
```

## 이관 완료 판정 기준

- ✅ 축 1 (Parity): key-level 집계 diff < 0.1%
- ✅ 축 2 (Timezone): 모든 파티션 테이블 `KST_MATCH` 99%+ 또는 `UTC_MATCH` 99%+ (혼재 없음)
- ✅ 축 4 (멱등성): 재실행 결과 완전 동일 (RAND 컬럼 제외)
- ✅ 축 5 (dbt tests): 100% pass

---

# Part IV. 이관 순서

### Phase 0 — 결정 확정 (Part VI Open Questions 답 필요)

- Loupe 서비스 GCP 목적지
- Bronze 소스 이관 전략 (BigLake vs 관리형)
- Airflow → Cloud Composer? Spark → Dataproc?
- 파티션 표준 (`DATE`/`DATETIME` KST-벽시계) 최종 승인

### Phase 1 — 공통 기반 (1-2주)

- `music-dbt-utils` BQ 브랜치 릴리스
- 표준 매크로 (tz wrapper, insert_only, generate_alias_name)
- 표준 profiles.yml 템플릿
- 표준 model config 스니펫

### Phase 2 — mlb-dbt 파일럿 (2-3주)

- 39개 모델 이관
- adapter_override.sql 재작성
- 방언 변환 (date_format 304회)
- 검증 축 1~5 실측

### Phase 3 — musicdata-lab-dbt 본편 (4-6주)

- 629개 모델 이관
- hourly 파티션 재설계 (partition_hour 260회 폐지)
- `$partitions` → `INFORMATION_SCHEMA.PARTITIONS`
- sequenced 모델 재검토 (MERGE 도입 가능성)
- Loupe export 재구축

### Phase 4 — 스킬·현황판 재정비 (2-3주)

- `.claude/skills/` BQ 버전 (presto-query, model-dashboard)
- `chartmetric-schema-fix` 대체
- `presto-hive-constraints` → `bigquery-constraints`

---

# Part V. 우선순위·부채 회복 관점

BQ 이관은 단순 엔진 교체가 아니라 **부채 회복**임을 팀에 명시:

### 회복되는 기능 (Presto 0.272에서 못 썼던 것)

- MERGE (upsert)
- 정식 snapshot
- Materialized view
- Grants (IAM 매핑)
- 컬럼 타입 변경
- 트랜잭션
- Row-level / Column-level security
- **자동 관리 클러스터링** — Hive에도 `bucketed_by`/`sorted_by` 개념은 있으나 버킷 수 고정·수동 재정렬·관리 부담이 커서 두 프로젝트 실사용 **0회**. BQ 클러스터링은 auto-managed(백그라운드 자동 재정렬)·저비용·최대 4컬럼 계층 정렬로 사실상 **신규 도구**. 파티션과 조합 시 스캔량 100~1000배 절감 가능.

### 사라지는 부채

- `dbt-custom-adapter` + `dbt-custom-trino` 커스텀 어댑터 2개
- `delete+insert` + Hive 세션 트릭
- `tag:cdc` 직접 구현 CDC (MERGE·snapshot 대체 가능)
- sequenced 모델의 순차 실행 강제
- `--full-refresh` 강제 (컬럼 타입 변경 시)
- parquet 스키마 타입 불일치 (`chartmetric-schema-fix`)

---

# Part VI. 결정 대기 항목 (Open Questions)

플랫폼 팀 → DE·서비스 팀 협의 필요:

| # | 질문 | 답 필요 이유 |
|---|---|---|
| 1 | Loupe 서비스 GCP 목적지 (MongoDB 유지 / BQ 직접 서빙 / 다른 저장소) | musicdata Gold/API 마트 설계 결정 |
| 2 | Bronze 소스 전략 — BigLake 외부 테이블 vs BQ 관리형 로드 | S3 존치 부분과 이관 부분 경계 |
| 3 | Airflow → Cloud Composer 이관 방식 | 스케줄링·백필 오케스트레이션 |
| 4 | Spark(Loupe export) → Dataproc? Dataflow? | 파이프라인 재설계 범위 |
| 5 | `music-dbt-utils` — Presto/BQ dispatch vs 브랜치 분리 | dual-run 기간 정책에 따라 |
| 6 | dual-run 기간 유무 (Presto·BQ 병렬 운영) | 매크로 호환성 전략 결정 |
| 7 | `timestamp` 표준 — `TIMESTAMP`(UTC) vs `DATETIME`(KST) 통일 여부 | 컬럼 명명 규약 완성 |
| 8 | hourly 파티션 폐지 시점 — sequenced 모델 재설계 여부 | musicdata Silver 영향 큼 |
| 9 | `filter(...)` 945회 (musicdata) 의 정체 (배열 함수 vs SQL FILTER 절) | 이관 부하 산정 |
| 10 | 서비스별 KST/UTC 혼재 유지 vs 통일 | 컬럼 명명·조인 규약 |
| 11 | 슬롯 예약(Reservation) 전략 — On-demand 유지 vs Capacity(slot) 전환 | 이관 규모(660+ 모델)에서 On-demand는 비용 예측 불가. 백필 기간 flex slot 도입 검토. 재무팀 협의 필요 |

---

# Part VII. 참고 실측 데이터

## storydata-dbt 파일럿 실측 (2026-07-20)

### KST/UTC 파티션 함정 (`bizberry_community_contents_artistpost_integration`)

같은 데이터에 세 가지 방식으로 "KST 2026-07-20" 조회:

| 쿼리 방식 | 결과 | 판정 |
|---|---|---|
| `WHERE DATE(event_ts) = DATE '2026-07-20'` (기본 UTC) | 1,722 rows | 🔴 44%만 반환 |
| `WHERE event_ts >= TIMESTAMP('2026-07-20 00:00:00', 'Asia/Seoul') AND ...` | 3,936 rows | ✅ 정답 |
| `WHERE DATE(DATETIME(event_ts, 'Asia/Seoul')) = DATE '2026-07-20'` | 3,936 rows | ✅ 정답 |

**결론**: 2,214 rows(56%) 손실 위험. KST 자정~오전 9시 데이터가 UTC 기준으로 전날 파티션.

### BQ 세션 tz 실측

`SET @@time_zone = 'Asia/Seoul';` 세션 설정 후 `TIMESTAMP '2026-07-19 18:00:00 UTC'` (= KST 20일 03시) 테스트:

| 함수 | 결과 | 세션 tz 따름? |
|---|---|---|
| `DATE(ts)` | `2026-07-20` (KST) | ✅ |
| `DATE(ts, 'UTC')` | `2026-07-19` | 명시 인자 우선 |
| `DATETIME(ts)` | `2026-07-20 03:00` (KST 벽시계) | ✅ |
| `EXTRACT(HOUR FROM ts)` | `3` (KST 시간) | ✅ |
| `CURRENT_DATE()`, `CURRENT_DATETIME()` | KST | ✅ |
| `FORMAT_TIMESTAMP()` | KST | ✅ |

**결론**: 세션 tz가 광범위하게 먹음. 하지만 dbt-bigquery 세션 격리로 프로덕션 dbt에서는 신뢰 불가. **아드혹 세션에만 활용**.

---

**문서 상태**: v0.1 draft. Open Questions 답 회수 후 v0.2로 승격.
