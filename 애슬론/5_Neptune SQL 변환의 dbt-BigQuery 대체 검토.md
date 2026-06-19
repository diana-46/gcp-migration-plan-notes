# 5. Neptune SQL 변환의 dbt-BigQuery 대체 검토

## 배경 및 전제

- athlon api 의 `com.kakaopage.athlon.neptune` 패키지는 AWS Neptune 이 아닌 **사내 ETL 오케스트레이션 프레임워크**의 코드네임
- 사용자가 GraphQL 로 ETL 스펙(Presto SQL + 파티션 + 스케줄)을 정의하면 Airflow DAG 을 자동 생성/배포하는 메타데이터 레이어
- **이번 검토 범위**: SQL 변환 영역만. 오케스트레이션·센서·외부 연동·DAG 생성은 향후 **Python 으로 자체 작성** 예정이므로 dbt 가 책임지지 않음
- **타깃 워크로드**: BigQuery (Hive/Presto 기반 데이터레이크에서 BQ 로 이전)

---

## Neptune 의 SQL 변환 영역 현황

### 입력 포맷 두 가지

| 포맷 | 구조 | 비고 |
|---|---|---|
| **PLAIN** | SQL 한 덩어리. temp 자동 생성 → perm 으로 merge | 후크 없음. `EtlCodeDoc.kt` |
| **YAML** | 1 perm + N temp 잡, 각 잡에 `before` / `main` / `after` exec point | `YamlPipelineBuilder.kt`, `EtlCodeYamlUtil.kt:48-101` 에서 검증 |

YAML 스키마 (필수/제약):
```yaml
job:
  name: "temp_table_name"          # CreateTempTable 일 때만 필수
  type: CreatePermTable | CreateTempTable
  details:
    - execPoint: before | main | after   # main 은 잡당 정확히 1개 필수
      codeType: presto                    # "presto" 만 지원
      code: |
        SELECT ...
```

다중 문서 YAML (`---` 구분) 가능. PLAIN/YAML 분기는 `WorkflowService.getPipelineBuilder()` 가 `etl.codeFormat` 으로 라우팅.

### 파티션 (`EtlPartition.kt`, `EtlTaskAddPartition.kt`)

- `partitionKey` + `partitionValue` (Jinja2 매크로) 로 선언
- 예: `{{ data_interval_start.in_timezone(dag.timezone).strftime('%Y-%m-%d') }}`
- **SQL 본문에 WHERE 자동 주입 안 함** — 사용자가 직접 필터 작성
- 매 실행마다 `ALTER TABLE ADD PARTITION (key='value') LOCATION '/path/key=value/'` 로 메타 등록
- HDFS 경로 컨벤션: `/{db}/{phase}/modeled/neptune/{table}/key=value/`
- 백필: `startDt~endDt` 범위로 ETL 단위 재실행, `maxActiveRuns` 동시성 제어 (`BackfillService.kt`)

### 출력 포맷 (`Etl.kt:72-74`)

- `OutputFormat` enum: **PARQUET (기본) / AVRO** 두 개만
- 흥미로운 구현 디테일: Presto `CREATE TABLE WITH (format='PARQUET')` 은 하드코딩, AVRO 는 별도 `merge_and_move_dataframes.sh --output-format avro` 스크립트로 **사후 변환** (`QueryBaseCommandHelper.kt:14-28`)

---

## 0. 모든 평가 위에 깔리는 전제: SQL 방언 번역

이건 dbt 의 능력과 무관하지만, **마이그레이션 작업의 가장 큰 비중을 차지할 가능성이 높음**. dbt 가 자동으로 해주는 게 아니라 사람이 SQL 을 고쳐야 함.

자주 걸리는 변환:
- `date_trunc('day', x)` → `DATE_TRUNC(x, DAY)`
- `from_unixtime(x)` → `TIMESTAMP_SECONDS(x)`
- `cast(x as decimal(10,2))` → `CAST(x AS NUMERIC)`
- `array_agg`, `array_join`, `unnest` 시그니처 미묘하게 다름
- `||` 문자열 연결 → `CONCAT(...)` (BQ 도 `||` 지원하지만 NULL 처리 다름)
- `approx_distinct` → `APPROX_COUNT_DISTINCT`
- WITH 절 안의 RECURSIVE / lateral join 패턴 차이

→ 별개의 **마이그레이션 비용 라인 아이템**으로 산정 필요.

---

## 1. PLAIN / YAML 포맷 매핑 (BigQuery 기준)

### PLAIN
dbt model 1개에 1:1 매핑. SQL 방언만 BQ 로 바꾸면 끝.

```sql
{{ config(materialized='table') }}
SELECT ...
```

### YAML 의 `before` / `main` / `after`

| Neptune                                               | dbt-bigquery 대응                                                  |
| ----------------------------------------------------- | ---------------------------------------------------------------- |
| `before: SET SESSION QUERY_MAX_TOTAL_MEMORY = ...`    | **BQ 동등물 없음**. 메모리/슬롯은 reservation/job priority 레벨에서 제어 → 대부분 드롭 |
| `before: SET SESSION query_priority = 'low'`          | dbt-bigquery profile 의 `priority: 'batch'`                       |
| `main`                                                | 모델 본문 SELECT                                                     |
| `after: SELECT COUNT(*) FROM ... HAVING count = 0` 검증 | dbt `tests` 로 매핑 (실패 시 run 중단)                                   |
| `after: INSERT INTO audit_log ...` 사이드 이펙트            | `post_hook`                                                      |

⚠️ **dbt-bigquery 의 `pre_hook` 은 Trino 와 달리 별도 BQ job 으로 실행됨** — 같은 세션이 아니므로 Presto 의 `SET SESSION` 같은 "본 쿼리에 영향 주는 세션 설정" 패턴은 BQ 에서 의미를 잃음. 다행히 BQ 가 그런 튜닝을 거의 필요로 하지 않음 (옵티마이저가 자동 처리).

### YAML 다중 temp 테이블

- 진짜 중간 단계 → `materialized='ephemeral'` (CTE 인라인). BQ 에서 권장 — 옵티마이저가 잘 처리하고 물리 테이블 생성 시 스토리지 비용 발생
- 물리화 필요한 경우 → `materialized='table'` + `ref()` 체인

### PLAIN ↔ dbt 파일 분할

Neptune YAML 은 "탑다운 한 파일에 모든 잡 기술", dbt 는 "잡 = 파일". **자동 변환 시 파일 분할이 필요**.

---

## 2. 파티션 — BigQuery 가 훨씬 깔끔함

여기가 BQ 이전의 가장 큰 이득 포인트.

**dbt-bigquery 의 `insert_overwrite` 패턴이 Neptune 의 "한 파티션 다시 빌드" 시맨틱과 거의 완벽 매칭**:

```sql
{{ config(
    materialized='incremental',
    partition_by={
      'field': 'create_date',
      'data_type': 'date',
      'granularity': 'day'
    },
    incremental_strategy='insert_overwrite',
    partitions=["DATE('{{ var("run_date") }}')"]
) }}

SELECT
  ...,
  DATE('{{ var("run_date") }}') AS create_date
FROM {{ ref('source') }}
WHERE event_ts >= TIMESTAMP('{{ var("run_date") }}')
  AND event_ts <  TIMESTAMP_ADD(TIMESTAMP('{{ var("run_date") }}'), INTERVAL 1 DAY)
```

이 한 모델이 Neptune 의 다음 단계 전부를 대체:
- temp 테이블 생성 (BQ 가 내부 staging)
- merge & move (insert_overwrite 가 처리)
- `ALTER TABLE ADD PARTITION ... LOCATION ...` → **불필요** (BQ 네이티브 파티션은 메타 등록 개념 없음)
- 외부 테이블 location 관리 → **불필요** (BQ 가 스토리지 직접 관리)

### 매핑 표

| Neptune (Hive/Presto)                                                | dbt-bigquery                                                           |
| -------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| `partitionKey: "create_date"`                                        | `partition_by.field: 'create_date'`                                    |
| `partitionValue: "{{ data_interval_start...strftime('%Y-%m-%d') }}"` | `--vars '{run_date: "2026-06-15"}'` 로 주입                               |
| hourly: `strftime('%Y-%m-%d-%H')`                                    | `granularity: 'hour'` (DATETIME/TIMESTAMP 컬럼)                          |
| `ALTER TABLE ADD PARTITION ...` task                                 | **삭제**                                                                 |
| HDFS 경로 컨벤션 `/db/.../key=value/`                                     | **삭제** — BQ 가 알아서                                                      |
| 백필 범위 (`startDt~endDt`, `maxActiveRuns`)                             | Python 에서 date loop + `dbt run --vars` 반복 + `asyncio.Semaphore` 동시성 제어 |
| 파티션 클러스터 키 (Neptune 엔 없음)                                            | `cluster_by=['user_id']` 옵션 보너스 성능                                     |

### 주의 포인트

⚠️ **타입 일치**: `partitions` 리스트와 `partition_by.field` 의 타입이 일치해야 함. 한 파티션을 `["DATE('2026-06-15')"]` 로 넘기면 그 파티션만 `DELETE` 후 `INSERT` — Neptune 의 "재실행 = 그 파티션 덮어쓰기" 와 정확히 같은 시맨틱.

⚠️ **시간대 처리**: Neptune 은 `in_timezone(dag.timezone)` 매크로로 KST 등 로컬 타임존에서 partition value 생성. BQ 파티션은 보통 UTC 기준이라 Python 오케스트레이터가 vars 로 넘길 때 **타임존 변환을 명시적으로 처리**해야 함. 안 하면 자정 근처 데이터가 다른 파티션으로 감.

⚠️ **`is_incremental()` 마이그레이션**: Neptune 은 SQL 에 WHERE 자동 주입 안 함 → 기존 ETL SQL 을 그대로 dbt 로 옮기면 **매 실행마다 전체 데이터 재계산 후 한 파티션에 덮어쓰기** 가 됨. `is_incremental()` 블록 추가 작업 필요.

---

## 3. Avro 출력 — 근본적으로 다른 그림

**BigQuery 는 "테이블을 Avro 포맷으로 저장한다" 는 개념이 없음.** BQ 테이블은 BQ 의 내부 컬럼나 스토리지(Capacitor) 에만 존재.

Avro 가 필요한 진짜 이유에 따라 두 갈래:

### 경우 A: 다운스트림이 BQ 테이블을 직접 쿼리하면 됨 → Avro 불필요

- dbt model → BQ 테이블 → 끝
- 컨슈머가 다른 시스템이어도 BQ Storage Read API / Federated query 로 접근 가능
- **이게 가능하다면 가장 깔끔한 답**

### 경우 B: 다운스트림이 진짜 GCS 의 Avro 파일을 필요로 함 → 별도 export 단계

dbt 가 직접 Avro 를 못 씀. 패턴 둘 중 하나:

**옵션 1: dbt `post_hook` 에 EXPORT DATA 끼우기**
```sql
{{ config(
    materialized='table',
    post_hook="""
      EXPORT DATA OPTIONS(
        uri='gs://bucket/path/{{ var("run_date") }}/*.avro',
        format='AVRO',
        overwrite=true
      ) AS SELECT * FROM {{ this }}
    """
) }}
SELECT ...
```

**옵션 2: Python 오케스트레이터가 dbt run 후 별도 EXPORT job 호출** ← 사용자가 이미 가는 방향과 자연스럽게 맞물림
```python
client.query(f"""
  EXPORT DATA OPTIONS(uri='gs://.../*.avro', format='AVRO', overwrite=true)
  AS SELECT * FROM `proj.dataset.table` WHERE create_date = '{run_date}'
""").result()
```

### Avro 호환성 리스크

BQ `EXPORT DATA ... format='AVRO'` 의 스키마 매핑은 BQ 의 룰을 따름:
- `NUMERIC` → Avro `bytes` + `decimal` logical type
- `TIMESTAMP` → `long` + `timestamp-micros` logical type
- `STRUCT` → Avro `record`
- `REPEATED` → Avro `array`
- `use_avro_logical_types` 옵션 켜야 일부 타입이 logical type 으로 나옴

Neptune 의 `merge_and_move_dataframes.sh --output-format avro` 가 만들던 스키마와 **거의 확실히 다름** (필드명 케이스, namespace, decimal vs string 표현 등). 다운스트림이 schema-strict 한 consumer (Spark/Flink/Hive external table) 라면 Avro 스키마 호환성 깨질 가능성 큼.

**검증 권장**: BQ 에서 가장 자주 쓰이는 Avro 테이블 1개를 `EXPORT DATA` 로 떠보고, `avro-tools getschema` 로 기존 산출물과 diff. 한 번의 비교가 마이그레이션 리스크의 90% 를 보여줌.

---

## 사라지는 Neptune Task 들 (참고)

BQ 가 처리 메인이 되는 순간 다음 task 들은 거의 다 의미를 잃음:

| Task | BQ 이후 |
|---|---|
| `BIGQUERY_JOB` | dbt 자체가 BQ job 발행자 |
| `BQ_LOAD` (GCS→BQ) | 데이터가 이미 BQ 안 → 불필요 |
| `BIGQUERY_SENSOR` | dbt run 의 동기 호출을 Python 이 await |
| `GCS_UPLOAD` | BQ 출발이라 GCS 로 나갈 일은 EXPORT DATA 한 가지 |
| `HIVE_TO_GCS_SYNC` | 동일 — EXPORT DATA 로 흡수 |

---

## 종합 평가표

| 항목 | 평가 | 비고 |
|---|---|---|
| PLAIN | ✅ 1:1 매핑 | SQL 방언 번역 비용 추가 |
| YAML before/main/after | ✅ pre_hook / 본문 / tests+post_hook | `SET SESSION` 류 before 는 BQ 에서 무의미해 드롭 |
| YAML 다중 temp | ✅ ephemeral 권장 | BQ 에서 더 자연스러움 |
| **파티션** | ✅✅ **`insert_overwrite` 가 Neptune 시맨틱 거의 완벽 매칭** | Hive 기준보다 훨씬 깔끔. ADD PARTITION/external_location 고민 사라짐 |
| 백필 | ✅ Python date loop + vars | 변함없음 |
| PARQUET 출력 | N/A | BQ 는 내부 포맷, 사용자 선택 없음 |
| **Avro 출력** | ⚠️ **dbt 영역 밖, EXPORT DATA 로 별도 단계** | 스키마 호환성 POC 필수 |
| Avro 가 정말 필요한지 재확인 | 🟡 | 다운스트림이 BQ 직접 쿼리 가능하면 Avro 자체가 불필요할 수도 |
| SQL 방언 번역 | 🟡 | 자동화 불가, 사람이 수정. 마이그레이션 공수의 큰 부분 |

---

## 다음 액션

1. **Avro POC** — 가장 자주 쓰이는 Avro ETL 1개 골라 `EXPORT DATA ... format='AVRO'` 로 산출 → `avro-tools getschema` 로 기존과 diff. 호환성 리스크 조기 확정
2. **복잡 YAML 변환 POC** — 파티션 + before/main/after 다 쓰는 YAML ETL 1개 골라 dbt 모델 1개로 변환. PLAIN 보다 손이 가는 케이스로 실제 마이그레이션 공수 가늠
3. **Avro 필요성 재확인** — 컨슈머 목록 훑어서 "실제 Avro 파일을 원하는지" vs "BQ 테이블 직접 쿼리로 충분한지" 확정. 후자면 EXPORT 단계 전체 삭제 가능
4. **SQL 방언 번역 도구 조사** — `sqlglot` 같은 라이브러리로 Presto→BigQuery 자동 변환 커버리지 측정. 100% 안 되겠지만 80% 자동화만 돼도 큰 절감