# 1. dbt materialization 정리

> Neptune SQL 변환의 dbt-BigQuery PoC (2026-06) 진행하며 정리한 materialization 별 동작·trade-off.
> 관련: [[애슬론/5_Neptune SQL 변환의 dbt-BigQuery 대체 검토]]

## 0. materialization 이란

dbt 모델 SQL 의 SELECT 결과를 **어떻게 물리화할지** 결정하는 config. `{{ config(materialized='...') }}` 로 지정. BigQuery 어댑터(dbt-bigquery) 가 각 타입을 어떤 BQ DDL 로 풀어내는지가 핵심.

---

## 1. 종류 5가지 (+ 1 BQ 전용)

### 1-1. `table`

- **동작**: 매 실행마다 `CREATE OR REPLACE TABLE` — 테이블 통째로 재생성
- **BQ 객체**: TABLE
- **장점**: 항상 fresh, full refresh
- **단점**: 데이터 큰 경우 비싸고 느림 (전체 재계산)
- **언제 쓰나**: 데이터 작거나 매번 전체 재생성이 의도된 마트 / 차원 테이블

```sql
{{ config(materialized='table') }}
SELECT ... FROM {{ ref('source') }}
```

### 1-2. `view`

- **동작**: `CREATE OR REPLACE VIEW` — 메타데이터만 저장. 쿼리 시점에 평가
- **BQ 객체**: VIEW
- **장점**: 스토리지 0, 항상 최신 source 반영
- **단점**: 매 쿼리마다 계산. 무거운 SELECT 면 다운스트림 모두 느려짐
- **언제 쓰나**: 가벼운 staging, source 약간 변환만 한 wrapper

```sql
{{ config(materialized='view') }}
SELECT ... FROM {{ source('raw', 'orders') }}
```

### 1-3. `incremental`

- **동작**: 처음엔 `table` 처럼 생성, 이후엔 `MERGE` / `INSERT` 로 신규/변경분만 처리
- **BQ 객체**: TABLE
- **dbt-bigquery 전략**:
  - `merge` (기본) — unique_key 기준 UPSERT
  - `insert_overwrite` — partition 단위 덮어쓰기 (Neptune 의 "한 파티션 재빌드" 와 매칭)
  - `append` — 단순 INSERT
- **언제 쓰나**: 대용량 사실 테이블, 시계열, 일별/시간별 ETL

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'create_date', 'data_type': 'date', 'granularity': 'day'},
    partitions=["DATE('" ~ var('run_date') ~ "')"]
) }}
SELECT
    DATE('{{ var("run_date") }}') AS create_date,
    ...
FROM {{ ref('source') }}
{% if is_incremental() %}
  WHERE event_ts >= TIMESTAMP('{{ var("run_date") }}')
{% endif %}
```

### 1-4. `ephemeral`

- **동작**: BQ 에 아무것도 안 만듦. **다운스트림 모델 SQL 안에 CTE 로 인라인**
- **BQ 객체**: 없음 (`__dbt__cte__<model>` 형태로 CTE 화)
- **장점**: 스토리지 0, BQ 객체 0, 중간 단계 표현용으로 깔끔
- **단점**:
  - 단독으로 `dbt run --select <ephemeral>` 호출 시 "할 일 없음" — Cosmos 에서 hang 유발한 사례 있음
  - 다운스트림이 여러 곳에서 ref 하면 매번 SQL 인라인 → BQ 쿼리 사이즈 비대화
- **언제 쓰나**: Neptune YAML 의 temp 테이블 패턴, 진짜 중간 산출물

```sql
{{ config(materialized='ephemeral') }}
SELECT ... FROM {{ source('raw') }}
```

### 1-5. `snapshot`

- **동작**: SCD Type-2 — 변경 이력 추적. `dbt_valid_from` / `dbt_valid_to` 컬럼 자동 추가
- **BQ 객체**: TABLE
- **언제 쓰나**: 차원 데이터의 변경 이력 보존 필요 (멤버십 등급 변경, 가격 변경 등)

```sql
{% snapshot product_snapshot %}
{{ config(
    target_schema='snapshots',
    unique_key='id',
    strategy='check',
    check_cols=['price', 'status']
) }}
SELECT * FROM {{ source('shop', 'products') }}
{% endsnapshot %}
```

### 1-6. `materialized_view` (BQ 전용)

- **동작**: BQ 의 native materialized view — BQ 가 source 변경 자동 감지해서 백그라운드 refresh
- **BQ 객체**: MATERIALIZED VIEW
- **장점**: SELECT 시점에 빠른 응답 + 자동 refresh
- **제약**: BQ 의 materialized view 제약 (지원 함수, JOIN 종류 등 제한 많음) 적용
- **언제 쓰나**: 자주 조회되는 집계, BQ 가 직접 관리해주길 원할 때

```sql
{{ config(materialized='materialized_view') }}
SELECT user_id, SUM(amount) AS total
FROM {{ source('events', 'purchases') }}
GROUP BY user_id
```

---

## 2. PoC 에서 얻은 실전 학습

### 2-1. Cosmos 의 Airflow task 생성 규칙

Cosmos 의 `DbtTaskGroup(select=...)` 가 만드는 Airflow task 의 기준은:

| selector 매칭 | materialization | Airflow task 생김? |
|---|---|---|
| O | table / view / incremental / snapshot | ✅ task 생성 + dbt 가 실제 BQ 객체 생성 |
| O | **ephemeral** | ⚠️ **task 는 생기지만 hang 가능** (`dbt run --select <ephemeral>` 가 dbtRunner 에서 멈춤) |
| X | (모두) | ❌ task 없음. 다만 다운스트림 모델이 ref 하면 컴파일 시 자동 포함 |

**핵심 시사점**: ephemeral 모델은 task 가 안 생기는 게 아니라 selector 에서 빼면 task 가 안 생기는 것. materialization 자체로 task 생성 여부를 제어하는 메커니즘은 없음.

→ ephemeral 의 의도(별도 task 없이 인라인) 를 살리려면 **selector 에서 ephemeral 을 빼야** 함:
```python
# ❌ tmp_* 들이 task 로 보임 (그리고 hang)
select=["tag:case3"]

# ✅ perm 만 task. tmp_* 는 CTE 로 인라인
select=["stat_first_open_detail_daily"]
```

### 2-2. 병렬 dbt task 의 메모리 압박

**증상**: 같은 Celery worker pod 에서 dbt task 5개 이상 동시 실행 → pod OOM 으로 한꺼번에 죽음 (다른 DAG 의 task 까지 같이 끌어내림)

**원인**:
- Cosmos 의 LOCAL 모드는 worker 안에서 dbt 를 Python 으로 invoke
- dbt 인스턴스 마다 manifest 파싱 + 어댑터 초기화 = 수백 MB
- 5개 동시 = GB 단위 메모리 점유
- Composer worker pod 메모리 한도(4 GB 정도) 초과

**해결**:
- 단일 task 케이스: dbtRunner 기본 모드로 충분 (Celery worker 1 task 정도는 OK)
- 병렬 task 케이스: `executor="KubernetesExecutor"` 로 task 마다 별도 pod
  - trade-off: task 당 pod 셋업 ~3분 추가

### 2-3. dbt 1.9 manifest 호환성

Composer 의 dbt 버전과 manifest 생성에 쓴 dbt 버전이 다르면 worker 가 manifest 못 읽고 hang.

- 사내 Composer: dbt 1.9.3
- 로컬 dev env: dbt 1.11.x

→ CI / 로컬에서 manifest 생성할 때 **Composer 와 같은 dbt 버전 사용** 필수.
→ `pip install 'dbt-core==1.9.*' 'dbt-bigquery==1.9.*'` 로 venv 분리.

### 2-4. partition 컬럼은 데이터에 포함시켜야 함

Neptune (Hive/Presto) 은 `ALTER TABLE ADD PARTITION` 으로 메타에만 partition 등록. SQL 본문엔 partition 컬럼 없어도 됨.

BQ 는 partition 컬럼이 **데이터의 일부**여야 함. SELECT 에 `DATE('{{ var("run_date") }}') AS create_date` 같이 명시 추가 필요.

```sql
SELECT
    DATE('{{ var("run_date") }}') AS create_date,   -- ← Neptune SQL 엔 없던 컬럼
    ...
FROM ...
```

### 2-5. AVRO 출력은 dbt 영역 밖

BQ 테이블은 내부 Capacitor 포맷만. Avro 로 떨굴 일 있으면 `post_hook` 에 `EXPORT DATA` 끼우거나 별도 Airflow task 로 분리.

```sql
{{ config(
    materialized='incremental',
    post_hook="""
      EXPORT DATA OPTIONS(uri='{{ var("avro_uri") }}', format='AVRO', overwrite=true, use_avro_logical_types=true)
      AS SELECT * EXCEPT(create_date) FROM {{ this }}
      WHERE create_date = DATE('{{ var("run_date") }}')
    """
) }}
```

### 2-6. AVRO 스키마 호환성 검증 결과

`kp_eventcash_remain_users_target_daily` (etl_id=1051) 의 Neptune 원본 Avro 와 BQ `EXPORT DATA` 산출물 스키마를 직접 비교했음.

**Neptune 원본** (`merge_and_move_dataframes.sh --output-format avro`):
```
record topLevelRecord {
  user_uid:    [long, null]
  at:          [long, null]
  expired_at:  [string, null]
  cost_number: [long, null]
  hh:          [string, null]
}
```

**BQ EXPORT DATA** (`format='AVRO', use_avro_logical_types=true`):
```
record Root {
  user_uid:    [null, long]  (default: null)
  at:          [null, long]  (default: null)
  expired_at:  [null, string]  (default: null)
  cost_number: [null, long]  (default: null)
  hh:          [null, string]  (default: null)
}
```

**비교 결과**:

| 항목 | 매칭 |
|---|:---:|
| 컬럼명 (5개) | ✅ |
| 컬럼 타입 (long/string) | ✅ |
| 컬럼 순서 | ✅ |
| nullable (union with null) | ✅ |
| record name (`topLevelRecord` vs `Root`) | ⚠️ |
| union 순서 ([T,null] vs [null,T]) | ⚠️ |
| default value 명시 | ⚠️ (BQ 만 명시) |

**결론**: 대부분 consumer 는 호환. 필드 by name 매칭하는 Hive/Spark/BigQuery 외부 테이블/Pandas 모두 문제 없음. 깨질 가능성 있는 시나리오는:
- Schema Registry compatibility = STRICT/FULL 인 환경
- record name 으로 deserialization class 매핑하는 Java/Kafka consumer
- byte-level diff 검증하는 골든 파일 테스트

PoC 의 최대 unknown 이었던 **Avro 호환성 실용적 합격**.

⚠️ **검증 범위 한계**: 이번 케이스의 컬럼은 `long` 4개 + `string` 1개 뿐. `use_avro_logical_types=true` 옵션이 적용되는 타입(`NUMERIC`, `BIGNUMERIC`, `TIMESTAMP`, `DATE`, `DATETIME`, `TIME`) 이 없어서 logical type 매핑은 실제로 확인되지 않았음. 운영 ETL 중 이 타입 쓰는 모델에 대해선 별도 검증 필요:
- BQ `TIMESTAMP` → Avro `long + timestamp-micros logical type`
- BQ `NUMERIC` → Avro `bytes + decimal logical type` (precision/scale)
- BQ `DATE` → Avro `int + date logical type`
이런 매핑이 Neptune 의 기존 Avro 와 일치하는지가 추가 검증 포인트.

---

## 3. Neptune 패턴 → dbt materialization 매핑

| Neptune 의 의미                          | dbt 매핑                                                         | PoC case                                     |
| ------------------------------------- | -------------------------------------------------------------- | -------------------------------------------- |
| PLAIN, no partition (매번 전체 재생성)       | `table`                                                        | case1 stat_partner_commerce_sku              |
| PLAIN, daily partition (매일 한 파티션 재빌드) | `incremental` + `insert_overwrite` + DAY partition             | case2 kp_stat_ticket_use_daily               |
| YAML, 다중 temp + perm                  | temp 들은 `ephemeral`, perm 은 `incremental`                      | case3 stat_first_open_detail_daily           |
| PLAIN, AVRO 출력                        | `incremental` + `post_hook` 에 `EXPORT DATA`                    | case4 kp_eventcash_remain_users_target_daily |
| PLAIN, hourly partition               | `incremental` + `insert_overwrite` + HOUR partition (DATETIME) | case5 snapshot_t_category                    |

---

## 4. 실전 권장사항

### 4-1. materialization 선택 가이드

```
데이터 작고 매번 재계산해도 OK? ─→ table
스토리지 없이 그때그때 쿼리? ──→ view
대용량 + 증분 처리? ────────→ incremental
중간 산출물, 다음 모델에만 쓰임? ─→ ephemeral (단, Cosmos selector 주의)
변경 이력 추적 필요? ──────→ snapshot
자주 조회되는 집계? ────────→ materialized_view (BQ 어댑터)
```

### 4-2. Cosmos selector 와 materialization 의 일관성

ephemeral 모델은 **selector 에서 제외**하는 게 자연스러움. dbt 의 ephemeral 의미("별도 step 없이 인라인") 와 Airflow task 생성을 분리해서 생각해야 함:

- 모델 정의: `materialized='ephemeral'`
- DAG: `select=["<perm_model_name>"]` 또는 tag 가 ephemeral 안 잡게 분리

### 4-3. 운영 가시성 vs 효율의 trade-off

| 우선 | 선택 |
|---|---|
| 단순/효율 | ephemeral + perm 만 task → 1 task, 빠름, OOM 없음 |
| temp 가시성 | view + tag selector + K8s executor → 7 task, 21+ 분, 안정 |

PoC 단계에선 효율, 운영 안정화 후 가시성으로 이동하는 점진적 접근 권장.

---

## 5. 미해결/추가 검증 필요

- snapshot materialization 의 BQ 비용 패턴 (변경 이력 누적 시 스토리지)
- materialized_view 가 Neptune 의 어떤 ETL 패턴을 대체할 수 있는지
