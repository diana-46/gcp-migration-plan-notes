# 8. insert_overwrite 매커니즘 (dbt-bigquery)

> dbt-bigquery 의 `incremental_strategy='insert_overwrite'` 가 내부적으로 어떤 BQ SQL 을
> 발행하는지, 왜 그렇게 하는지, 실패 시 어떤 흔적이 남는지, 언제 이 전략을 선택할지.
> 관련: [[1_materialization]], [[2_schema 관리]], [[7_테이블 아웃풋]]

## 0. 왜 이 매커니즘을 이해해야 하나

`insert_overwrite` 는 이름만 보면 "그 파티션 지우고 새로 넣는다" 처럼 심플해 보이지만
실제로는 **tmp 테이블 생성 → MERGE → tmp 삭제** 라는 다단계 시퀀스로 동작한다.
- 실패 시 tmp 가 leftover 로 남음
- 스키마 drift 있으면 MERGE 에서 실패
- 동시 실행 시 tmp 이름 충돌 가능성

내부 흐름을 알고 있어야 leftover 진단, 스키마 변경 배포, 백필 병렬 안전 설계가 가능.

Story 팀 이관 케이스: `bizberry_community_contents_artistpost` 를 hourly partition +
insert_overwrite 로 실험하다 이 매커니즘 관련 이슈 2회 반복 발생 (schema drift,
leftover tmp). 이 문서는 그 경험을 기반으로 정리.

---

## 1. 실행 시퀀스 (dbt-bigquery 소스 기준)

`bq_generate_incremental_insert_overwrite_build_sql` (`dbt-adapters/dbt-bigquery/src/dbt/include/bigquery/macros/materializations/incremental_strategy/insert_overwrite.sql`) 이 만드는 SQL.

### 1-1. Dynamic 모드 (기본)

`partitions` 를 명시 안 하고 target 이 이미 존재할 때. 매 run 마다 다음 시퀀스 실행:

```sql
-- Step 1. tmp 테이블 생성 (모델 SELECT 결과 저장)
CREATE OR REPLACE TABLE `proj.ds.target__dbt_tmp{timestamp_hash}`
PARTITION BY <partition_expr>
CLUSTER BY <cluster_cols>
OPTIONS (expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR))
AS (
    SELECT ... FROM ...   -- 모델의 컴파일된 SELECT
);

-- Step 2. 실행 대상 파티션 값들을 tmp 에서 추출 (스크립트 변수로 담음)
DECLARE dbt_partitions_for_replacement ARRAY<...>;
SET dbt_partitions_for_replacement = (
    SELECT AS ARRAY_AGG(DISTINCT <partition_expr>)
    FROM `proj.ds.target__dbt_tmp{hash}`
);

-- Step 3. MERGE 로 원자적 replace
MERGE INTO `proj.ds.target` AS DBT_INTERNAL_DEST
USING `proj.ds.target__dbt_tmp{hash}` AS DBT_INTERNAL_SOURCE
ON FALSE   -- ← 절대 매치 안 함 (row-level merge 아님)
WHEN NOT MATCHED BY SOURCE
     AND <partition_expr> IN UNNEST(dbt_partitions_for_replacement)
     THEN DELETE
WHEN NOT MATCHED BY TARGET THEN INSERT (col1, col2, ...) VALUES (col1, col2, ...);

-- Step 4. tmp 삭제 (성공 시)
DROP TABLE IF EXISTS `proj.ds.target__dbt_tmp{hash}`;
```

**핵심 관찰**:
- `ON FALSE` → row-level 매칭 없음. unique_key 필요 없음.
- `WHEN NOT MATCHED BY SOURCE ... DELETE` → target 의 해당 파티션 rows 를 지움
- `WHEN NOT MATCHED BY TARGET ... INSERT` → tmp 의 rows 를 INSERT
- 두 개가 한 MERGE 안 → **원자적** (all-or-nothing)

### 1-2. Static 모드

`partitions=[...]` 를 config 에 명시하면. 예:

```python
{{ config(
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'create_date', 'data_type': 'date'},
    partitions=["DATE('" ~ var('run_date') ~ "')"]
) }}
```

Step 2 (파티션 값 추출) 이 생략되고 config 의 partitions 리스트를 그대로 사용.

### 1-3. 첫 run (target 없음)

Incremental materialization 은 target 이 존재하지 않으면 **CTAS path** 로 분기.
`bq_create_table_as` 매크로가 `CREATE OR REPLACE TABLE target AS SELECT ...` 한 번으로 끝.
tmp 안 만듦.

즉 tmp + MERGE 시퀀스는 **2번째 run 부터**만 발생.

---

## 2. 왜 MERGE 를 쓰나 (pure DELETE + INSERT 안 되나)

BQ 는 multi-statement transaction 을 지원하지만, dbt 는 어댑터 간 일관성을 위해
**한 statement 안의 원자성** 을 선호. MERGE 는 한 문 안에서 DELETE + INSERT 를 원자적으로
실행하는 표준 SQL 도구.

Pure DELETE + INSERT 로 하면:
```sql
BEGIN TRANSACTION;
DELETE FROM target WHERE <partition_predicate>;
INSERT INTO target SELECT * FROM tmp;
COMMIT;
```
DELETE 성공, INSERT 실패 시 트랜잭션 롤백. 원자성은 확보됨. 그런데:
- BQ 트랜잭션은 script mode 에서만 (client 마다 지원 다름)
- MERGE 는 어디서든 동일 동작 보장
- MERGE 는 최적화 (파티션 pruning, 병렬 등) 도 더 매끄러움

그래서 dbt-bigquery 는 `ON FALSE` 관용구로 "row-level 매칭 없이 block-level replace" 를 표현.

---

## 3. tmp 테이블 lifecycle

### 3-1. Expiration 안전망

dbt-bigquery 는 tmp 생성 시 **`expiration_timestamp = now + 12h`** 를 항상 세팅.
```sql
OPTIONS (expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR))
```
정상 flow 에선 Step 4 의 DROP 이 곧바로 청소. 실패 flow (MERGE 에러) 로 DROP 못 가면
12h 뒤 BQ 가 자동 삭제. **누적되지 않음**.

### 3-2. 실패 시 leftover 확인

```bash
bq ls --max_results=100 <project>:<dataset> | grep __dbt_tmp
```

이름 패턴: `{target_name}__dbt_tmp{timestamp_hash}` — hash 는 dbt 가 세팅한 실행 시각
기반 (ms 단위 정도). 여러 실패면 여러 개 쌓임.

Manual cleanup (자동 소멸 기다리기 싫으면):
```bash
bq rm -f -t '<project>:<dataset>.<name>__dbt_tmp<hash>'
```

### 3-3. Story 팀 실제 경험 (2026-07-10)

`bizberry_community_contents_artistpost` 를 daily → hourly partition + event_ts 로 재설계
후 첫 hourly run 에서 MERGE 실패:

```
Query error: Name event_ts not found inside DBT_INTERNAL_DEST at [208:48]
```

원인: target 이 옛 스키마 (22 컬럼, no event_ts) 인 상태에서 새 모델의 MERGE 가
`event_ts` 를 target 필드로 참조 → not found.

Leftover:
```
bizberry_community_contents_artistpost_integration__dbt_tmp085310256888
```
Step 1 은 성공 (tmp 는 새 스키마 23 컬럼으로 생성). Step 3 MERGE 에서 실패해 tmp 남음.
12h 뒤 자동 소멸 예정이었으나 즉시 정리.

---

## 4. 스키마 drift 시나리오

dbt-bigquery incremental materialization 의 기본 `on_schema_change='ignore'` 상태에서
컬럼 추가/삭제/파티션 변경이 target 과 model 사이에 벌어지면:

### 4-1. 컬럼 추가

Model 이 새 컬럼 넣었는데 target 은 옛 스키마.
- Step 1 tmp: 새 스키마로 생성 OK
- Step 3 MERGE: `WHEN NOT MATCHED BY TARGET THEN INSERT (new_col, ...)` 에서 target 에
  new_col 없음 → 에러

**해결**: `on_schema_change='append_new_columns'` 로 자동 ALTER ADD COLUMN, 또는
`pre_hook="ALTER TABLE {{ this }} ADD COLUMN IF NOT EXISTS ..."` 명시.

### 4-2. 컬럼 삭제

- Step 1 tmp: 컬럼 제거된 스키마
- Step 3 MERGE: `WHEN NOT MATCHED BY SOURCE ... DELETE` 는 문제 없음, `INSERT` 는 target 컬럼
  일부를 안 채우면 NULL 로 입력. 다만 이후 컬럼이 target 에 계속 남아 stale 데이터로 오해될 수 있음.

**해결**: `on_schema_change='sync_all_columns'` (자동 ADD/DROP) 또는 명시적 pre_hook DROP.

### 4-3. 파티션 스키마 변경 (오늘 우리 케이스)

`partition_by` 자체가 바뀌면 (`create_date` DAY → `event_ts` HOUR) MERGE 예측이 완전히
어긋남. Partition column 자체가 target 에 없거나 이름이 달라짐.

**해결**: `on_schema_change` 옵션으로 자동화 불가. 반드시:
- `dbt run --full-refresh` (target DROP + CTAS) 또는
- 수동 `DROP TABLE` + 다음 run

---

## 5. 백필 병렬 안전

Static 모드로 여러 백필 dag_run 을 병렬로 돌리면 tmp 이름은 timestamp hash 로 분리되므로
tmp 자체는 안전. 하지만 target 에 대한 MERGE 는 동일 target 에 대한 다중 write —
BQ 스케줄러가 자동 큐잉 (BQ 는 같은 테이블 concurrent DML 을 순차 처리).

- 백필 4개 dag_run 병렬 → 4개 tmp + 4번의 MERGE 순차
- 각 MERGE 는 자기 partition 만 replace → 서로 안 부딪힘
- 다만 총 실행 시간 = MERGE 순차 소요 시간

**Note 7 § 5-6 의 alias run-scoped 패턴은 여기 안 필요**. 그건 temp 모델 (독립 물리 테이블)
용도지, insert_overwrite 의 dbt-internal tmp 와는 별개.

---

## 6. 다른 전략과의 매커니즘 비교

| 전략 | 실행 SQL | tmp 사용 | 원자성 | row-level 매칭 |
|---|---|---|---|---|
| `merge` | `MERGE INTO target USING (SELECT) ON <unique_key>` | X (subquery) | O | O |
| `insert_overwrite` | tmp 생성 → MERGE ON FALSE | O | O | X |
| `microbatch` | insert_overwrite + time-window predicate | O | O | X |
| `insert_only` (custom) | tmp OR sql 인라인 → INSERT | Optional | X | X |

**핵심 차이**:
- `merge` 는 subquery 를 USING 절에 직접 넣어 tmp 불필요.
- `insert_overwrite` / `microbatch` 는 partition 계산 위해 tmp materialize 필요.
- `insert_only` (커스텀) 은 pre_hook DELETE 로 partition 비운 뒤 INSERT.

---

## 7. 언제 insert_overwrite 를 선택하나

### 7-1. 자연 fit

- partition 단위로 idempotent 재실행 필요
- unique_key 지정하기 부담스러울 만큼 grain 이 넓음
- Neptune 스타일 "이번 파티션 통째로 갈아치우기" 세만틱

예: hourly snapshot → hour partition + hourly 실행 → 매 run 이 자기 hour partition replace.

### 7-2. Non-fit

- **hourly 실행 + daily partition**: 같은 날 다른 hour block 을 보존해야 하는데
  insert_overwrite 는 day partition 통째로 replace → 다른 hour rows 유실.
  → `insert_only` (pre_hook DELETE + INSERT) 로 hour-block replace 하는 게 답.

- **row-level upsert 필요**: 특정 row 만 갱신하고 나머지는 보존.
  → `merge` + unique_key.

- **1년치 스냅샷 매 hour 재계산 (Neptune 유형)**: partition 단위 replace 가 아니라
  hour-block replace 라 hourly partition 필요. BQ 10k partition 상한과 tension.

---

## 8. 요약

- `insert_overwrite` = **tmp 생성 → ON FALSE MERGE → tmp 삭제** 시퀀스
- 실패 시 tmp leftover 는 12h 뒤 자동 소멸 (BQ expiration)
- 스키마 drift 는 `on_schema_change` 옵션으로 일부 방어, 파티션 재설계는 무조건
  full-refresh 또는 수동 DROP
- 첫 run 은 CTAS path (tmp 없음), 2회차부터 위 시퀀스
- 병렬 백필은 tmp 이름 timestamp hash 로 안전, 하지만 target 은 BQ 가 순차 처리
- **fit 판단**: partition 단위 replace 가 자연스러운가? hour-in-day 같이 partition
  granularity 와 실행 주기 mismatch 있으면 `insert_only` 등 다른 전략 고려.
