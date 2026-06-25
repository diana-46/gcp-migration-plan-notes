# 2. schema 관리

> Neptune 의 자동 스키마 동기화 (Hive 외부 테이블) 와 dbt 의 schema 관리 (schema.yml + on_schema_change + contract) 를 1:1 비교.
> 관련: [[1_materialization]], [[애슬론/5_Neptune SQL 변환의 dbt-BigQuery 대체 검토]]

## 0. 왜 비교가 필요한가

Neptune 은 사용자가 SQL 만 정의하면 Hive External 테이블의 DDL 을 **자동으로 만들어주고 유지보수**해줘요. 그래서 사용자는 컬럼이 늘었거나 줄어든 걸 신경 안 써도 ETL 이 다음 run 에 동기화함. dbt + BQ 로 가면 이 자동화 메커니즘이 그대로 옮겨오는 게 아니라 **다른 모델**로 대체됨. 그 다름을 정확히 알아야 운영 가능.

---

## 1. Neptune 의 스키마 관리 (현재 동작)

### 1-1. 트리거와 source of truth

- **트리거**: ETL **생성/수정 시점**에 `TableSynchronizer.sync()` 호출 (`TableSynchronizer.kt:67-127`). 매 run 마다가 아니라 **메타 변경 시점**에만 한 번
- **source of truth**: 사용자 SQL 의 SELECT 결과 컬럼. Presto JDBC metadata 로 결과 컬럼/타입 추출 (`EtlContext.sqlForMetaData`, `EtlContext.kt:41-42`)
- **drift 감지**: 현 Hive 테이블 메타 vs SQL 결과 메타 비교 (`TableMeta.kt:8-24`)
  - `ischangedColumnsFrom()` — 겹치는 컬럼의 타입 차이
  - `addColumnsFrom()` — 새로 생긴 컬럼
  - `dropColumnsFrom()` — 사라진 컬럼

### 1-2. 자동 발행 DDL

| 시나리오                      | Neptune 동작                                                                                              |
| ------------------------- | ------------------------------------------------------------------------------------------------------- |
| 첫 ETL 생성                  | `CREATE TABLE ... WITH (format='PARQUET', external_location='hdfs://...')` 발행 (`PrestoClient.kt:76-91`) |
| 컬럼 추가                     | `ALTER TABLE ADD COLUMN <name> <type>` 자동 (`BaseClient.kt:13-15`)                                       |
| 컬럼 삭제                     | `ALTER TABLE DROP COLUMN <name>` 자동. **HDFS 데이터는 보존**, 메타만 제거 (`BaseClient.kt:17-19`)                   |
| 컬럼 타입 변경                  | ❌ **`ColumnTypeChangedException` 던지고 sync 중단** (`TableSynchronizer.kt:44-48`)                           |
| 컬럼 이름 변경                  | (감지 불가 — 다른 이름은 add + drop 으로 처리됨)                                                                      |
| 파티션 키                     | 데이터 컬럼과 분리 처리. 외부 테이블 DDL 에 포함되되 diff 비교에선 제외 (`PrestoClient.kt:61`)                                    |
| 출력 포맷 변경 (PARQUET ↔ AVRO) | ❌ 불가. 테이블 drop & recreate 수동                                                                            |


### 1-3. 안 하는 것 (Neptune 한계)

- 동시성 락 없음 — 같은 테이블 ETL 두 개 동시 수정 시 race 가능
- 스키마 감사 로그/이력 없음 — 변경 후엔 이전 스키마 흔적 없음
- 데이터 타입 사전 검증 없음 — SQL 출력의 타입은 JDBC metadata 로만 inference
- 행수/통계 미수집
- 다운스트림 호환성 체크 없음 — drop 된 컬럼을 누가 쓰는지 모름

---

## 2. dbt 의 스키마 관리 (대응 메커니즘)

dbt 는 스키마 관리를 **세 가지 직교 메커니즘**으로 풀어요. 각각 다른 시점/책임:

### 2-1. `schema.yml` — 선언과 문서화

```yaml
version: 2
models:
  - name: orders
    description: "주문 팩트"
    columns:
      - name: order_id
        data_type: int64
        description: "주문 PK"
        constraints:
          - type: not_null
        data_tests: [unique, not_null]
```

**역할**: 모델의 "기대 스키마" 선언 + 문서. **자동 강제 아님** (contract enforced=true 일 때만 강제). 주로:
- 컬럼 description → BQ INFORMATION_SCHEMA / dbt docs / DataHub 반영
- 테스트 정의 → `dbt test` 시 SQL 로 자동 변환되어 실행
- 메타데이터 (owner, tags, policy_tags 등) 외부 시스템 연동

### 2-2. `on_schema_change` — incremental 모델의 자동 동기화

incremental materialization 의 모델에서 SELECT 결과 컬럼이 기존 BQ 테이블 컬럼과 다를 때 어떻게 할지:

```sql
{{ config(
    materialized='incremental',
    on_schema_change='append_new_columns'
) }}
```

| 옵션                   | 동작                      | Neptune 대비                      |
| -------------------- | ----------------------- | ------------------------------- |
| `ignore` (기본)        | drift 무시. 새 컬럼은 결과에서 빠짐 | 가장 보수적, Neptune 보다 약함           |
| `fail`               | drift 감지 시 에러           | 안전, 운영자 개입 강제                   |
| `append_new_columns` | 새 컬럼만 ADD. 기존 컬럼은 유지    | Neptune 의 ADD COLUMN 자동화와 거의 동일 |
| `sync_all_columns`   | 새 컬럼 ADD + 사라진 컬럼 DROP  | Neptune 의 ADD+DROP 자동화와 동일      |

→ **`sync_all_columns` 가 Neptune 의 sync() 와 가장 가까운 동작**.

### 2-3. `contract` — 빌드 시 강제

```yaml
models:
  - name: dim_user
    config:
      contract: {enforced: true}
    columns:
      - name: user_id
        data_type: int64
        constraints: [{type: not_null}]
```

`enforced: true` 면:
- dbt 가 빌드 전에 SQL 결과의 컬럼 이름/타입 을 schema.yml 과 비교
- 불일치 → 에러로 실패 (테이블 swap 안 함)
- BQ 의 PRIMARY KEY / NOT NULL constraints 도 반영 (NOT NULL 만 실제 강제됨, PK 는 informational)

→ Neptune 의 `ColumnTypeChangedException` (타입 변경 거부) 와 같은 결의 보호.

---

## 2-3-1. 스키마 체크 끄기

운영 초기 / 사고 복구 / 의도적 느슨 운영 시 두 체크를 끌 수 있음.

### 두 체크의 역할

| 체크 | 비교 대상 | 비활성화 값 |
|---|---|---|
| `contract` | SQL output ↔ schema.yml | `{'enforced': false}` |
| `on_schema_change` | SQL output ↔ BQ 실제 테이블 | `'ignore'` (default) |

### 모델 단위로 끄기

```sql
{{ config(
    contract={'enforced': false},      -- 또는 줄 자체 제거
    on_schema_change='ignore',         -- 또는 줄 자체 제거 (default 가 ignore)
) }}
```

### 프로젝트 전역 default

`dbt_project.yml`:
```yaml
models:
  dbt_test:
    +contract:
      enforced: false
    +on_schema_change: ignore
```

→ 그 다음 개별 모델에서 `+enforced: true` 로 일부만 켤 수도 있음.

### 조건부 (vars 로 토글)

```sql
{{ config(
    contract={'enforced': var('strict_mode', false)},
    on_schema_change=('fail' if var('strict_mode', false) else 'ignore'),
) }}
```

→ `dbt run --vars '{strict_mode: true}'` 일 때만 strict, 평소엔 느슨.

### 끄면 일어나는 것

| 항목 | strict (기본 권장) | loose (끄면) |
|---|---|---|
| schema.yml 빠뜨림 | dbt run fail | 조용히 무시. BQ 에 잘못된 schema 생성 가능 |
| BQ schema drift | dbt run fail | default `ignore` — 종종 데이터 손실 |
| dbt run 속도 | introspection 쿼리 1개 추가 (~수백 ms) | 더 빠름 (체크 안 함) |
| 운영 사고 가능성 | 낮음 | 높음 (drift 가 silently 진행) |

### 끌 때의 권장 시나리오

- **개발 초기 / 프로토타이핑**: 빠른 iteration 우선. 운영 들어가면 다시 켜기
- **read-only 모델**: source 가 신뢰 가능하고 schema 변경 영향 없을 때 (드물긴 함)
- **emergency**: 사고 복구 중 임시로 끄고 처리 후 다시 켜기

운영 권장은 **켜둔 채로**. introspection 쿼리는 `WHERE 1=0` 이라 데이터 스캔 0 bytes, 비용 거의 0. 그 대가로 silent drift 사고 막아주는 가치가 압도적.

---

## 2-4. 스키마 변경이 BQ/Hive 에 반영되는 **타이밍**

운영자가 가장 헷갈리는 부분. 시나리오: ETL 의 SQL 을 4 컬럼 → 5 컬럼으로 바꿨을 때 언제 실제 테이블에 적용되나.

### Neptune — **UI 저장 시점에 즉시 sync**

`tableSynchronizer.sync()` 가 호출되는 곳은 `EtlService.kt` 의 두 군데뿐:
- `saveEtlActionsAndDependencies` (생성) — 492
- `updateEtlActionsAndDependencies` (수정) — 504

→ GraphQL `updateEtl` mutation 응답이 돌아오기 **전에** ALTER TABLE 이 이미 실행됨. 사용자가 "저장" 누른 순간 Hive 테이블에 5번 컬럼이 즉시 생김.

```
T0: ETL 저장 (4컬럼 → 5컬럼)
    └─ Athlon API 안에서:
       ├─ validate
       ├─ Airflow DAG action 재생성
       ├─ ALTER TABLE ADD COLUMN col5 <type>   ◄── 여기서 즉시
       └─ API 응답 200
T0+ε: Hive 테이블 schema = (1,2,3,4,5). 기존 row 의 col5 = NULL
T1 (다음 새벽 0시): Airflow DAG run → 새 파티션 들어옴 (col5 값 채워짐)
이후: 옛 파티션의 col5 는 영원히 NULL
```

**메타와 데이터가 분리**됨. 스키마는 미리 변경되고, 데이터는 다음 스케줄 run 부터 채워짐.

### dbt — **다음 `dbt run` 시점에 동시 적용**

dbt 는 메타 저장 개념이 없음. 모델 SQL = 코드. git commit 으로 "저장" 하지만 **그건 BQ 에 영향 0**.

```
T0: 모델 SQL 수정 (col5 추가), git commit
    └─ BQ 영향 0
T0+α: PR 머지, Composer 에 sync (manifest.json 업데이트)
    └─ BQ 영향 0 (manifest 만 바뀜)
T1 (다음 dbt run):
    ├─ dbt 가 SELECT 결과 vs BQ 테이블 컬럼 비교
    ├─ on_schema_change 에 따라:
    │  ├─ append_new_columns → ALTER ADD COLUMN col5
    │  ├─ ignore → col5 무시하고 INSERT
    │  └─ fail → 에러로 중단
    └─ 데이터 INSERT/MERGE 까지 한 번에
T1+ε: 스키마 + 데이터 동시 반영
```

**메타와 데이터가 결합**됨. 같은 task 안에서 스키마 변경 + 데이터 처리.

### 운영 차이 요약

| 상황                   | Neptune                           | dbt                                         |
| -------------------- | --------------------------------- | ------------------------------------------- |
| 저장/머지 직후 컬럼 추가 확인    | 즉시 BQ/Hive 에 보임                   | 다음 run 까지 안 보임                              |
| 스키마 변경 실패 시          | 저장 자체가 fail → 옛 SQL 유지            | git 에 코드 머지된 상태로 run 만 fail → 코드와 BQ 불일치 잔존 |
| 다운스트림 알람 시점          | "ETL 저장됨" 직후 영향                   | "dbt run 성공" 후 (데이터까지 완성된 후)                |
| 백필로 옛 파티션 재계산 후 col5 | NULL (Neptune sync 가 옛 파티션 안 건드림) | NULL 또는 백필 SQL 의 결과 (재계산 시 col5 도 채움)       |

**dbt 가 더 안전한 측면**: 코드 = 단일 출처(single source of truth). run 이 코드를 BQ 에 반영하는 유일한 경로. Neptune 의 "메타 저장 즉시 sync" 는 빠르긴 한데 메타-데이터 불일치 상태가 자연스럽게 발생.


---

## 3. 컬럼 변경 시나리오별 대응 매핑

운영에서 실제로 발생하는 5가지 케이스:

### 3-1. 컬럼 추가 (new optional column)

| 시스템     | 어떻게                                                                       |
| ------- | ------------------------------------------------------------------------- |
| Neptune | sync 시점에 자동 ADD COLUMN. NULL 채워짐                                          |
| dbt     | `on_schema_change='append_new_columns'` 또는 `'sync_all_columns'`. NULL 채워짐 |

→ **동등 매칭**. dbt 가 자동화 정도 동일.

### 3-2. 컬럼 삭제

| 시스템 | 어떻게 |
|---|---|
| Neptune | sync 시점에 자동 DROP COLUMN. HDFS 데이터 보존 (메타만 제거) |
| dbt | `on_schema_change='sync_all_columns'` 면 자동 DROP. **BQ 의 DROP COLUMN 은 데이터도 제거** ⚠️ |

→ **데이터 보존 측면에서 다름**. BQ 에서 컬럼 drop 은 실제 컬럼나 스토리지 해제. 복구 안 됨. 보수적으로 가려면 `on_schema_change='fail'` 로 두고 운영자가 명시적 결정.

### 3-3. 컬럼 타입 변경

| 시스템     | 어떻게                                                                                                                |
| ------- | ------------------------------------------------------------------------------------------------------------------ |
| Neptune | ❌ 거부 (`ColumnTypeChangedException`). 운영자가 수동으로 처리해야                                                                |
| dbt     | `on_schema_change` 옵션에 type change 자체 처리 없음. **`contract: enforced=true`** 로 거부하거나, **`--full-refresh`** 로 테이블 재생성 |

→ **둘 다 보수적**. dbt 는 강제 아니지만 contract 로 같은 효과 가능. 진짜 타입 바꿀 땐 `dbt run --full-refresh` 가 표준 (전체 재생성).

### 3-4. 컬럼 이름 변경

| 시스템     | 어떻게                                            |
| ------- | ---------------------------------------------- |
| Neptune | 감지 불가 (이름 다르면 add + drop 으로 처리됨 → 데이터 손실 가능)   |
| dbt     | 동일하게 add + drop. 의도적 rename 이면 별도 마이그레이션 모델 필요 |

→ **둘 다 위험**. BQ 의 `ALTER COLUMN RENAME` 을 활용하려면 dbt 의 [pre_hook 또는 macro](https://docs.getdbt.com/reference/resource-configs/pre-hook-post-hook) 로 직접 발행:
```sql
{{ config(
    pre_hook="ALTER TABLE {{ this }} RENAME COLUMN old_name TO new_name"
) }}
```
점진적 마이그레이션 패턴 (dual write → cutover → drop old) 권장.

### 3-5. 파티션 키 변경

| 시스템     | 어떻게                                               |
| ------- | ------------------------------------------------- |
| Neptune | 변경 시 External 테이블 drop & recreate 필요              |
| dbt     | `partition_by` 변경 → `--full-refresh` 만 가능. 전체 재생성 |

→ 둘 다 비싸다. 운영 ETL 의 파티션 키는 사실상 immutable 로 취급.

---

## 4. Neptune 이 못 하던 것 → dbt 로는?

| Neptune 한계      | dbt 대응                                                                                                                                                               |
| --------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 데이터 타입 사전 검증 없음 | `contract: enforced=true` 로 빌드 전 검증                                                                                                                                  |
| 다운스트림 호환성 체크 없음 | dbt 의 `ref()` 그래프 + `dbt run --select +<model>` 로 영향 모델 추적 가능                                                                                                        |
| 스키마 감사 로그 없음    | git 히스토리 + manifest.json 변천사가 자동 감사 (dbt project 자체가 versioned)                                                                                                      |
| 통계/행수 미수집       | `dbt-bigquery` 의 [`generate_columns_macros`](https://docs.getdbt.com/reference/resource-properties/columns) + `dbt source freshness` 로 일부 가능. BQ 의 native stats 도 활용 |
| 동시성 락 없음        | dbt 의 모델 단위 실행은 격리됨 (한 모델 = 한 BQ job). 같은 모델 동시 실행은 dbt-cloud / Airflow scheduler 가 관리                                                                               |
| 스키마 버저닝 없음      | [model versions](https://docs.getdbt.com/docs/collaborate/govern/model-versions) (1.5+) — `orders_v1`, `orders_v2` 동시 존재 가능                                          |

---

## 5. Neptune 이 하던 것 → dbt 로는?

| Neptune 자동 동작          | dbt 매핑                                          | 등가성                         |
| ---------------------- | ----------------------------------------------- | --------------------------- |
| 첫 run 시 CREATE TABLE   | `materialized='incremental'` / `'table'` 의 첫 실행 | ✅ 동등                        |
| 컬럼 추가 자동 sync          | `on_schema_change='append_new_columns'`         | ✅ 동등                        |
| 컬럼 삭제 자동 sync          | `on_schema_change='sync_all_columns'`           | ⚠️ 데이터 보존 차이                |
| 타입 변경 거부               | `contract: enforced=true`                       | ✅ 동등                        |
| Stack rollback         | (없음) — git revert + `--full-refresh` 가 대안       | ⚠️ 메커니즘 다름. ACID 더 약하지만 명시적 |
| 파티션 추가 (ADD PARTITION) | `insert_overwrite` 가 자동 처리                      | ✅ 동등 (실제론 dbt 가 더 깔끔)       |

---

## 6. 운영 권장 패턴

### 6-1. 기본 설정 (권장 — § 8 에서 검증된 패턴)

```yaml
# schema.yml
models:
  - name: orders
    config:
      contract: {enforced: true}     # SQL output 과 schema.yml 일치 강제
      on_schema_change: fail          # BQ drift 시 사람 개입
    columns:
      - name: order_id
        data_type: int64
        data_tests: [unique, not_null]
```

→ schema.yml = single source of truth. Neptune 의 "자동 sync" 보다 명시적이고 안전. 컬럼 변경 시 SQL + schema.yml + BQ (pre_hook 또는 `--full-refresh`) 세 곳 동기화 필요.

> 더 느슨한 자동화 원하면 `contract.enforced=false` + `on_schema_change='append_new_columns'` 도 가능. 단 § 7-5 의 한계 (재실행 파티션 NULL 덮어쓰기) 있음.


### 6-2. 컬럼 추가 워크플로우

```
1. 모델 SQL 에 새 컬럼 추가
2. schema.yml 에 컬럼 description / test 추가
3. dbt run — on_schema_change 가 BQ 에 ALTER ADD COLUMN 발행
4. dbt test — 새 컬럼 정합성 검증
5. git commit (이력 보존)
```

### 6-3. 컬럼 삭제 워크플로우

```
1. 모델 SQL 에서 컬럼 제거
2. schema.yml 에서 컬럼 항목 제거
3. (선택) on_schema_change='fail' 로 두고 일시적으로 sync_all_columns 로 바꿔서 dbt run
4. 또는 명시적 ALTER 를 pre_hook 으로:
   pre_hook="ALTER TABLE {{ this }} DROP COLUMN old_col"
5. git commit
```

⚠️ **dependents 먼저 확인**: `dbt ls --select +<my_model>` 으로 영향받는 다운스트림 모델 목록 확인 후 진행.

### 6-4. 타입 변경 워크플로우

```
1. 영향 분석 (다운스트림 확인)
2. 새 컬럼 추가 (예: old_col → old_col_v2) → 점진 dual write
3. 다운스트림 모두 v2 로 마이그레이션
4. old_col 제거
```

→ Neptune 에선 어차피 거부됐던 거라 수동 처리. dbt 도 동일 패턴이 안전.

---

## 7. 실측 — case2 에 컬럼 추가 (PoC 2026-06-18)

`case2_kp_stat_ticket_use_daily` (incremental + DAY partition) 에 `revenue INTEGER` 컬럼 추가 → Cloud Composer 에서 dbt run → BQ 동작 확인.

### 7-1. 설정 변경

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    on_schema_change='append_new_columns',   ◄── 추가
    partition_by={...},
    partitions=[...]
) }}

SELECT
    ...,
    'ko' AS series_language,
    5000 AS revenue   ◄── 새 컬럼
```

### 7-2. dbt 가 발행한 BQ job 시퀀스

| 순서  | 명령                                                              | 의미                       |
| --- | --------------------------------------------------------------- | ------------------------ |
| 1   | `CREATE OR REPLACE TABLE case2_..._dbt_tmp`                     | 새 스키마 임시 테이블             |
| 2   | **`ALTER TABLE case2_kp_stat_ticket_use_daily ADD COLUMN ...`** | 실제 테이블 스키마 변경            |
| 3   | `MERGE INTO case2_kp_stat_ticket_use_daily`                     | insert_overwrite 로 새 파티션 |
| 4   | `DROP TABLE case2_..._dbt_tmp`                                  | 임시 정리                    |

→ **dbt 가 자동으로 ALTER 발행**. 사용자가 직접 DDL 칠 필요 없음. on_schema_change 가 의도대로 동작.

### 7-3. 옛 파티션의 새 컬럼 값

```
+-------------+-----------+------------------+---------+
| create_date | series_id | ticket_use_count | revenue |
+-------------+-----------+------------------+---------+
|  2026-06-15 |      1001 |               42 |    NULL |   ← 옛 파티션, BQ 가 자동 NULL
|  2026-06-17 |      1001 |               42 |    NULL |   ← 옛 파티션
|  2026-06-18 |      1001 |               42 |    5000 |   ← 이번 run 새 값
+-------------+-----------+------------------+---------+
```

→ ALTER TABLE ADD COLUMN 시 **BQ 가 기존 row 의 새 컬럼을 자동으로 NULL 로 채움**. 에러 없이 쿼리 가능.


### 7-4. Neptune 과의 비교

|                            | Neptune                            | dbt + BQ (이번 검증)                                                                                |
| -------------------------- | ---------------------------------- | ----------------------------------------------------------------------------------------------- |
| ALTER 시점                   | ETL **저장 시점** (사용자가 "저장" 누른 순간)    | **다음 dbt run** (코드 머지 후)                                                                        |
| ALTER 발행자                  | `TableSynchronizer.sync()` 가 동기 호출 | dbt run 의 incremental materialization 매크로                                                       |
| 옛 파티션 새 컬럼 값               | NULL (HDFS 데이터 안 건드림)              | NULL (BQ 메타만 변경)                                                                                |
| 옛 파티션 backfill 시 새 컬럼 채워지나 | ❌ Neptune sync 는 옛 파티션 데이터 안 만짐    | ✅ `dbt run --vars '{run_date: "2026-06-15"}'` 로 재실행하면 새 컬럼도 같이 채워짐 (insert_overwrite 가 통째로 재기록) |

→ **dbt 의 backfill 모델이 더 깔끔**. 옛 파티션 재실행이 자연스럽게 새 스키마로 마이그레이션.

### 7-5. `append_new_columns` 의 한계 — 컬럼 제거 시나리오

위 검증 후 추가 실험: revenue 추가 상태에서 SQL 에서만 revenue 빼고 다시 run.

| 확인 | 결과 |
|---|---|
| dbt 가 ALTER DROP COLUMN 발행? | ❌ 안 함 (append_new_columns 의 의도된 안전 동작) |
| BQ schema | revenue 컬럼 그대로 유지 (drop 안 됨) |
| **이번 run 의 파티션 revenue 값** | **NULL 로 덮어짐** (SQL 에서 빠졌으니) |
| 다른 옛 파티션 revenue 값 | 그대로 보존 |

⚠️ **부분적 안전망**: schema 는 보호되지만 **재실행되는 파티션의 데이터는 NULL 로 덮여짐**. 백필 돌리면 옛 데이터도 손실. → 더 보수적인 접근 필요 → § 8.

---

## 8. 실측 — `contract: enforced=true` + `on_schema_change=fail`

§ 7 의 한계 (자동화의 부분적 보호) 를 본 뒤, **schema.yml = single source of truth** 로 전환. 모든 변경을 명시적으로 강제.

### 8-1. 설정

```yaml
# schema.yml
- name: case2_kp_stat_ticket_use_daily
  config:
    contract: {enforced: true}
    on_schema_change: fail
  columns:
    - name: create_date
      data_type: date
    - name: series_id
      data_type: int64
    ... (전체 9 컬럼)
```

```sql
{{ config(
    materialized='incremental',
    contract={'enforced': true},
    on_schema_change='fail',
    ...
) }}
```

baseline: `dbt run --full-refresh` 로 BQ 9 컬럼 깨끗하게 리셋.

### 8-2. 시나리오 A — SQL 만 수정, schema.yml 안 건드림

```sql
SELECT ..., 5000 AS revenue   -- SQL 에만 추가
```

**결과**: dbt 컴파일 단계 fail

```
Compilation Error: This model has an enforced contract that failed.
| column_name | definition_type | contract_type | mismatch_reason     |
| revenue     | INT64           |               | missing in contract |
```

**BQ 영향**:
- DDL/DML 발행: **0개**
- 발행된 쿼리: SELECT 1개 (`SELECT * FROM (...) WHERE 1=0`, 데이터 0 스캔, contract 검증용 introspection)

→ 운영자가 schema.yml 빠뜨려도 BQ 까지 안 감.

### 8-3. 시나리오 B — SQL + schema.yml 동시 수정, BQ 는 아직 drift

```yaml
# schema.yml 에도 revenue 추가
- name: revenue
  data_type: int64
```

**결과**: contract 통과, 다음 단계의 on_schema_change check 에서 fail

```
The source and target schemas on this incremental model are out of sync!
They can be reconciled in several ways:
  - set the `on_schema_change` config to either append_new_columns or sync_all_columns
  - Re-run with `full_refresh: True`
  - update the schema manually and re-run

   Source columns not in target: [<BigQueryColumn revenue (INT64, NULLABLE)>]
```

**BQ 영향**:
- DDL/DML 발행: 0개
- 발행된 쿼리: SELECT introspection + `CREATE OR REPLACE TABLE __dbt_tmp` (임시 객체)
- ⚠️ **`case2_..._dbt_tmp` 가 BQ 에 잔존** — fail 단계가 contract 보다 늦어서 cleanup 안 됨. 다음 정상 run 에서 덮어써짐. 영향은 작음 (작은 임시 테이블).

### 8-4. 두 시나리오 비교

| | 시나리오 A | 시나리오 B |
|---|---|---|
| 막히는 매크로 | `assert_columns_equivalent` (contract) | `process_schema_changes` (on_schema_change) |
| 비교 대상 | SQL output ↔ schema.yml | SQL output ↔ BQ 실제 |
| 에러 메시지 | "missing in contract" | "source/target out of sync" |
| BQ 발자국 | SELECT introspection 1개 | introspection + `__dbt_tmp` 잔존 |
| 의미 | schema.yml 빠뜨림 잡음 | BQ 상태 갱신 필요 잡음 |

→ **contract 가 더 일찍, 더 깨끗하게 막아주는 1차 방어선**. on_schema_change 는 2차 백업.

### 8-5. 시나리오 C — SQL + schema.yml + pre_hook 으로 BQ 도 동기화 (정상 워크플로우)

시나리오 B 에서 막힌 BQ drift 를 명시적 DDL 로 해결. pre_hook 에 `ALTER TABLE ADD COLUMN IF NOT EXISTS` 추가.

```sql
{{ config(
    materialized='incremental',
    contract={'enforced': true},
    on_schema_change='fail',
    pre_hook=["ALTER TABLE {{ this }} ADD COLUMN IF NOT EXISTS revenue INT64"],
    ...
) }}
```

`IF NOT EXISTS` 필수 — pre_hook 은 **매 run 실행**되므로 idempotent 보장 필요.

**BQ job 시퀀스** (성공 케이스):

| 순서 | 시각 | 명령 | 의미 |
|---|---|---|---|
| 1 | 09:45:45 | `ALTER TABLE case2 ADD COLUMN ... IF NOT EXISTS` | **pre_hook 발행** |
| 2 | 09:45:46 | `SELECT * FROM (...) WHERE 1=0` | contract introspection |
| 3 | 09:45:47 | `CREATE OR REPLACE TABLE __dbt_tmp` | 임시 테이블 (10 컬럼) |
| 4 | 09:45:50 | `MERGE INTO case2` | 실제 데이터 merge |
| 5 | 09:45:51 | `DROP TABLE __dbt_tmp` | cleanup |

**결과**:
- BQ schema: 10 컬럼 (revenue 추가됨)
- 이번 파티션의 revenue = 5000
- 시나리오 B 에서 잔존했던 `__dbt_tmp` 도 단계 3 의 `CREATE OR REPLACE` 가 덮어쓰면서 자연스럽게 청소됨

### 8-6. 권장 운영 워크플로우 (컬럼 추가)

```
한 PR 안에:
1. SQL 에 새 컬럼 추가
2. schema.yml 에 컬럼 항목 추가 (data_type 포함)
3. pre_hook 에 `ALTER TABLE {{ this }} ADD COLUMN IF NOT EXISTS <new_col> <type>` 추가

머지 → 다음 dbt run:
  └─ pre_hook 이 BQ 에 컬럼 추가
  └─ contract 통과 (SQL == schema.yml)
  └─ on_schema_change 통과 (SQL == BQ, 방금 ALTER 했으니)
  └─ 정상 merge

다음 PR (선택):
  pre_hook 의 IF NOT EXISTS 줄 제거 (일회성 작업 정리)
  또는 그대로 두면 idempotent 라 무해
```

### 8-7. Neptune 과의 비교 (이번 패턴 기준)

| 동작 | Neptune | dbt + contract + fail |
|---|---|---|
| 컴파일 단계 검증 | 없음 | ✅ schema.yml 일치 강제 |
| 빠뜨림 감지 | (자동 sync 가 silently 처리) | ✅ 표 형태로 어떤 컬럼 미스매치인지 알림 |
| BQ/Hive 자동 수정 | ✅ 사용자 의도 무관 자동 ALTER | ❌ 의도적 단계 필요 (pre_hook 또는 --full-refresh) |
| 사고 가능성 | 자동화 의존 → 실수 그대로 적용 | 명시적 의도 표현 강제 |
| 운영자 부담 | 낮음 (저장만 하면 됨) | 약간 높음 (3곳 다 수정 필요) — 안전 비용 |

**핵심 결론**: 자동화 ↓ 안전성 ↑. POC 단계엔 명시 패턴이 운영 사고 막아주는 게 더 가치 있음.

---

## 9. 미해결 / 추가 검토

- BQ 의 [SCHEMA_UPDATE_OPTIONS](https://cloud.google.com/bigquery/docs/managing-table-schemas#updating_a_tables_schema) (`ALLOW_FIELD_ADDITION`, `ALLOW_FIELD_RELAXATION`) 과 dbt 의 `on_schema_change` 가 어떻게 상호작용하는지 (둘 다 적용 시 우선순위?)
- dbt-bigquery 의 column policy tag 자동 동기화 동작 (변경 시 BQ 정책 업데이트되는지)
- model versions 가 Neptune 의 backfill 시나리오 (구 모델로 과거 데이터 다시 빌드) 를 대체 가능한지
- 컬럼 **삭제** 실측 (sync_all_columns + 옛 파티션 동작) — 추가가 아닌 drop 의 운영 영향. § 7 의 추가 검증은 했지만 삭제는 아직