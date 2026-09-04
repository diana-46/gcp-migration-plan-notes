---
title: "MySqlDataFrameImporter — 앱 상세"
status: draft
tags:
  - spark-apps
  - gcp이관
  - mysql
  - cdc
created: 2026-08-31
updated: 2026-08-31
---

# MySqlDataFrameImporter — 앱 상세

> `com.kakaopage.spark.app.imports.MySqlDataFrameImporter` · 실행 스크립트 `bin/run_mysql_dump.sh`, `run_mysql_dump_ex5.sh`
> 근거: 프로덕션 `actions` + Airflow `task_instance`(90일) + `action_dependencies` + 코드.
> 관련: [[1_사용중인_spark_job]] #2 · [[5_MongoDataFrameImporter]] (같은 `DataFrameImporter` 프레임워크)

## 한 줄

MySQL 테이블 → HDFS parquet → Hive 뷰 + Presto 뷰 덤프 앱.
현행 **9 태스크 / 2 DAG**, 90일간 **전부 success**.

**이미 대부분 CDC 로 넘어갔고 9건이 잔여분이다** (§4 참고). 이관 결정에서 가장 중요한 맥락.

## 1. 현행 9건

| DAG | cron | 태스크 | 소스 | 특이 옵션 |
|---|---|---|---|---|
| `data_0004_dump_mysql_userinven_daily` | `0 18 * * *` | **8** | `userinven01`~`08` . `view_history_meta` | `--max-records-per-partition 200000`<br>`--create-hive-table false` |
| `data_0007_dump_mysql_page_userpublic_daily` | `0 0 * * *` | 1 | `userpublic.t_waitfree_user` | `--contain-db-name-in-hive-table false` |

- 8건은 `run_mysql_dump.sh`, 1건은 `run_mysql_dump_ex5.sh` (executor 5개 버전)
- **`userinven` 은 샤드마다 pool 이 따로다** — `mysql_userinven01_large` ~ `mysql_userinven08_large`.
  소스 DB 별로 동시성을 독립 제어한다.
- `data_0004` 만 유일하게 **18:00 스타트** (다른 daily 는 자정). 소스 DB 오프피크 회피로 추정.
- 90일 실행: userinven 8건 각 60회, userpublic 1건 60회 — 전부 success.

### 코드에 있지만 아무도 안 쓰는 옵션

`-w`(where), `-i`/`-x`(컬럼 필터), `--estimated-row-count`, `--partition-size` 는 **현행 9건에서 사용 0건**.
이관 시 이 기능들은 재현하지 않아도 된다.

## 2. 연결된 테이블

### 읽기 — MySQL

| 소스 DB | 테이블 |
|---|---|
| `userinven01` ~ `userinven08` (8 샤드) | `view_history_meta` |
| `userpublic` | `t_waitfree_user` |

### 쓰기 — Hive (3 객체)

`DataFrameImporter` 프레임워크 공통 규칙:

```
hiveDbName   = {serviceName}_{env}                    # -s, -e
hiveViewName = {dbName}_{table}                       # 기본
             = {table}                                # --contain-db-name-in-hive-table false

① 물리 테이블 : {hiveDbName}.{hiveViewName}_{outputDirTimestamp}
② Hive 뷰     : {hiveDbName}.{hiveViewName}
③ Presto 뷰   : {hiveDbName}.{hiveViewName}_presto
```

| 태스크 | Hive 뷰 |
|---|---|
| `userinven01_view_history_meta` | `page_userinven_{phase}.userinven01_view_history_meta` |
| … `userinven08` | `page_userinven_{phase}.userinven08_view_history_meta` |
| `t_waitfree_user` | `page_userpublic_{phase}.t_waitfree_user` ← db 이름 prefix 없음 |

프로덕션 액션에서 실제로 확인된다 (후속 Impala 태스크):
```sql
run_impala_sql.sh kakaopage_{phase}
  "INVALIDATE METADATA userdb_t_waitfree_user;
   INVALIDATE METADATA userdb_t_waitfree_user_{timestamp};"
```

> **`--contain-db-name-in-hive-table false` 는 현행에서 `t_waitfree_user` 1건뿐이다.**
> 기본값이면 `userpublic_t_waitfree_user` 가 됐을 텐데 이것만 오버라이드한다.
> ❓ 다른 소비처와 이름을 맞춰야 하는지, 이관 이력이 있는지 확인 필요.

### 후속 파이프라인

`userinven` 8건의 공통 후속은 `hdfs_mkdir_userinven_view_history_meta` — 8개 샤드 출력을 받을 디렉토리를 만든다.

```
/page_userinven/{phase}/raw/mysql/userinven/view_history_meta/create_date={YYYYMMDD}
```

`--create-hive-table false` 인 이유가 여기 있다. **8 샤드를 각각 Hive 테이블로 만들지 않고,
한 디렉토리에 모아 별도 스텝에서 병합 처리**하는 구조로 보인다.

## 3. 코드 특이점

### 커스텀 MySQL Dialect

```scala
// KS-7008: length 가 1 을 넘는 bit type 을 binary 로 가져오도록
JdbcDialects.unregisterDialect(JdbcDialects.get("jdbc:mysql"))
JdbcDialects.registerDialect(KakaoPageMySQLDialect)
```

Spark 기본 `MySQLDialect` 를 제거하고 사내 dialect 를 등록한다.
**이관 시 `BIT(n)` 컬럼의 타입 매핑이 달라질 수 있다** — Datastream/BQ 로 옮길 때 검증 필요.

### 파티셔닝

`MIN(uid)`/`MAX(uid)` 범위를 구해 JDBC 병렬 read.
`--max-records-per-partition` 기본값은 2,000,000 인데 `userinven` 은 **200,000** 으로 명시적으로 낮췄다.
row 사이즈가 크거나 메모리 이슈 회피용으로 추정.

## 4. ⚠️ 가장 중요한 맥락 — 이미 CDC 로 넘어갔다

`run_mysql_dump` 계열 액션 전체를 상태별로 세어보면:

| DAG 상태 | hidden | 액션 | DAG 수 |
|---|:---:|---:|---:|
| AIRFLOW없음 | 0 | 897 | 14 |
| AIRFLOW없음 | 1 | 13 | 4 |
| PAUSED | 1 | 3 | 1 |
| **활성** | **0** | **9** | **2** |
| **활성** | **1** | **11** | **4** |

**활성 DAG 안에서 꺼진(hidden=1) 덤프가 11건** 있다. 무엇이 꺼졌는지 보면 패턴이 뚜렷하다.

| DAG | 꺼진 액션 | 테이블 |
|---|---:|---|
| `data_0208_dump_mysql_page_service_hourly` | 6 | `t_category`, `series_product`, `single_product`, `info_product`, `series_rank`, `update_noti` |
| `data_0205_dump_mysql_page_billing_hourly` | 2 | `batch_management`, `credit_balances` |
| `data_0206_dump_mysql_page_user_hourly` | 2 | **`t_user`, `t_publisher`** |
| `data_0006_dump_mysql_page_user_daily` | 1 | `agreement_info` |

이 테이블들은 **mandu / Debezium CDC 의 `_ro` Hudi 테이블로 대체**됐다.
`MySqlDataFrameChangeApplier` 가 소스로 쓰는 Neptune 스냅샷이 바로 이 `_ro` 테이블에서 나온다
(`t_category`, `t_publisher`, `t_series_product`, `t_ticket_info_product`).

> **함정** — `hidden=1` 액션은 Airflow 에서 `DummyOperator` 로 치환된다.
> `t_user` / `t_publisher` 의 `task_instance` 에 **2,160회 success** 가 찍혀 있지만
> 전부 no-op 이다. 실행 이력만 보면 "시간별로 잘 돌고 있다"고 오판하게 된다.

### 왜 이 9건만 배치로 남았나 — 답이 나왔다

**둘 다 CDC 로 못 가서 남은 것이다. 우선순위 문제가 아니다.**

| 대상 | 태스크 | 배치로 남은 이유 |
|---|---:|---|
| `userinven01~08 . view_history_meta` | 8 | **CDC 불가로 판명** (팀 확인) |
| `userpublic . t_waitfree_user` | 1 | **변경분이 많아 당시 Hudi 수집이 너무 느렸음** (팀 확인) |

`t_waitfree_user` 는 "기다리면 무료" 유저별 타이머 상태로 추정되며, UPDATE 빈도가 매우 높다.
Debezium → Hudi 파이프라인이 그 변경량을 따라가지 못해 **일 1회 스냅샷 덤프로 남긴** 것이다.

즉 **현행 9건은 전부 "CDC 로 대체 불가" 판정을 이미 받은 잔여분**이고,
[[1_사용중인_spark_job]] 의 "CDC 이관 검토" 초안 판정은 이 앱에 대해서는 **부정으로 결론난 상태**다.

> ⚠️ **다만 GCP 에서는 재검증할 여지가 있다.**
> `t_waitfree_user` 를 배치로 남긴 판단은 **Hudi 의 수집 성능 한계**에 대한 것이지
> CDC 자체에 대한 것이 아니다. GCP 의 CDC 타겟은 Hudi 가 아니라 **Datastream → BigQuery** 이고,
> BQ 는 고빈도 변경 테이블에 대해 Hudi 와 다른 병합 메커니즘을 쓴다.
> **당시 제약이 새 스택에서도 유효한지는 별도 확인이 필요하다** — 자동으로 승계할 판단이 아니다.

## 5. 이관 방향

**전제: 9건 모두 배치 수집을 유지한다** (§4 — 둘 다 CDC 불가 판정).
따라서 선택지는 "CDC 로 갈지"가 아니라 **"배치 덤프를 GCP 에서 어떻게 구현할지"** 다.

| 옵션 | 방식 | 평가 |
|---|---|---|
| **A. `gcloud sql export csv`** | Cloud SQL → GCS CSV → BQ 로드 | **유력.** Spark 불필요. [[6_AgeGenderCategorizingImporter]] 와 같은 패턴. 샤드별 태스크로 나누면 현행 구조(샤드별 pool)와 그대로 대응 |
| B. Dataproc lift | Spark 그대로 | 커스텀 dialect·파티셔닝 로직을 그대로 재사용. 규모가 크면 유리 |
| ~~C. Datastream (CDC)~~ | — | **§4 에서 배제됨.** 단 Hudi → BQ 로 타겟이 바뀌므로 재검증 여지는 있음 |

A 와 B 의 갈림길은 **`view_history_meta` 의 규모**다.
행 수·소요 시간을 재보면 [[6_AgeGenderCategorizingImporter]] 때와 같은 방식으로 판단할 수 있다.

| # | 확인 항목 | 왜 |
|---|---|---|
| 1 | **`view_history_meta` 행 수 / 현행 소요 시간** | A(단일 export) vs B(Spark) 판단의 핵심 수치 |
| 2 | **8 샤드 병합 후속 처리** | `--create-hive-table false` + `hdfs_mkdir` 로 한 디렉토리에 모은다. BQ 에서는 와일드카드 로드로 통합 가능한지 |
| 3 | **`BIT(n)` 컬럼 존재 여부** | 커스텀 dialect (KS-7008) 대상. `gcloud sql export csv` 의 CSV 직렬화에서 어떻게 나오는지 확인 필요 |
| 4 | **샤드별 pool 대체** | `mysql_userinven01_large`~`08_large` 로 소스 DB 별 동시성을 제어 중. GCP 에서 동등한 보호책 |
| 5 | **`t_waitfree_user` 변경량 재측정** | Datastream + BQ 에서도 여전히 부담인지 (§4 의 재검증 항목) |

## 6. ❓ 논의 필요

**해결됨**
- ~~왜 이 9건만 CDC 에서 빠졌는지~~ → 둘 다 CDC 불가 판정 (§4)
- ~~`t_waitfree_user` 의 뷰 이름 오버라이드 이유~~ → MySQL DB 가 `userdb` → `userpublic` 로
  이동하면서 Hive 뷰명이 바뀌는 것을 막으려고 prefix 를 뺀 것으로 보임 (§7)

**남은 것**
- **`view_history_meta` 행 수 / 현행 소요 시간** — A(export) vs B(Dataproc) 판단 근거
- `userinven` 샤드 구조가 GCP 에서도 유지되는지 (Cloud SQL 샤드 그대로 vs 통합)
- `view_history_meta` / `v_t_waitfree_user` **소비처** — 둘 다 athlon 안에서 읽는 액션이 없다
- `--max-records-per-partition 200000` 의 근거 (row 크기? 메모리?)
- 샤드별 pool 을 GCP 에서 어떤 형태로 대체할지
- 커스텀 dialect 대상 `BIT` 컬럼이 실제로 있는지
- **`t_waitfree_user` 를 Datastream + BQ 로 재검증할지** — 당시 제약은 Hudi 성능이었다 (§4)

## 7. `t_waitfree_user` 이력

DB 를 옮기면서 전용 DAG 로 분리됐다.

| 시기 | DAG | 인자 |
|---|---|---|
| 과거 | `data_0000_dump_mysql_userdb_daily` (DAG 삭제됨) | `-d userdb -t t_waitfree_user **-x user_uid**` |
| 현행 | `data_0007_dump_mysql_page_userpublic_daily` | `-s page_userpublic -d userpublic -t t_waitfree_user **--contain-db-name-in-hive-table false**` |

- MySQL DB 가 `userdb` → `userpublic` 로 이동했다.
- `--contain-db-name-in-hive-table false` 는 그때 뷰명이 `userpublic_t_waitfree_user` 가 되는 것을
  막고 `t_waitfree_user` 로 고정하려는 조치로 보인다.
- 과거에는 **`-x user_uid`** 로 컬럼을 제외했으나 현행에는 없다. 스키마나 정책이 바뀐 듯. ❓

### 같은 스키마에서 유일하게 CDC 로 안 간 테이블

`page_userpublic_production` 에서 참조되는 테이블은 대부분 `_ro`(mandu/Debezium CDC) 다.

| 테이블 | 참조 횟수 | CDC |
|---|---:|:---:|
| `t_user_referrer_link_ro` | 33 | ✅ |
| `t_user_history_ro` | 11 | ✅ |
| `t_user_login_log_ro` | 10 | ✅ |
| `user_taste_ro` | 5 | ✅ |
| `account_ro` | 5 | ✅ |
| `user_signup_info_ro` | 4 | ✅ |
| `kakao_group_user_token_ro` | 3 | ✅ |
| `user_agreement_ro` | 2 | ✅ |
| **`t_waitfree_user`** | 2 (`v_` 뷰) | ❌ |

### 전용 DAG 이고 후속 소비자가 없다

`data_0007_dump_mysql_page_userpublic_daily` 는 태스크가 **2개뿐**이다.

```
7121  dump_mysql_userpublic_t_waitfree_user          (덤프)
19268 replace_presto_view_userpublic_t_waitfree_user (Presto 뷰 교체)
        DROP VIEW IF EXISTS page_userpublic_production.v_t_waitfree_user;
        CREATE VIEW ... AS SELECT * FROM page_userpublic_production.t_waitfree_user_{timestamp}
```

`v_t_waitfree_user` 를 **읽는 athlon 액션은 없다.** 소비처가 athlon 밖(애드혹 분석 또는 타 시스템)이다.
이관 시 소비처 확인이 필요하다.

## 재현

```sql
-- 현행 9건
SELECT w.name dag, w.schedule_interval, a.uid, a.name, a.pool, a.kwargs
FROM actions a
  JOIN workflows w ON w.uid = a.workflow_uid
  JOIN dag d ON d.dag_id = w.name
WHERE a.hidden = 0
  AND d.is_paused = 0 AND d.is_active = 1 AND d.next_dagrun IS NOT NULL
  AND a.kwargs LIKE '%run_mysql_dump%';

-- 상태별 전체 분포 (CDC 이전 현황 파악용)
SELECT CASE WHEN d.dag_id IS NULL THEN 'AIRFLOW없음'
            WHEN d.is_active = 0 THEN 'is_active=0'
            WHEN d.is_paused = 1 THEN 'PAUSED'
            WHEN d.next_dagrun IS NULL THEN '예약없음'
            ELSE '활성' END st,
       a.hidden, COUNT(*), COUNT(DISTINCT w.name)
FROM actions a
  JOIN workflows w ON w.uid = a.workflow_uid
  LEFT JOIN dag d ON d.dag_id = w.name
WHERE a.kwargs LIKE '%run_mysql_dump%'
GROUP BY 1, 2;

-- 활성 DAG 안에서 꺼진 덤프 (= CDC 로 넘어간 것들)
SELECT w.name dag, a.name
FROM actions a
  JOIN workflows w ON w.uid = a.workflow_uid
  JOIN dag d ON d.dag_id = w.name
WHERE a.kwargs LIKE '%run_mysql_dump%' AND a.hidden = 1
  AND d.is_paused = 0 AND d.is_active = 1 AND d.next_dagrun IS NOT NULL;
```

코드: `MySqlDataFrameImporter.scala` — 커스텀 dialect(84~86행), `hiveViewName`(88~94행),
`locationInfix`(97~98행). 공통 프레임워크는 `DataFrameImporter.scala`(115~125, 149행).
