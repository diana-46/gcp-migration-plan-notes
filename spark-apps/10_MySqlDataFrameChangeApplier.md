---
title: "MySqlDataFrameChangeApplier — 앱 상세"
status: draft
tags:
  - spark-apps
  - gcp이관
  - mysql
  - reverse-etl
created: 2026-09-01
updated: 2026-09-01
---

# MySqlDataFrameChangeApplier — 앱 상세

> `com.kakaopage.spark.app.exports.mysql.MySqlDataFrameChangeApplier` · 실행 경로 `bin/run.sh` (클래스 직접 지정)
> 근거: 프로덕션 `actions` + `action_dependencies` + `task_instance`(90일) + 코드.
> 관련: [[1_사용중인_spark_job]] #5

## 한 줄

**두 시점 스냅샷(parquet)을 비교해 차이만 MySQL 에 반영하는** reverse ETL 앱.
현행 **5 태스크 / 2 DAG**, 90일간 전부 success.

Spark 은 여기서 **diff 계산 + JDBC write** 용도로만 쓰인다.

## 1. 동작 — diff 후 3종 DML

```scala
val beforeDf = spark.read.parquet(config.beforePath).select(columnsToApply)
val afterDf  = spark.read.parquet(config.afterPath ).select(columnsToApply)

val diff = DataFrameUtils.findDifference(beforeDf, afterDf, keys = keyColumns)

insertIgnoreIntoTable(diff.added,    ...)   // INSERT IGNORE
deleteFromTable      (diff.removed,  ...)   // DELETE WHERE key = ?
replaceIntoTable     (diff.modified, ...)   // REPLACE INTO
```

- `-b`(before) / `-a`(after) parquet 두 개를 읽어 `-c` 컬럼만 뽑고 `-k` 키로 diff
- MySQL **master** 커넥션으로 write
- `INSERT IGNORE` / `REPLACE INTO` 는 MySQL 전용 문법 → 사내 확장 `SparkMySqlExtensionUtils` 의존

## 2. 현행 5건 — 용도가 두 가지다

### Case A — Neptune 스냅샷 시간 델타 → 정산 DB (4건, 매시간)

`data_0200_dump_hourly` / `0 * * * *` / 각 1,440회 success

| uid | 대상 | before/after 소스 | keys | 반영 컬럼 |
|---|---|---|---|---|
| 33745 | `settlement.t_category` | `/page_service/production/raw/neptune/snapshot_t_category/snap_date=…/snap_hour=…` | `uid` | `uid, title` |
| 33743 | `settlement.t_publisher` | `/page_user/production/raw/neptune/snapshot_t_publisher/…` | `uid` | `uid, display_name, seller_type, status` |
| 33679 | `settlement.t_series_product` | `/page_service/production/raw/neptune/snapshot_t_series_product/…` | `uid` | `uid, series_id, title, category_uid, start_sale_dt, business_model, author_name, product_code, …` |
| 33744 | `settlement.t_ticket_info_product` | `/page_service/production/raw/neptune/snapshot_t_ticket_info_product/…` | **`series_id, position`** (복합키) | `series_id, ticket_type, item_code, position` |

**앞 시간 스냅샷 vs 현재 시간 스냅샷**을 비교해 변경분만 정산 DB(`page_settlement`)로 복제한다.
서비스 쪽 마스터 데이터를 정산 쪽으로 시간별 동기화하는 구조다.

전체 체인:
```
Service MySQL (page_service / page_user)
   ↓ CDC (mandu/Debezium — 기존 시스템)
_ro Hudi 테이블
   ↓ Neptune (Presto CTAS, 명시적 CAST) — 시간별 스냅샷
/…/raw/neptune/snapshot_{table}/snap_date=…/snap_hour=…
   ↓ ChangeApplier (앞뒤 스냅샷 diff)
정산 MySQL (page_settlement.settlement.*)
```

### Case B — 집계 캐시 refresh (1건, 일 1회)

`data_0008_dump_mysql_page_service_daily` / `0 0 * * *` / 60회 success / uid 8854

```bash
run.sh MySqlDataFrameChangeApplier cluster 2 2g 1 \
  -b /page_service/{phase}/hive/db/t_series_alarm_summary_base \
  -a /page_service/{phase}/hive/db/t_series_alarm_summary_export \
  -c series_id,alarm_count -k series_id \
  -s page_service -e {phase} -d service -t t_series_alarm_summary
```

- upstream: `create_t_series_alarm_summary_export` (uid 19975) 가 Presto CTAS 로 두 테이블을 만든다
  - `_base`   : 서비스 DB 현재 캐시(`t_series_alarm_summary_ro`)를 그대로 재구성 → **현재 상태**
  - `_export` : 원천 알람 노티(`t_series_update_noti_ro`)에서 `COUNT(*)` 재계산 → **정확한 값**
- diff = **캐시와 정확값의 편차** → 서비스 DB `service.t_series_alarm_summary` 를 교정

### Case A vs B 비교

| | Case A (매시간, 4건) | Case B (일 1회, 1건) |
|---|---|---|
| `-b` before | 이전 시간 스냅샷 | 서비스 DB 현재 캐시 |
| `-a` after | 현재 시간 스냅샷 | 원천에서 재계산한 정확값 |
| diff 의미 | **시간 축 변경분** | **캐시와 정확값의 편차** |
| 타겟 | 정산 DB (`page_settlement`) | 서비스 DB (`page_service`) — 자기 시스템 |
| 목적 | 조직 경계 넘어 마스터 데이터 복제 | 집계 캐시 교정 |
| 왜 diff 인가 | 시간별 변경분만 반영 | 서비스가 실시간 참조 중 → TRUNCATE+INSERT 불가 |

**앱은 하나지만 대체 방안은 케이스별로 다르다.**

## 3. ⚠️ 소스가 세대 교체됐다 — 구세대가 hidden 으로 남아 있다

`data_0200_dump_hourly` 에 같은 이름의 태스크가 **두 세트** 있다.

| 세대 | uid | hidden | before/after 소스 |
|---|---|:---:|---|
| **구** | 8310~8313 | **1** | `/page_service/{phase}/raw/mysql/service/t_series_product_{timestamp}` ← **MySQL 덤프 산출물** |
| **현** | 33679, 33743~33745 | 0 | `/page_service/production/raw/neptune/snapshot_{table}/snap_date=…` ← **Neptune 스냅샷** |

즉 소스가 **`MySqlDataFrameImporter` 덤프 → Neptune(Presto CTAS) 스냅샷** 으로 바뀌었다.
[[8_MySqlDataFrameImporter]] §4 에서 확인한 "활성 DAG 안에서 꺼진 덤프 11건" 과 짝이 맞는다
— 덤프가 CDC(`_ro`) 로 넘어가면서, 그걸 소비하던 ChangeApplier 도 Neptune 스냅샷 기반으로 재작성된 것이다.

> **함정** — 구세대 4건의 `task_instance` 에도 **1,440회 success** 가 찍혀 있다.
> `hidden=1` 이라 `DummyOperator` 로 치환된 no-op 이다. 실행 이력만 보면 8건이 도는 것처럼 보인다.

## 4. 존재 이유 (가설)

- 정산 시스템이 자기 계산에 서비스 쪽 마스터 데이터를 필요로 한다
- 정산 시스템이 서비스 DB 에 직접 접근할 수 없는 사정이 있다 (조직/보안 추정)
- 그래서 **CDC(`_ro`) → Neptune 스냅샷 → diff → 정산 DB** 라는 우회 체인이 유지된다

> ❓ 이 가설 확인 필요. 정산 팀이 BQ 를 직접 읽을 수 있다면 Case A 는 앱 자체가 불필요해진다.

## 5. 이관 관점

### Case A

> **전제: GCP 에서도 Kafka / Debezium 은 유지된다** (팀 확인).
> 이 전제가 A-0 을 가능하게 한다.

#### A-0. Debezium + JDBC Sink 직결 — **최선** ⭐

```
현재:  서비스 MySQL → Debezium → Kafka → _ro Hudi
                                            ↓ Neptune (Presto CTAS, 시간별 스냅샷)
                                          snapshot_{table}
                                            ↓ ChangeApplier (앞뒤 스냅샷 diff)
                                          정산 MySQL

A-0 :  서비스 MySQL → Debezium → Kafka → [Kafka Connect JDBC Sink] → 정산 Cloud SQL
```

**4단계가 1단계로 줄고, 앱이 사라진다.**

| | 현재 (스냅샷 diff) | A-0 (CDC 직결) |
|---|---|---|
| 지연 | 최대 1시간 | 준실시간 |
| **drift** | **한 번 실패하면 영구 누락** (§ 인접 시점만 비교) | **구조적으로 없음** |
| 단계 | Neptune + ChangeApplier | Sink 커넥터 하나 |
| 컬럼 subset | 앱의 `-c` 인자 | SMT 로 필드 제외 |
| DELETE | 앱이 처리 | tombstone 처리 |

**Debezium 은 source 커넥터**이고, 반대편에 **Kafka Connect JDBC Sink** 를 붙이면 타겟 DB 에 바로 쓴다.
지금 ChangeApplier 가 수동으로 하는 일(diff 계산 → INSERT/DELETE/REPLACE)을 커넥터가 대신한다.

**확인이 필요한 것 2가지**

1. **정산 쪽이 준실시간 반영을 받아도 되는가**
   지금은 시간별 배치라 **정산 계산 중에 마스터 데이터가 안 바뀐다.**
   CDC 직결이면 계산 도중 카테고리명·출판사 정보가 바뀔 수 있다.
   정산은 정합성이 중요한 도메인이라 **"안정된 스냅샷"을 일부러 원하고 있을 가능성**이 있다.
   → 이것이 시간별 배치인 이유일 수도 있다. **기술이 아니라 업무 요건 문제.**

2. **스키마 drift 방어를 무엇으로 대체하는가**
   Neptune CTAS 의 명시적 CAST(`CAST(uid AS decimal(20,0))`)가 지금 방어막 역할을 한다.
   CDC 직결이면 이 층이 없어지므로 소스 스키마 변경이 싱크를 깨뜨릴 수 있다.

#### 그 외 옵션

Kafka 를 안 쓰거나 A-0 이 막힐 때의 대안.

| 옵션 | 방식 | 평가 |
|---|---|---|
| A-1. BQ 에서 diff + Composer/Python write | diff 를 BQ SQL 로, 결과만 Cloud SQL 에 반영 | 반영 대상이 변경분뿐이라 데이터량은 작다. **비교 기준을 "마지막 push 상태"로 바꾸면 drift 도 해소** |
| A-2. Dataproc lift | Spark 그대로 | 사내 확장(`INSERT IGNORE`/`REPLACE INTO`) 재사용. **문제를 그대로 안고 감** |
| A-3. 정산 팀이 BQ 직접 read | 앱 자체 제거 | 가장 깔끔하나 **조직 협의 필요** (§4). 정산이 배치로만 읽으면 가능 |

> Datastream 은 **BQ/GCS 로만 랜딩**한다. 서비스 Cloud SQL → 정산 Cloud SQL 직접 CDC 는 불가하므로,
> Kafka 를 안 쓰는 시나리오에서는 A-1/A-3 만 남는다.

### Case B

조직 경계가 없어 선택지가 더 넓다.
- BQ scheduled query 로 정확값 계산 → Cloud SQL reverse ETL
- 또는 **Cloud SQL 안에서 stored procedure 로 직접 재계산** (Spark 불필요)

### 공통 제약

`INSERT IGNORE` / `REPLACE INTO` 는 MySQL 전용 문법이다.
Cloud SQL for MySQL 을 유지하면 그대로 쓸 수 있고, 다른 DB 로 가면 `INSERT … ON DUPLICATE KEY UPDATE` 등으로 재표현해야 한다.

> Datastream 은 BQ/GCS 로만 랜딩한다. **서비스 Cloud SQL → 정산 Cloud SQL 직접 CDC 는 불가**하므로
> 필요하면 Cloud SQL External Replica 나 DMS 를 별도 검토해야 한다.

## 6. ❓ 논의 필요

- **정산 쪽이 준실시간 반영을 받아도 되는지** — A-0 의 유일한 블로커.
  지금 시간별 배치인 이유가 "계산 중 데이터 고정" 이라면 CDC 직결이 막힌다 (§5 A-0)
- **정산 시스템이 왜 마스터 데이터를 자기 DB 에 두어야 하는지** — 조직/보안/성능? (§4)
  → 이 답이 A-3(앱 제거) 가능 여부를 결정한다
- 정산 팀이 **BQ 를 직접 read 할 수 있는지** (협의 가능 여부)
- **스키마 drift 방어** — Neptune CTAS 의 명시적 CAST 를 CDC 직결에서 무엇으로 대체할지 (§5 A-0)
- Neptune 의 Presto CTAS + 명시적 CAST 를 **BQ view 로 대체할 때 스키마 안정성** (Datastream 랜딩 스키마 검증)
- Case B `_base` / `_export` 계산 로직 — Cloud SQL stored procedure 로 옮길 수 있는지
- Cloud SQL for MySQL 유지 여부 (`INSERT IGNORE` / `REPLACE INTO` 호환성)
- 구세대 hidden 액션 4건(8310~8313) 정리 가능 여부 (§3)

## 재현

```sql
-- 현행 5건
SELECT w.name dag, w.schedule_interval, a.uid, a.name, a.pool, a.kwargs
FROM actions a
  JOIN workflows w ON w.uid = a.workflow_uid
  JOIN dag d ON d.dag_id = w.name
WHERE a.hidden = 0
  AND d.is_paused = 0 AND d.is_active = 1 AND d.next_dagrun IS NOT NULL
  AND a.kwargs LIKE '%MySqlDataFrameChangeApplier%';

-- 세대 비교 (hidden 포함)
SELECT a.uid, w.name dag, a.name, a.hidden, a.kwargs
FROM actions a JOIN workflows w ON w.uid = a.workflow_uid
WHERE a.kwargs LIKE '%MySqlDataFrameChangeApplier%'
ORDER BY a.name, a.hidden DESC;

-- Case B upstream (_base / _export 를 만드는 태스크)
SELECT up.uid, up.name, up.kwargs
FROM action_dependencies d JOIN actions up ON up.uid = d.upstream_action_uid
WHERE d.action_uid = 8854;
```

코드: `MySqlDataFrameChangeApplier.scala` — parquet read + diff(87~90행),
JDBC master 커넥션(92~102행), `applyDiff` 3종 DML(107~126행).
`DataFrameUtils.findDifference` 가 added/removed/modified 를 계산한다.
