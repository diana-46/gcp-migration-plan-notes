---
title: "TicketUseRecord — 앱 상세"
status: draft
tags:
  - spark-apps
  - gcp이관
  - hudi
  - buydb
created: 2026-08-31
updated: 2026-08-31
---

# TicketUseRecord — 앱 상세

> `com.kakaopage.spark.app.etl.TicketUseRecord` · 실행 스크립트 `bin/adhoc/run_ticket_use_record.sh`
> 근거: 프로덕션 `actions` + `action_dependencies` + `etl`/`etl_mapping` + `task_instance`(90일) + 코드.
> 관련: [[1_사용중인_spark_job]] #8

## 한 줄

buydb 의 **티켓 사용 이력**(Hudi 16 샤드)과 **티켓 구매 이력**(Neptune parquet)을 조인해 중간 산출물을 만드는 앱.
현행 **1 태스크 / 1 DAG**, 매시간, 90일간 **1,439회 success**.

**이 앱은 기존 Presto ETL 을 대체하며 들어왔다** (§4). 이관 방향을 정할 때 그 이유가 핵심이다.

## 1. 실행 형태

```bash
adhoc/run_ticket_use_record.sh \
  -e production -v buydb2 \
  -d {{ data_interval_start KST %Y%m%d }} \
  --from-timestamp  "{{ data_interval_start KST }}" \
  --until-timestamp "{{ data_interval_end   KST }}" \
  -o /page_buydb/production/tmp/ticket_use_record/{{ YYYYMMDD-HH }}
```

| 항목 | 값 |
|---|---|
| DAG | `data_0203_dump_mysql_buydb_hourly` |
| cron | `0 * * * *` — 매시간 |
| action uid | 19612 (`data_spark_kp_ticket_use_record_make_file`) |
| pool | `mysql_buydb01_large` |
| 출처 | **수동 생성** (athlon ETL 등록 아님) |
| 90일 실행 | 1,439회 success |

## 2. 연결된 대상

### 읽기 ①  Hudi — 티켓 사용 이력 (16 샤드)

```
/page_buydb/production/raw/mysql/boracay_production/ticket_use_history/data/shard={01..16}/create_date={YYYYMMDD}/
```

- `1L to 16` 루프로 각 샤드를 읽어 `reduce(_ union _)`
- `created_dt` 시간 윈도우 필터 (`from` ≤ x < `until`)
- 컬럼 리네임: `product_id → single_id`, `created_dt → use_dt`, `id → uid`(decimal 캐스팅)

### 읽기 ②  HDFS parquet — 티켓 구매 이력 (Neptune 산출물)

```
/page_buydb/production/raw/neptune/ticket_buy_record
```

- 필터: `create_date <= {targetDate}` **AND** `sale_type = 'S'`
- 컬럼 리네임: `uid → sales_ticket_uid`, `create_dt → buy_dt`
- 이 경로는 같은 DAG 의 **선행 Neptune ETL** (`data_neptune_kp_ticket_buy_record_*`, etl_id 730) 산출물이다
  → Hive `page_buydb_production.ticket_buy_record`

### 조인

```scala
ticketBuyRecords.join(broadcast(ticketSales), Seq("ticket_uid"), "inner")
```

> ⚠️ **[[1_사용중인_spark_job]] #8 의 "buy records 를 broadcast" 는 반대다.**
> 코드는 `broadcast(ticketSales)` — **시간 윈도우로 자른 사용 이력 쪽**을 broadcast 한다.
> 구매 이력(`ticketBuyRecords`)은 `create_date <= targetDate` 라 **전체 누적분**이므로 훨씬 크다.
> 즉 "작은 쪽(1시간치 사용 이력)을 broadcast" 가 맞고, 방향은 합리적이다.

### 쓰기

```
/page_buydb/production/tmp/ticket_use_record/{YYYYMMDD-HH}
```
`repartition(1)` → parquet, `SaveMode.Overwrite`. **테이블 등록 없음** (tmp 중간 산출물).

## 3. DAG 체인 — 앞뒤가 다 붙어 있다

```
[센서] kp_buydb_v_ticket_buy_history_hourly_sensor
       kp_buydb_v_ticket_buy_history_detail_hourly_sensor
   ↓
① data_neptune_kp_ticket_buy_record_*        (5스텝, 활성, etl_id 730)
     → page_buydb_production.ticket_buy_record
   ↓  (+ kp_buydb_v_ticket_use_history_hourly_sensor)
② data_spark_kp_ticket_use_record_make_file  ← 이 앱 (19612)
     → /page_buydb/production/tmp/ticket_use_record/{YYYYMMDD-HH}
   ↓
   merge_and_move → add_partition → cleanup_and_compute_stats → end_etl  (19613~19616)
   ↓
③ data_neptune_kp_export_ticket_use_record_* (5스텝, 활성, etl_id 1649)
     → page_buydb_production.kp_export_ticket_use_record
```

- 이 앱은 **체인 중간의 중간 산출물**이다. 최종 소비 대상은 ③ 의 `kp_export_ticket_use_record`.
- ②의 후속 `merge_and_move` 는 `DataFrameMerger` — **이관 제외(소멸) 결정된 앱**이다.
  즉 이 체인은 어차피 뒷부분부터 재설계 대상이다.

## 4. ⚠️ 이 앱은 Presto ETL 을 대체하며 들어왔다

같은 DAG 에 **꺼진 Neptune 판**이 통째로 남아 있다.

| 경로 | 액션 | hidden | 출처 |
|---|---|:---:|---|
| `data_neptune_kp_ticket_use_record_*` | 19045~19050 (5스텝) | **1** | athlon ETL (etl_id 731) |
| `data_spark_kp_ticket_use_record_*` | 19612~19616 (5스텝) | 0 | **수동 생성** |

꺼진 Presto 판의 SQL 을 보면 **같은 조인을 Presto 로 하고 있었다**:

```sql
CREATE TABLE page_buydb_production.ticket_use_record_{ts} WITH (format='PARQUET') AS
SELECT ts.uid, ts.ticket_uid, br.uid sales_ticket_uid, ts.user_uid, ts.series_id,
       ts.pid single_id, br.publisher_uid, ts.product_type, br.category_uid, ...
FROM (SELECT * FROM page_buydb_production.ticket_buy_record
      WHERE create_date <= '{YYYYMMDD}' AND sale_type = 'S') br
INNER JOIN page_buydb_production.v_t_ticket_sales_ro ts ON ts.ticket_uid = br.ticket_uid
WHERE ts.create_date = '{YYYYMMDD}' AND ts.create_dt ...
```

### 무엇이 바뀌었나

| | Presto 판 (꺼짐) | Spark 판 (현행) |
|---|---|---|
| 사용 이력 소스 | `page_buydb_production.**v_t_ticket_sales_ro**` (Hive 뷰) | **Hudi 16 샤드 직접 read** |
| 실행 엔진 | Presto CTAS | Spark |
| 등록 방식 | athlon ETL | 수동 생성 |
| DB 버전 | `t_ticket_sales` (buydb1) | `ticket_use_history` (**buydb2**) |

### 전환 사유 — **Presto 부하** (팀 확인)

Hudi 미지원이 아니라 **Presto 클러스터 부하가 심해서** Spark 으로 옮긴 것이다.

이게 중요한 이유는, **부하의 원인이 아직 코드에 그대로 남아 있기 때문**이다.

```scala
// 매시간 실행되는데 구매 이력은 누적 전체를 읽는다
.filter(s"create_date <= '${config.targetDate}'")
```

- 사용 이력(`ticket_use_history`) : **1시간치**
- 구매 이력(`ticket_buy_record`) : **전체 누적분** ← 매시간 풀스캔

지금 사용된 티켓이 오래전에 구매됐을 수 있으므로 **로직상 전체 구매 이력이 필요하다.**
Presto 는 이 시간당 풀스캔을 감당하지 못했고, Spark 은 전용 리소스로 버티고 있는 상태다.

> ⚠️ **BQ 로 그냥 옮기면 문제의 형태만 바뀐다.**
> Presto 에서는 *클러스터 부하*였던 것이 BigQuery 에서는 *스캔 비용*이 된다
> (BQ 는 스캔한 바이트 기준 과금). 매시간 전체 구매 이력을 읽는 구조를 그대로 두면
> 비용이 그대로 따라온다.
>
> **대응: `ticket_buy_record` 를 `ticket_uid` 로 클러스터링**하면
> 조인 시 해당 블록만 읽어 스캔량을 크게 줄일 수 있다.
> `create_date` 파티셔닝은 이 쿼리에 도움이 안 된다 — 어차피 전 기간을 읽어야 하기 때문이다.
> 즉 **이관은 "엔진 교체"가 아니라 "스캔 구조 재설계"와 함께 가야 한다.**

## 5. 코드 특이점

### A. OLD_DB(buydb1) 경로가 dead code 로 남아 있다

```scala
case object BuyDbVersion { val OLD_DB = "buydb1"; val NEW_DB = "buydb2" }
private val SHARD_NO_OLD_DB = 8
private val SHARD_NO_NEW_DB = 16
```
- `loadOldDb` : 8 샤드, `t_ticket_sales`, `pid → single_id`, `create_dt`
- `loadNewDb` : 16 샤드, `ticket_use_history`, `product_id → single_id`, `created_dt`

현행 액션은 `-v buydb2` 만 쓴다. **OLD_DB 경로는 정리 가능.**

### B. 매시간 전체 구매 이력을 재스캔한다

```scala
.filter(s"create_date <= '${config.targetDate}'")
```
사용 이력은 1시간치인데 구매 이력은 **누적 전체**를 읽는다. 매시간 반복되므로 비효율이 크다.
BQ 로 옮기면 파티션 프루닝·클러스터링으로 개선 여지가 크다.

### C. `repartition(1)`

출력을 단일 파일로 만든다. 후속 `merge_and_move` 가 있는데도 1로 줄이는 이유는 불명. ❓

### D. `--disable-broadcast-join` 옵션은 미사용

기본값(broadcast 활성) 그대로 쓴다.

### E. `boracay_production` 경로

`/page_buydb/${config.env}/raw/mysql/boracay_${config.env}/...` — `env` 로 치환되므로
[[7_PushTargetUserImporter]] 의 하드코딩 케이스와 달리 여기서는 정상이다.

## 6. 이관 방향

| 옵션 | 방식 | 평가 |
|---|---|---|
| **A. BQ SQL 재구현 + 스캔 구조 재설계** | Datastream 이 buydb → BQ 랜딩 후 SQL 로 조인. `ticket_buy_record` 를 `ticket_uid` 클러스터링 | **유력.** 로직이 `union + filter + inner join` 뿐이고 **원래 Presto SQL 이었다**(§4). 16 샤드 union 도 BQ 랜딩이 단일 테이블이면 자연 해소 |
| B. Dataproc lift | Spark 그대로 | Hudi 를 계속 쓸 경우. 단 **부하 문제를 그대로 안고 간다** |

**전제 조건: buydb 의 CDC 이관 방향.** Datastream → BQ 로 가면 A, Hudi 유지면 B.

> **A 로 가더라도 "SQL 만 옮기면 끝"이 아니다.**
> Presto 를 떠난 이유가 부하였고 그 원인(매시간 누적 풀스캔)이 코드에 그대로 있다(§4).
> BQ 에서는 같은 문제가 스캔 비용으로 나타난다. **클러스터링 설계가 이관의 일부**다.

체인 전체가 어차피 재설계 대상이다 — ②의 후속 `merge_and_move`(`DataFrameMerger`)가 소멸 결정이고,
③ `kp_export_ticket_use_record` 도 Neptune ETL 이라 함께 옮겨야 한다.
**앱 하나만 lift 하는 것은 의미가 적다.**

## 7. ❓ 논의 필요

- **buydb CDC 이관 방향** (Datastream → BQ vs Hudi 유지) — 이 앱 존폐를 좌우한다
- ~~Presto → Spark 전환 사유~~ → **Presto 부하** 로 확인됨 (§4)
- **`ticket_buy_record` 의 규모** — 매시간 풀스캔 대상. BQ 스캔 비용 추산에 필요
- **매시간 주기가 필수인지** — 일 1회로 낮출 수 있으면 스캔 비용이 24분의 1이 된다
- ③ `kp_export_ticket_use_record` 의 소비처 — 이 체인의 최종 목적
- OLD_DB(buydb1) dead code 정리 가능 여부
- `repartition(1)` 인 이유 (§5-C)
- `boracay` 가 무엇인지 (사내 시스템 이름으로 추정)
- 매시간 구매 이력 전체 재스캔의 실제 비용 (§5-B)

## 재현

```sql
-- 현행 액션
SELECT a.uid, w.name dag, w.schedule_interval, a.name, a.pool, a.kwargs
FROM actions a
  JOIN workflows w ON w.uid = a.workflow_uid
  JOIN dag d ON d.dag_id = w.name
WHERE a.hidden = 0
  AND d.is_paused = 0 AND d.is_active = 1 AND d.next_dagrun IS NOT NULL
  AND a.kwargs LIKE '%run_ticket_use_record%';

-- DAG 체인 (Neptune 판/Spark 판 비교 · 의존성 포함)
SELECT a.uid, a.name, a.operator_class, a.hidden,
       (SELECT GROUP_CONCAT(up.name SEPARATOR ', ')
          FROM action_dependencies dd JOIN actions up ON up.uid = dd.upstream_action_uid
         WHERE dd.action_uid = a.uid) upstream
FROM actions a JOIN workflows w ON w.uid = a.workflow_uid
WHERE w.name = 'data_0203_dump_mysql_buydb_hourly'
  AND (a.name LIKE '%ticket_use_record%' OR a.name LIKE '%ticket_buy_record%')
ORDER BY a.uid;

-- ETL 등록 정보 (dest_table 확인)
SELECT m.action_uid, e.id, e.unique_title, e.dest_table, c.name, c.type
FROM etl_mapping m JOIN etl e ON e.id = m.etl_id
  LEFT JOIN catalog_db c ON c.id = e.dest_db_id
WHERE m.action_uid IN (19040, 19046, 54084);
```

코드: `TicketUseRecord.scala` — 샤드 union(110~112행), Neptune parquet read(114행),
broadcast join(124~128행), `repartition(1)` 출력(144~146행). Jira: DD-3177 / DD-3991 / DD-5373.
