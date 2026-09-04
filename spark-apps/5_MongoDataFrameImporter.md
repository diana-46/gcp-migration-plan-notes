---
title: "MongoDataFrameImporter — 앱 상세"
status: draft
tags:
  - spark-apps
  - gcp이관
  - mongodb
created: 2026-08-31
updated: 2026-08-31
---

# MongoDataFrameImporter — 앱 상세

> `com.kakaopage.spark.app.imports.MongoDataFrameImporter` · 실행 스크립트 `bin/run_mongo_dump.sh`
> [[1_사용중인_spark_job]] 의 #1 을 앱 단위로 파고든 문서.
> 근거: 프로덕션 `actions` + Airflow `task_instance`(90일) + `action_dependencies` + 코드.

## 한 줄

**MongoDB 컬렉션 → HDFS parquet → Hive 뷰 + Presto 뷰** 를 만드는 덤프 앱.
현행 **37 태스크 / 2 DAG**, 90일간 **4,770회 전부 success** (최근 2026-08-21).

## 1. 태스크 형태가 2종이다

같은 앱인데 호출 방식이 완전히 다르다. **이관도 따로 다뤄야 한다.**

| | **A. stat 덤프** | **B. open_log** |
|---|---|---|
| 태스크 수 | 36 | 1 |
| DAG | `data_0102_dump_mongo_stat_daily` | `data_0200_dump_hourly` |
| 주기 | daily `0 0 * * *` | **hourly `0 * * * *`** |
| Mongo 클러스터 | `stat` | `contents` |
| 컬렉션명 | 고정 (`DAdCapOverflow` 등) | **월 단위 시프트** `open_log_{YYYYMM}` |
| 적재 방식 | 전체 스냅샷 | **시간 윈도우 증분** (aggregation pipeline) |
| 파티션 키 | 없음 (기본 `_id`) | `open_dt` |
| 대상 스키마 | `page_contentdb_{phase}` | `page_service_{phase}` |
| Hive 테이블 생성 | true (34건) / false (2건) | **false** |

### B 의 실제 커맨드

```bash
run_mongo_dump.sh -s page_service -e {phase} --cluster-id contents \
  -c open_log_{{ data_interval_start.strftime('%Y%m') }} \
  --partition-key open_dt \
  -p '{$match: {open_dt: {$gte: ISODate("{{ data_interval_start }}"),
                          $lt:  ISODate("{{ data_interval_end }}") } } }' \
  --output-dir-timestamp {{ data_interval_end.int_timestamp }} \
  --create-hive-table false
```

- **컬렉션 이름 자체가 템플릿**이라 매월 대상이 바뀐다.
- `-p` aggregation pipeline 으로 시간 윈도우를 잘라 **hourly 증분 로드를 흉내낸다.**
- 후속 태스크가 `move_mongo_contents_open_log` (= `merge_and_move_dataframes.sh`) 하나뿐.
  A 계열의 Presto 뷰 재생성 흐름을 타지 않는다.

> **관찰** — 이 `open_log_{YYYYMM}` 컬렉션은 [[1_사용중인_spark_job]] 에 없는 앱인
> `kafka.MongoOpenLogStreamingApp` 이 **쓰는 대상**과 이름이 같다.
> 즉 **Kafka → (스트리밍) → MongoDB → (배치 덤프) → Hive** 라는 우회 경로로 보인다.
> 이관 시 이 우회가 필요한지부터 확인하는 편이 낫다. Kafka 에서 바로 BQ 로 가면 두 단계가 함께 사라진다.

## 2. 출력이 한 번에 3개 만들어진다

`DataFrameImporter` 프레임워크 공통 규칙 (`MySqlDataFrameImporter` 와 동일).

```
hiveDbName   = {serviceName}_{env}                         # -s, -e. -s 기본값은 kakaopage
hiveViewName = camelToSnake("{clusterId}_{collection}")     # Mongo 전략

① 물리 테이블 : {hiveDbName}.{hiveViewName}_{outputDirTimestamp}
② Hive 뷰     : {hiveDbName}.{hiveViewName}
③ Presto 뷰   : {hiveDbName}.{hiveViewName}_presto          # 코드상 접미어
```

`camelToSnake` 는 `StringUtils.scala` 기준 — `DAdCapOverflow` → `d_ad_cap_overflow`,
`DETCReport` → `detc_report` (연속 대문자 처리 규칙 주의).

### 실제로는 `v_` 접두 뷰가 하나 더 있다

후속 태스크 `replace_preto_view_*` (32건, 오타 그대로 `preto`) 가 Presto 로 뷰를 다시 만든다.

```sql
DROP VIEW IF EXISTS page_contentdb_production.v_stat_d_ad_cap_overflow;
CREATE OR REPLACE VIEW page_contentdb_production.v_stat_d_ad_cap_overflow AS ...
```

즉 소비자가 실제로 보는 건 **`v_` 접두 Presto 뷰**다. 이관 시 이 뷰 계층까지 대응이 필요하다.

## 3. 대상 테이블 전량 (37)

### A. `data_0102_dump_mongo_stat_daily` — 36건, `stat` 클러스터 → `page_contentdb_{phase}`

| 컬렉션 | Hive 뷰 | 특이 옵션 |
|---|---|---|
| `celery_task_meta` | `stat_celery_task_meta` | |
| `DAdCapOverflow` | `stat_d_ad_cap_overflow` | |
| `DAdvertisementReport` | `stat_d_advertisement_report` | |
| `DAgreementStat` | `stat_d_agreement_stat` | |
| `DArgsPass` | `stat_d_args_pass` | **`-x kwargs`** |
| `DAutomation` | `stat_d_automation` | |
| `DBigBangReport` | `stat_d_big_bang_report` | |
| `DBigBangRetention` | `stat_d_big_bang_retention` | |
| `DCashFriend` | `stat_d_cash_friend` | **`-x start_dt`** |
| `DCohortGroup` | `stat_d_cohort_group` | |
| `DCounters` | `stat_d_counters` | |
| `DETCReport` | `stat_detc_report` | |
| `DEventStat` | `stat_d_event_stat` | |
| `DFormData` | `stat_d_form_data` | **`--create-hive-table false`** |
| `DFormSchema` | `stat_d_form_schema` | |
| `DGiftfree` | `stat_d_giftfree` | |
| `DInfluxUser` | `stat_d_influx_user` | |
| `DMissionBigBang` | `stat_d_mission_big_bang` | |
| `DPageStat` | `stat_d_page_stat` | |
| `DPushReport` | `stat_d_push_report` | |
| `DRedeemSlack` | `stat_d_redeem_slack` | |
| `DRedeemTicket` | `stat_d_redeem_ticket` | |
| `DSalesReport` | `stat_d_sales_report` | |
| `DSalesStat` | `stat_d_sales_stat` | |
| `DServerState` | `stat_d_server_state` | **`-x kwargs`** |
| `DTermReport` | `stat_d_term_report` | |
| `DTest` | `stat_d_test` | ← 이름상 테스트 컬렉션 |
| `DTodayCash` | `stat_d_today_cash` | |
| `DTodayCashReport` | `stat_d_today_cash_report` | |
| `DTopSalesBySeries` | `stat_d_top_sales_by_series` | |
| `DUserReport` | `stat_d_user_report` | |
| `DUserSearchFile` | `stat_d_user_search_file` | |
| `DWebStat` | `stat_d_web_stat` | |
| `DWeekCummReport` | `stat_d_week_cumm_report` | |
| `DWelcomeUserReport` | `stat_d_welcome_user_report` | |
| `SCashFriend` | `stat_s_cash_friend` | |

### B. `data_0200_dump_hourly` — 1건, `contents` 클러스터 → `page_service_{phase}`

| 컬렉션 | Hive 뷰 |
|---|---|
| `open_log_{YYYYMM}` | `contents_open_log_{YYYYMM}` (테이블 생성 안 함) |

## 4. 특이 케이스 4건

기본 옵션 조합(`-s -e --cluster-id -c --output-dir-timestamp --create-hive-table true`)에서 벗어난 것.

| 태스크 | 벗어난 점 | 추정 이유 |
|---|---|---|
| `dump_mongo_stat_DArgsPass` | `-x kwargs` | `kwargs` 필드가 nested/mixed type 이라 parquet 변환 실패 회피로 추정 |
| `dump_mongo_stat_DServerState` | `-x kwargs` | 동일 |
| `dump_mongo_stat_DCashFriend` | `-x start_dt` | 타입 문제 or 불필요 컬럼 |
| `dump_mongo_stat_DFormData` | `--create-hive-table false` | **[[1_사용중인_spark_job]] 에 누락돼 있던 건.** 나머지 34건은 true |

`-x` 는 Mongo 의 스키마리스 특성상 컬렉션마다 필드 타입이 섞여 있을 때 쓰는 회피책으로 보인다.
**❓ 왜 이 컬럼들만 빼는지 확인 필요** (데이터 이슈 / 개인정보 / 스키마 다형성).

## 5. 후속 파이프라인

`action_dependencies` 기준, 현행 후속 태스크 **33건** (전부 `BashOperator`).

```
MongoDataFrameImporter (37)
   ├─ A 계열 (32) → replace_preto_view_{table}   : Presto v_ 뷰 재생성
   │                 (일부는 run_impala_sql.sh 로 INVALIDATE METADATA / COMPUTE STATS)
   └─ B (1)      → move_mongo_contents_open_log : merge_and_move_dataframes.sh
```

- A 계열 후속이 **Presto 뷰 재생성**이라는 건, 매 실행마다 timestamp 접미 물리 테이블이 새로 생기고
  뷰를 그쪽으로 갈아끼우는 구조라는 뜻. **이관 시 이 "뷰 스위칭" 패턴을 어떻게 대체할지**가 관건.
- B 의 후속 `merge_and_move_dataframes.sh` (= `DataFrameMerger`) 는 **이관 제외 결정된 앱**이다.
  B 를 이관하면 이 후속도 함께 재설계 대상.

## 6. 이관 관점

### 소스 (MongoDB)

- `stat`, `contents` 두 클러스터. Datastream 은 MongoDB 소스를 지원하지만
  **컬렉션명이 매월 바뀌는 B 케이스는 CDC 로 자연스럽게 표현되지 않는다.**
- A 는 매일 전체 스냅샷 → CDC 로 옮기면 효율이 크게 오른다.

### 대상 (Hive → BigQuery)

- 물리 테이블 + Hive 뷰 + Presto `v_` 뷰 **3계층을 BQ 에서 어떻게 표현할지** 결정 필요.
  BQ 는 파티션/스냅샷 개념이 달라서 timestamp 접미 테이블 + 뷰 스위칭이 그대로 필요 없을 수 있다.
- `--create-hive-table false` 인 2건(`DFormData`, `open_log`)은 애초에 테이블이 없으므로 별도 취급.

### 우선순위 제안

1. **A 계열 36건** — 패턴이 동일해 기계적 이관 가능. `-x` 3건만 개별 확인.
2. **B 1건** — 컬렉션 시프트 + 증분 pipeline + 스트리밍과의 중복 경로까지 얽혀 있어 **단독 설계 필요**.

## 7. ❓ 논의 필요

- **B(open_log) 의 존재 이유** — `MongoOpenLogStreamingApp` 이 Kafka → Mongo 로 쓰고,
  이 앱이 다시 Mongo → Hive 로 덤프한다. Kafka 에서 바로 BQ 로 갈 수 있는지.
- `-x` 3건의 제외 사유 (스키마 / 타입 / 개인정보 중 무엇인지)
- `DFormData` 만 `--create-hive-table false` 인 이유
- `DTest` 컬렉션 — 이름상 테스트인데 매일 덤프 중. 폐기 가능한지
- Presto `v_` 뷰의 소비처 (어느 팀/시스템이 읽는지)
- 36개 컬렉션 중 실제로 소비되는 것이 몇 개인지 (덤프만 하고 아무도 안 읽는 게 있을 수 있음)

## 재현

```sql
-- 현행 태스크 37건
SELECT w.name dag, w.schedule_interval, a.uid, a.name, a.kwargs
FROM actions a
  JOIN workflows w ON w.uid = a.workflow_uid
  JOIN dag d ON d.dag_id = w.name
WHERE a.hidden = 0
  AND d.is_paused = 0 AND d.is_active = 1 AND d.next_dagrun IS NOT NULL
  AND a.kwargs LIKE '%run_mongo_dump%';

-- 후속 태스크
SELECT down.name, down.operator_class, down.kwargs
FROM actions a
  JOIN action_dependencies dp ON dp.upstream_action_uid = a.uid
  JOIN actions down ON down.uid = dp.action_uid
WHERE a.kwargs LIKE '%run_mongo_dump%' AND a.hidden = 0 AND down.hidden = 0;
```

Hive 뷰 이름 규칙은 `MongoDataFrameImporter.scala:79` 의 `hiveViewName()` 과
`DataFrameImporter.scala:118,149` 의 `hiveTableName` / `hiveDbName` 참고.
