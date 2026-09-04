---
title: "neptune — 이관 대상 DAG Operator 인벤토리"
status: draft
tags:
  - neptune
  - gcp이관
  - airflow
  - operator
created: 2026-08-13
updated: 2026-08-13
---

# neptune — 이관 대상 DAG Operator 인벤토리

> **출처**: localhost:3306 `neptune` DB (개발환경 덤프) / `workflows`, `actions`, `action_dependencies`, `etl`, `actions_meta`, `etl_mapping`, `actions_meta_mapping`
> **코드 근거**: `athlon/api/.../service/neptune/`, `core/.../model/Actions.kt`
> neptune은 Airflow DAG를 **파일로 쓰지 않는다**. DAG 파일은 3줄 스텁(`airflow_dag('name')`)이고 태스크는 전부 이 DB의 `actions` 행이다. 따라서 **이 표가 곧 이관해야 할 태스크 전량**이다.

---

## 1. 이관 대상 DAG

| DAG (`workflows.name`) | uid | type | schedule_interval | start_date | 액션 수 | 의존성 엣지 |
|---|---|---|---|---|---|---|
| `berriz_0101_bizberry_hourly` | 550 | NONE | `0 * * * *` | 2025-09-15 | 102 | 104 |
| `berriz_0112_service_ranking_daily` | 559 | NONE | `0 0 * * *` | 2026-02-11 | 36 | 47 |
| `berriz_0121_service_popular_posts_hourly` | 561 | NONE | `0 0,12 * * *` | 2026-05-20 | 12 | 12 |
| **합계** | | | | | **150** | **163** |

- 세 DAG 모두 `type=NONE`(ETL 전용 DAG가 아님) → `start_etl`이 `TimeDeltaSensor` 대신 **`DummyOperator`**로 생성됨 (`EtlTaskStart.kt`).
- `hidden=1`인 액션은 **0건**. 전부 활성.

---

## 2. Operator 총괄 (이관에 필요한 오퍼레이터 전량)

| Operator | 총 개수 | 생성 주체 | 정체 | GCP 이관 시 대응 |
|---|---|---|---|---|
| `BashOperator` | 86 | ETL(1세대) | Presto/Spark 셸 호출 → **3종으로 세분화, §3 참조** | 종류별로 다름 |
| `DummyOperator` | 12 | ETL(1세대) | `start_etl` 진입점 | `EmptyOperator` (Airflow 3 개명) |
| `HiveOperator` | 12 | ETL(1세대) | `add_partition` — `ALTER TABLE … ADD PARTITION … LOCATION` | **BigQuery 전환 시 소멸** (파티션 개념 대체) |
| `SlackWebhookOperator` | 12 | ETL(1세대) | `end_etl` 완료 알림 | 그대로 이식 가능 |
| `LoupeKafkaBatchOperator` | 12 | ActionsMeta(2세대) | 쿼리 결과 → Kafka 적재 (사내 커스텀) | 커스텀 오퍼레이터 이식 필요 |
| `LoupeSignalHttpOperator` | 12 | ActionsMeta(2세대) | Loupe admin API 로 revision 시그널 | 커스텀 오퍼레이터 이식 필요 |
| `AthlonQuerySensor` | 2 | ActionsMeta(2세대) | Presto 쿼리 결과로 조건 대기 | 커스텀 센서 이식 필요 |
| `SignalProduceOperator` | 2 | ⚠️ **UNMANAGED** | Kafka 토픽에 콜백 시그널 발행 | 커스텀 이식 + **정의 수기 확보 필요** |
| `ShortCircuitOperator` | 1 | ⚠️ **UNMANAGED** | xcom `total_count > 0` 이면 계속 | Airflow 표준, 그대로 이식 |

> ⚠️ **UNMANAGED 3건**은 `etl_mapping` / `actions_meta_mapping` 어디에도 없다. athlon UI로 만든 게 아니라 **DE가 DB에 직접 넣은 액션**이다. athlon이 재생성해줄 수 없으므로 이관 시 **수기로 옮겨야 하는 유일한 항목**. 또한 `OperatorClassType` enum(`Actions.kt:31`)에도 없다 → athlon 코드로는 애초에 생성 불가한 값.

---

## 3. BashOperator 유형 분류 (86건)

`kwargs.bash_command` 내용으로 분류. **미분류 0건** — 전부 1세대 ETL 파이프라인이 자동 생성한 것이며 수동 편집된 셸 명령은 없다.

| 유형 | 개수 | 실체 | 호출 대상 | 이관 난이도 |
|---|---|---|---|---|
| **① Presto CTAS** (`create_temp_table` / `create_table`) | 38 | `run_presto_sql_khp.sh UTC hadoop_kentdev <schema> "DROP…; CREATE TABLE … WITH (format='PARQUET') AS <사용자SQL>"` | `{{ var.value.spark_apps_bin }}/run_presto_sql_khp.sh` | **높음** — 사용자 SQL 본문이 여기 들어있음. BigQuery SQL 로 번역 대상 |
| **② Presto DROP** (`cleanup_temp_table` / `cleanup_and_compute_stats`) | 38 | `run_presto_sql_khp.sh … "DROP TABLE IF EXISTS <temp>;"` | 동일 스크립트 | **낮음** — BQ 전환 시 대부분 소멸 |
| **③ Spark merge&move** (`merge_and_move`) | 10 | `merge_and_move_dataframes.sh --file-nums 1 --filename-prefix … --src-dirs /team/…/hive/db/… --dst-dirs /team/…/neptune/integration/… --create-hive-table false --exit-empty-dir true` | `{{ var.value.spark_apps_bin }}/merge_and_move_dataframes.sh` | **높음** — HDFS 파일 이동 개념. GCS/BQ 로는 1:1 대응 없음 |

**DAG별 분포**

| DAG | ① CTAS | ② DROP | ③ merge&move | 소계 |
|---|---|---|---|---|
| `berriz_0101_bizberry_hourly` | 21 | 21 | 10 | 52 |
| `berriz_0112_service_ranking_daily` | 14 | 14 | 1 | 29 |
| `berriz_0121_service_popular_posts_hourly` | 2 | 2 | 1 | 5 |

---

## 4. DAG별 스테이지 상세

### 4-1. `berriz_0101_bizberry_hourly` (uid 550) — ETL 10개 × 표준 파이프라인

| 스테이지 | Operator | 개수 |
|---|---|---|
| 01 `start_etl` | DummyOperator | 10 |
| 02 `create_temp_table` | BashOperator ① | 11 |
| 03 `create_table` (perm) | BashOperator ① | 10 |
| 04 `merge_and_move` | BashOperator ③ | 10 |
| 05 `add_partition` | HiveOperator | 10 |
| 06 `cleanup_temp_table` | BashOperator ② | 11 |
| 07 `cleanup_and_compute_stats` | BashOperator ② | 10 |
| 08 `end_etl` | SlackWebhookOperator | 10 |
| 09 `loupe_query_to_kafka` | LoupeKafkaBatchOperator | 10 |
| 10 `loupe_signal_http` | LoupeSignalHttpOperator | 10 |

등록된 ETL 10건 (전부 `PRESTO` / dest_db `datawarehouse_berriz`(TEAM) / `PARQUET`):

| etl_id | unique_title | code_format | dest_table |
|---|---|---|---|
| 157 | `bizberry_community_contents_notice` | PLAIN | `bizberry_community_contents_notice` |
| 158 | `bizberry_community_contents_media` | PLAIN | `bizberry_community_contents_media` |
| 159 | `bizberry_community_contents_live` | PLAIN | `bizberry_community_contents_live` |
| 161 | `bizberry_community_overview` | **YAML** | `bizberry_community_overview` |
| 163 | `bizberry_community_fanclub_overview` | PLAIN | `bizberry_community_fanclub_overview` |
| 164 | `bizberry_community_fanclub_trend` | PLAIN | `bizberry_community_fanclub_trend` |
| 165 | `bizberry_community_overview_trend` | **YAML** | `bizberry_community_overview_trend` |
| 175 | `bizberry_community_contents_artistpost` | PLAIN | `bizberry_community_contents_artistpost` |
| 176 | `bizberry_community_contents_userpost` | PLAIN | `bizberry_community_contents_userpost` |
| 181 | `bizberry_community_contents_summary` | **YAML** | `bizberry_community_contents_summary` |

**temp 테이블 보유 ETL** (YAML 3건만 보유, PLAIN 7건은 0개 → 합 11개 = `create_temp_table` 11개와 일치):

| etl_id | unique_title | temp 개수 | temp 이름 |
|---|---|---|---|
| 181 | `bizberry_community_contents_summary` | 5 | `temp_artistpost`, `temp_userpost`, `temp_media`, `temp_live`, `temp_notice` |
| 161 | `bizberry_community_overview` | 3 | `temp_fanclub_product`, `temp_country`, `temp_overview_transform` |
| 165 | `bizberry_community_overview_trend` | 3 | `temp_fanclub_product_trend`, `temp_country_trend`, `temp_overview_trend_transform` |

CTAS 21개 = temp 11 + perm 10 ✓

### 4-2. `berriz_0112_service_ranking_daily` (uid 559) — ETL 1개(temp 13개) + 센서 + 수동 콜백

| 스테이지 | Operator | 개수 |
|---|---|---|
| 01 `start_etl` | DummyOperator | 1 |
| 02 `create_temp_table` | BashOperator ① | **13** |
| 03 `create_table` (perm) | BashOperator ① | 1 |
| 04 `merge_and_move` | BashOperator ③ | 1 |
| 05 `add_partition` | HiveOperator | 1 |
| 06 `cleanup_temp_table` | BashOperator ② | **13** |
| 07 `cleanup_and_compute_stats` | BashOperator ② | 1 |
| 08 `end_etl` | SlackWebhookOperator | 1 |
| 09 `loupe_query_to_kafka` | LoupeKafkaBatchOperator | 1 |
| 10 `loupe_signal_http` | LoupeSignalHttpOperator | 1 |
| 11 `athlon_query_sensor` | AthlonQuerySensor | 1 |
| — `callback_api_berriz_recommend_contents_rank` | ⚠️ SignalProduceOperator | 1 |

> ⚠️ **`etl` 행이 덤프에 없다.** `etl_mapping`은 `etl_id=205`(→ 액션명으로 보아 `berriz_service_recommend_contents_rank`)를 가리키지만 `etl` 테이블에 해당 행이 없음 = **끊긴 매핑**.
> **영향**: ETL 메타(code_format, dest_db, 파티션 키, 파라미터)를 `etl`에서 읽을 수 없다. 다만 **Presto SQL 본문은 actions.kwargs 에 그대로 남아 있어 복구 가능**하다. 13단 temp 체인이라 이 DAG가 SQL 번역 난이도 최상.

### 4-3. `berriz_0121_service_popular_posts_hourly` (uid 561) — ETL 1개(YAML, temp 1개)

| 스테이지 | Operator | 개수 |
|---|---|---|
| 01 `start_etl` | DummyOperator | 1 |
| 02 `create_temp_table` | BashOperator ① | 1 |
| 03 `create_table` (perm) | BashOperator ① | 1 |
| 04 `merge_and_move` | BashOperator ③ | 1 |
| 05 `add_partition` | HiveOperator | 1 |
| 06 `cleanup_temp_table` | BashOperator ② | 1 |
| 07 `cleanup_and_compute_stats` | BashOperator ② | 1 |
| 08 `end_etl` | SlackWebhookOperator | 1 |
| 09 `loupe_query_to_kafka` | LoupeKafkaBatchOperator | 1 |
| 10 `loupe_signal_http` | LoupeSignalHttpOperator | 1 |
| 11 `athlon_query_sensor` | AthlonQuerySensor | 1 |
| — `check_popular_posts_zero_records` | ⚠️ ShortCircuitOperator | 1 |
| — `callback_berriz_popular_posts` | ⚠️ SignalProduceOperator | 1 |

ETL: `etl_id=216` / `berriz_service_popular_posts` / **YAML** / PRESTO / `datawarehouse_berriz`(TEAM) / `PARQUET` / dest_table `service_popular_posts`

---

## 5. Operator별 kwargs 스펙 (이관 시 매핑해야 할 필드)

| Operator | kwargs 키 | 실제 값 예시 |
|---|---|---|
| `DummyOperator` | (없음) | `{}` |
| `BashOperator` | `bash_command` | §3 참조 |
| `HiveOperator` | `hive_cli_conn_id`, `hql` | `beeline_default` / `ALTER TABLE datawarehouse_berriz.<t>_integration ADD IF NOT EXISTS PARTITION (create_date='{{ … }}') LOCATION "/team/datawarehouse_berriz/neptune/integration/<t>/create_date={{ … }}"` |
| `SlackWebhookOperator` | `failure_callback_slack_alert_channel`, `message` | `#airflow-pre-release-noti-dev` / `"157번 … ETL 작업이 완료되었습니다."` |
| `LoupeKafkaBatchOperator` | `broker`, `topic`, `model_name`, `query`, `partition_field`, `additional_meta` | `dp-kafka-dev.kakaodev.io:9093` / `svc-loupe-v2-integ.bizberry.loupe-data` / `{"primaryKeys":"community_id,community_notice_id"}` |
| `LoupeSignalHttpOperator` | `queue`, `http_conn_id`, `token_variable_name`, `tenant_id`, `model_name`, `revision`, `total_count` | `http` / `loupe_admin_api` / `loupe_admin_token` / `2` / xcom pull 표현식 |
| `AthlonQuerySensor` | `retries`, `timeout`, `poke_interval`, `sql`, `conn_id`, `condition_expression` | `1` / `1800` / `60.0` / `presto_sensor` / `x and x == 'ACTIVE'` |
| `SignalProduceOperator` ⚠️ | `key`, `topic`, `value_data`, `kafka_config_id` | `berriz_popular_posts` / `svc-fanplatform-community-popular-posts-qa-updated` / `dp-kafka-dev` |
| `ShortCircuitOperator` ⚠️ | `condition_check`, `xcom_key`, `condition_expression` | `xcom_check` / `total_count` / `int(x) > 0` |

---

## 6. 외부 의존성 인벤토리 (이관 시 같이 옮겨야 할 것)

| 종류 | 값 | 용도 | GCP 대응 |
|---|---|---|---|
| Airflow Variable | `spark_apps_bin` | 셸 스크립트 경로 prefix | 스크립트 자체가 소멸 대상 |
| 셸 스크립트 | `run_presto_sql_khp.sh` | Presto SQL 실행 (catalog `hadoop_kentdev`) | BigQuery 실행으로 대체 |
| 셸 스크립트 | `merge_and_move_dataframes.sh` | HDFS 파일 병합/이동 | 대응 없음 — 재설계 필요 |
| Hive conn | `beeline_default` | `add_partition` | 소멸 |
| Presto conn | `presto_sensor` | AthlonQuerySensor | BQ 센서로 대체 |
| HTTP conn | `loupe_admin_api` | Loupe revision 시그널 | 그대로 |
| Airflow Variable | `loupe_admin_token` | 위 인증 토큰 | Secret Manager |
| Kafka broker | `dp-kafka-dev.kakaodev.io:9093` | Loupe 데이터 적재 | Kafka 유지 or Pub/Sub |
| Kafka config id | `dp-kafka-dev` | SignalProduceOperator | 동일 |
| Slack 채널 | `#airflow-pre-release-noti-dev`, `#airflow-noti-qa`, `#airflow-noti-sandbox` | 알림 | 그대로 |
| HDFS 경로 | `/team/datawarehouse_berriz/hive/db/…`(temp), `/team/datawarehouse_berriz/neptune/integration/…`(perm) | 적재 위치 | GCS |

---

## 7. 참조 소스 스키마 (CTAS SQL 에서 추출, 참조 횟수)

BigQuery 로 옮길 때 **원천이 먼저 준비돼야 하는 스키마**들.

| 스키마 | 참조 횟수 |
|---|---|
| `berriz_ods_service_qa` | 73 |
| `berriz_ods_account_qa` | 12 |
| `berriz_ods_comment_qa` | 10 |
| `datawarehouse_berriz` (자기 참조/중간산출) | 9 |
| `berriz_identified_qa` | 8 |
| `berriz_ods_personal_qa` | 4 |
| `berriz_non_identified_qa` | 1 |
| `berriz_ods_commerce_qa` | 1 |
| `gsheet_dev` | 1 |

> 개발환경 덤프라 스키마명에 `_qa` / `_dev` 접미사가 붙어 있다. 프로덕션 이관 시 실제 스키마명 재확인 필요.

---

## 8. Pool 사용 현황

| DAG | default_pool | presto_query_small | sensor_pool |
|---|---|---|---|
| `berriz_0101_bizberry_hourly` | 81 | 21 | – |
| `berriz_0112_service_ranking_daily` | 21 | 14 | 1 |
| `berriz_0121_service_popular_posts_hourly` | 10 | 2 | 1 |

`presto_query_small` 은 CTAS(`create_*_table`) 액션에만 붙는다 (`EtlContext.pool` ← `etl.slot`). BigQuery 전환 시 Presto 동시성 제어 목적이 사라지므로 pool 재설계 대상.

---

## 9. 이관 관점 요약

**소멸하는 것 (BigQuery 전환 시 그냥 없어짐)** — 150개 중 **72개(48%)**
- BashOperator ② Presto DROP 38
- BashOperator ③ merge&move 10
- HiveOperator `add_partition` 12
- DummyOperator `start_etl` 12 (→ 구조상 EmptyOperator 로 남길 수도)

**진짜 번역해야 하는 것**
- BashOperator ① Presto CTAS **38개** ← 사용자 SQL 본문. **이관 작업량의 핵심**
- 그중 `berriz_0112` 의 13단 temp 체인이 최난도

**커스텀 오퍼레이터 이식 (4종)**
- `LoupeKafkaBatchOperator`, `LoupeSignalHttpOperator`, `AthlonQuerySensor`, `SignalProduceOperator`

**수기 확보 필요 (athlon 재생성 불가)**
- UNMANAGED 3건 (`SignalProduceOperator` ×2, `ShortCircuitOperator` ×1)
- `etl_id=205` (0112) 의 ETL 메타 — SQL 은 actions 에서 복구 가능하나 파티션 키/파라미터 정의는 별도 확인 필요

---

## 재현 쿼리

```sql
-- 대상 DAG
SELECT uid, name, type, schedule_interval FROM workflows
WHERE name IN ('berriz_0101_bizberry_hourly','berriz_0112_service_ranking_daily','berriz_0121_service_popular_posts_hourly');
-- uid = 550, 559, 561

-- Operator 집계 + 생성 주체
SELECT w.name dag,
  CASE WHEN em.action_uid IS NOT NULL THEN 'ETL(1세대)'
       WHEN mm.action_uid IS NOT NULL THEN 'ActionsMeta(2세대)'
       ELSE 'UNMANAGED' END origin,
  a.operator_class, COUNT(*) cnt
FROM actions a
  JOIN workflows w ON w.uid = a.workflow_uid
  LEFT JOIN etl_mapping em ON em.action_uid = a.uid
  LEFT JOIN actions_meta_mapping mm ON mm.action_uid = a.uid
WHERE w.uid IN (550,559,561)
GROUP BY 1,2,3 ORDER BY 1,2,4 DESC;

-- BashOperator 유형 분류
SELECT w.name dag,
  CASE
    WHEN a.kwargs LIKE '%run_presto_sql_khp.sh%' AND a.kwargs LIKE '%CREATE TABLE%' THEN '1.presto CTAS'
    WHEN a.kwargs LIKE '%run_presto_sql_khp.sh%'                                    THEN '2.presto DROP'
    WHEN a.kwargs LIKE '%merge_and_move_dataframes.sh%'                             THEN '3.spark merge_and_move'
    WHEN a.kwargs LIKE '%bq load%'                                                  THEN '4.bq load'
    WHEN a.kwargs LIKE '%gcloud storage cp%'                                        THEN '5.gcs upload'
    ELSE '9.OTHER' END kind,
  COUNT(*) cnt
FROM actions a JOIN workflows w ON w.uid = a.workflow_uid
WHERE w.uid IN (550,559,561) AND a.operator_class = 'BashOperator'
GROUP BY 1,2 ORDER BY 1,2;
```
