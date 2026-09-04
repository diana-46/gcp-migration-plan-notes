---
title: "PushTargetUserImporter — 앱 상세"
status: draft
tags:
  - spark-apps
  - gcp이관
  - hudi
  - push
created: 2026-08-31
updated: 2026-08-31
---

# PushTargetUserImporter — 앱 상세

> `com.kakaopage.spark.app.imports.PushTargetUserImporter` · 실행 스크립트 `bin/run_hudi.sh`
> 근거: 프로덕션 `actions` + Airflow `task_instance`(90일) + `action_dependencies` + HDFS 실측 + 코드.
> 관련: [[1_사용중인_spark_job]] · [[5_MongoDataFrameImporter]]

## 한 줄

푸시 발송 그룹(`t_push_group`)에서 **발송 대상 유저 목록 파일을 읽어 펼쳐** `push_group_uid × user_uid` 행으로 적재하는 앱.
현행 **1 태스크 / 1 DAG**, 일 1회, 90일간 **60회 success**.

> ⚠️ [[1_사용중인_spark_job]] 에서는 `run_hudi` 계열이 **이관 검토 대상에서 제외**로 분류돼 있다.
> 다만 아래 §5 의 athlon 의존·외부 HTTP 호출 때문에 재검토 여지가 있다.

## 1. 실행 형태

```bash
run_hudi.sh com.kakaopage.spark.app.imports.PushTargetUserImporter cluster 10 16g 1 \
  -e {{ var.value.phase }} \
  --partition-by-uid-and-type \
  -w "send_start_dt >= '{{ execution_date }}' and send_start_dt < '{{ next_execution_date }}'"
```

| 항목 | 값 |
|---|---|
| DAG | `data_2001_import_push_users` |
| cron | `0 0 * * *` — 일 1회 |
| action uid | 5295 |
| 90일 실행 | 60회 전부 success |
| `--location` | **넘기지 않음** → 기본값 `/page_service/{env}/modeled/push_v2` |
| `--save-mode` | 넘기지 않음 → 기본값 **`append`** |

## 2. 연결된 대상

### 읽기 ①  Hudi — 푸시 그룹 메타

```
/page_service/{env}/raw/mysql/boracay_production/t_push_group/data
```
→ temp view `t_push_group` 으로 등록 후 SQL 조회

```sql
SELECT uid, target_type, target_file, send_status, send_start_dt, send_end_dt
FROM t_push_group
WHERE send_status = 'SUCCESS'
  AND target_type IN ('URL', 'ATHLON')
  AND {where}          -- send_start_dt 시간 윈도우
```

> **경로에 `boracay_production` 이 하드코딩돼 있다.** 바깥은 `$env` 인데 안쪽은 고정이라
> 비프로덕션 환경에서 프로덕션 Hudi 를 보게 된다. (코드 99행)

### 읽기 ②  외부 파일 — **UDF 안에서 가져온다**

`target_type` 에 따라 실제 유저 목록 파일을 읽어온다. 이게 이 앱의 핵심이자 이관 난점이다.

| `target_type` | 소스 | 방식 |
|---|---|---|
| `URL` | `target_file` 값 그대로가 full URL | **`Source.fromURL(...)` — Spark UDF 안에서 HTTP GET** |
| `ATHLON` | `/team/athlon/{env}/run/result/{target_file}` | **athlon 추출 실행 결과 파일**을 HDFS 에서 읽음 |
| `FILE` | — | 이 앱이 처리하지 않음 → `KagePushTargetImporter` 담당 (코드 80행 주석) |

읽어온 각 행을 `explode` → `split(',')` → 첫 컬럼을 `user_uid` 로 캐스팅한다.

### 쓰기

| 저장소 | 대상 |
|---|---|
| HDFS | `/page_service/{env}/modeled/push_v2` — parquet, `partitionBy(push_group_uid, data_type)`, **append** |
| Hive | `page_service_{phase}.push_user` ← 후속 `msck_repair_table_push_user` (uid 5296) |

출력 스키마는 2컬럼뿐이다: `push_group_uid`, `user_uid`. 여기에 `data_type = 'target'` 리터럴이 붙는다.

### 파티션 구조

```scala
// PushTargetUserImporter.scala:141-146
if (config.partitionByUidAndType) {
  df.partitionBy("push_group_uid", "data_type").parquet(location)
} else {
  df.parquet(location)
}
```

액션이 `--partition-by-uid-and-type` 를 넘기므로 **2단 파티션**이다.

```
/page_service/production/modeled/push_v2/
  push_group_uid=505312/
    data_type=recipient/part-....parquet
    data_type=target/part-....parquet
```

**3개 앱이 같은 파티션 구조를 쓴다** — `KagePushTargetImporter` 는 `partitionCols = Seq(UID_COL, DATA_TYPE)`,
`PageKappusPushLogStreamingApp` 은 `partitionBy(LIT_COL_PUSH_GROUP_UID, LIT_COL_DATA_TYPE)`.

출력 컬럼 3개 중 2개가 파티션 키이므로 **실제 데이터 컬럼은 `user_uid` 하나뿐**이다.

## 3. ⚠️ location 불일치

| 테이블 | Location |
|---|---|
| `push_user_v2` | `/page_service/production/modeled/push_v2` ← **이 앱이 쓰는 곳** |
| `push_user` | `/page_service/production/modeled/push` ← **후속 MSCK 가 repair 하는 곳** |

`--location` 을 안 넘겨서 기본값 `push_v2` 에 쓰는데, 같은 DAG 의 후속 태스크는 `push_user`(→`/modeled/push`)를 repair 한다.
**앱의 기본 location 이 `push` → `push_v2` 로 바뀐 시점에 DAG 의 MSCK 를 안 고친 것**으로 보인다.

`/modeled/push` 디렉토리에 데이터는 존재한다. athlon 액션 중 이 경로를 쓰는 것은 없다.
athlon 밖에서 쓰고 있을 수 있어 **"동작하는 것"으로 두고 이관 시 재확인**하기로 했다.

> ❓ `/modeled/push` 의 실제 writer 와 `push_user` 테이블 소비처 확인 필요.

## 4. `push_v2` 를 쓰는 앱이 3개다 — `data_type` × `target_type` 으로 분담

같은 경로 `/page_service/{env}/modeled/push_v2` 에 셋이 쓴다. 충돌이 아니라 역할 분담이다.

| 앱 | `data_type` | 담당 `target_type` | 쓰기 모드 | 주기 |
|---|---|---|---|---|
| **PushTargetUserImporter** | `target` | **`URL`, `ATHLON`** | `append` | 일 1회 (`data_2001`) |
| `imports.kage.KagePushTargetImporter` | `target` | **`FILE`** | `SaveMode.Overwrite` (dynamic) | 시간별 (`data_3200`) |
| `kafka.PageKappusPushLogStreamingApp` | `recipient` | — (발송 로그) | 스트리밍 append | 상주 |

두 Importer 가 **같은 `t_push_group` 테이블을 `target_type` 으로 나눠 읽는다**:

```sql
-- PushTargetUserImporter
WHERE send_status = 'SUCCESS' AND target_type IN ('URL','ATHLON') AND {where}

-- KagePushTargetImporter
WHERE target_type = 'FILE' AND {where}
```

둘 다 `-w send_start_dt >= ... AND < ...` 시간 윈도우를 받는다.

> **`data_type=target` 파티션을 둘이 공유하지만 `target_type` 이 배타적이라 같은 `push_group_uid` 를 다룰 일은 없어 보인다.**
> 다만 Kage 쪽이 `SaveMode.Overwrite` + `partitionOverwriteMode=dynamic` 이라,
> 만에 하나 겹치면 append 결과를 덮어쓴다. ❓ 의도 확인 권장.

HDFS 실측에서도 두 `data_type` 이 확인된다 (`push_group_uid=505312` 예시):
```
data_type=recipient/  part-...  (15:50, 17:05 — 스트리밍 append 2개)
data_type=target/     part-...  (18:01 — 배치 1개)
```
전체 파티션 38,721개.

## 5. 이관 관점 — 난점이 네 가지

### ① Spark UDF 안에서 HTTP 호출을 한다

```scala
case "URL" => Source.fromURL(targetFile)   // executor 에서 외부 HTTP GET
```

- executor 마다 외부로 나가는 네트워크 호출이 발생한다. GKE/Dataproc 으로 옮기면 **egress 경로·방화벽·서비스 계정** 재설계가 필요하다.
- 재시도·타임아웃 처리가 코드에 없다. 대상 서버가 느리면 태스크가 그대로 매달린다.
- BigQuery 로는 표현할 수 없는 동작이다. **SQL 로 대체 불가.**

### ② athlon 추출 결과에 의존한다 — **GCS 로 확정** ✅

```
현재 : /team/athlon/{env}/run/result/{target_file}   (HDFS)
GCP  : GCS                                            (팀 확정)
```

`target_type='ATHLON'` 이면 **athlon 추출 실행 산출물**을 읽는다.
**athlon 추출 결과는 GCP 에서 GCS 로 간다**는 방향이 정해졌으므로 이 의존은 해소된다.

- Spark 을 유지하면 `gs://` 경로로 바꾸기만 하면 된다 (gcs-connector 필요, 코드 변경 없음)
- Spark 을 벗어나면 오히려 더 쉽다 — GCS 객체 읽기는 Python 클라이언트로 몇 줄이다

> ❓ 남은 확인: 파일 **형식·인코딩이 그대로 유지**되는지 (현재는 CSV, 첫 줄 헤더를 `drop(1)` 로 건너뛴다),
> 그리고 경로 규칙(`{bucket}/{prefix}/{run_id}/...`)이 어떻게 되는지

### ③ 소스가 Hudi 다

`t_push_group` 을 Hudi 로 읽는다. mandu/Debezium CDC 파이프라인 산출물이며
Datastream + BQ 로 대체되면 이 읽기 방식도 함께 바뀐다.

### ④ 파티션 컬럼을 BigQuery 로 그대로 옮길 수 없다

`push_group_uid` 는 **고카디널리티 ID** 다. HDFS 실측:

```
$ hdfs dfs -ls /page_service/production/modeled/push_v2 | head
Found 38721 items
```

`push_group_uid` 파티션이 **약 38,700 개**, `data_type` 까지 곱하면 **7만 개 이상**이다.

**BigQuery 는 테이블당 파티션 4,000 개 제한이 있어 `push_group_uid` 로는 파티셔닝이 불가능하다.**
그대로 옮기려 하면 막힌다.

| 컬럼 | 현재 (Hive) | BigQuery 권장 |
|---|---|---|
| `push_group_uid` | 파티션 (~38,700) | **클러스터링 키** (카디널리티 제한 없음, 최대 4 컬럼) |
| `data_type` | 파티션 (2종) | 클러스터링 키 또는 일반 컬럼 |
| — | — | 파티션은 **날짜 기반으로 새로 설계** (`send_start_dt` 또는 적재일) |

조회 패턴이 "특정 푸시 그룹의 대상자 조회"라면 클러스터링으로 충분히 커버된다.

> **부작용 관찰** — 파티션 7만 개면 Hive metastore 부담이 크고 파티션당 파일이 매우 작다.
> 실측 샘플에서 5.7 KB / 510 B 짜리 파일이 확인된다. 전형적인 소파일 문제다.
> 이관 시 클러스터링으로 바꾸면 이 문제도 함께 해소된다.
>
> ❗ 이 파티션 구조는 3개 앱이 공유하므로, 바꾸려면 **세 앱을 함께 재설계**해야 한다.

### 이관 옵션

athlon 산출물이 GCS 로 간다는 게 정해지면서(§5-②) **난점 4개 중 1개가 해소**됐다.
남은 것은 ① HTTP fetch, ③ Hudi 소스, ④ 파티션 구조.

| 옵션 | 방식 | 평가 |
|---|---|---|
| **A. Composer + Python** | GCS/HTTP 파일 fetch·파싱을 Python 으로, 결과만 BQ 적재 | **유력.** 연산이 "파일 읽어 CSV 첫 컬럼 뽑기"뿐이다. GCS 읽기는 Spark 보다 Python 이 오히려 간단하다 |
| B. Dataproc lift | Spark 그대로 (`gs://` 경로로만 변경) | 변경 최소. 규모가 크면 유리 |
| C. BQ SQL 재구현 | — | **불가.** 외부 URL fetch 를 SQL 로 표현할 수 없다 |

**A 와 B 의 갈림길은 데이터 규모다.**
Spark 이 실제로 하는 일은 병렬 fetch + parquet write 정도이고,
출력은 `push_group_uid`, `user_uid` 2컬럼뿐이다.

> ❓ 판단에 필요한 수치: **일별 처리하는 `t_push_group` 행 수** / 그룹당 평균 target 파일 크기
> ([[6_AgeGenderCategorizingImporter]] 에서 `t_user` 3,227만 행 / 5분 을 재서 판단한 것과 같은 방식)

## 6. 코드 메모

```scala
/***
 * NOTE: 푸시 앱이 v1(python 기반)에서 v2(java 기반, kappus)로 바뀌면서
 *       관련 테이블 참조 수정이 있고 그에 기반한 코드 수정이 있음 (2025.07.03)
 * - v1: t_push_noti
 * - v2: t_push_group (2025.06.23 이후)
 */
```

- 소스 테이블이 `t_push_noti` → `t_push_group` 으로 교체됐다 (2025-06).
  같은 시기에 `PageKappusPushLogStreamingApp` 이 생겼고 `PagePushLogStreamingApp` 이 폐기됐다.
  **푸시 v1→v2(kappus) 전환의 일부**다.
- `--include-series-id` 옵션은 deprecated 처리돼 주석으로 남아 있다 (41~45행).
- `data_2001_import_push_users` 안에 **이전 세대 액션들이 hidden=1 로 남아 있다** —
  `extract_push_recipients`(`PushRecipientsImporter`, 1세대),
  `copy_push_target_user_from_cdh`(`adhoc/page_service_push_target.sh`, CDH→KHP 이관용 2세대).
  둘 다 이미 폐기 확정.

## 7. ❓ 논의 필요

- ~~athlon 추출 결과의 GCP 이관 방향~~ → **GCS 로 확정** (§5-②).
  남은 확인: 파일 형식·인코딩 유지 여부, GCS 경로 규칙
- `target_type='URL'` 의 대상 서버가 어디인지 (사내/외부, egress 정책) — **남은 난점 중 최대**
- **일별 처리 규모** — Composer+Python(A) vs Dataproc lift(B) 판단 근거
- `/modeled/push` 의 실제 writer 와 `push_user` 테이블 소비처 (§3)
- `push_v2` `data_type=target` 을 두 앱이 공유하는 것이 의도인지 (§4)
- `boracay_production` 하드코딩이 의도인지 (§2)
- **파티션 → 클러스터링 전환** (§5-④) — `push_group_uid` 는 BQ 파티션 불가. 3개 앱 공동 재설계 필요
- `push_user_v2` 의 **조회 패턴** — 특정 push_group_uid 단건 조회인지, 기간 스캔인지 (클러스터링 설계 근거)
- 일별 처리 규모 — Spark 이 필요한 수준인지 판단용 (§5)
- `push_user_v2` 최종 소비처 (푸시 발송 시스템?)

## 재현

```sql
-- 현행 액션
SELECT a.uid, w.name dag, w.schedule_interval, a.name, a.kwargs
FROM actions a
  JOIN workflows w ON w.uid = a.workflow_uid
  JOIN dag d ON d.dag_id = w.name
WHERE a.hidden = 0
  AND d.is_paused = 0 AND d.is_active = 1 AND d.next_dagrun IS NOT NULL
  AND a.kwargs LIKE '%PushTargetUserImporter%';

-- 같은 DAG 전체 (hidden 포함 — 세대 잔재 확인용)
SELECT a.uid, a.name, a.operator_class, a.hidden, a.kwargs
FROM actions a JOIN workflows w ON w.uid = a.workflow_uid
WHERE w.name = 'data_2001_import_push_users' ORDER BY a.uid;

-- push_v2 를 쓰는 앱 전부
SELECT uid, name, kwargs FROM actions
WHERE kwargs LIKE '%PushTargetUserImporter%'
   OR kwargs LIKE '%run_kage_push_target_importer%';
```

```bash
# HDFS 파티션 구조 확인
hdfs dfs -ls /page_service/production/modeled/push_v2 | head
hdfs dfs -ls -R /page_service/production/modeled/push_v2/push_group_uid=<uid>
```

코드: `PushTargetUserImporter.scala` — 외부 파일 fetch(78~96행), Hudi read(99행),
`data_type` 리터럴·partitionBy(139~146행), 기본 location(131행).
