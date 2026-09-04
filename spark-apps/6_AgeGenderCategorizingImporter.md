---
title: "AgeGenderCategorizingImporter — 앱 상세"
status: draft
tags:
  - spark-apps
  - gcp이관
  - mysql
  - pii
created: 2026-08-31
updated: 2026-08-31
---

# AgeGenderCategorizingImporter — 앱 상세

> `com.kakaopage.spark.app.imports.AgeGenderCategorizingImporter` · 실행 경로 `bin/run.sh` (클래스 직접 지정)
> [[1_사용중인_spark_job]] 의 #3 을 앱 단위로 파고든 문서.
> 근거: 프로덕션 `actions` + Airflow `task_instance`(90일) + `action_dependencies` + 코드.

## 한 줄

MySQL `user` DB 에서 사용자 나이·성별을 읽어 **구간으로 범주화한 결과만** HDFS 로 내리는 ETL.
**이름은 Importer 지만 실제로는 dump 가 아니라 변환 작업**이다.

현행 **1 태스크 / 1 DAG**, 주 1회(월요일), 90일간 **8회 전부 success** (최근 2026-08-16).

## 1. 실행 형태

```bash
run.sh com.kakaopage.spark.app.imports.AgeGenderCategorizingImporter cluster 10 4G 1 \
  -s page_user -e {{ var.value.phase }} \
  --num-partitions 200 \
  --output-dir /team/kakaopage_c1/{phase}/categorized_age_gender/create_date={YYYYMMDD} \
  --query-option new_global
```

| 항목 | 값 |
|---|---|
| DAG | `data_2000_categorize_age_gender` |
| cron | `0 0 * * 1` — **주 1회 월요일 00:00** |
| pool | `mysql_userdb_small` |
| action uid | 3962 |
| `t_user` row 수 | **32,274,771** (약 3,227만) |
| 현행 소요 시간 | **약 5분** |
| 산출물 추정 크기 | **CSV 약 1.26 GB** (행당 ~39 B × 3,227만) |

## 2. 연결된 테이블

### 읽기 — MySQL (`page_user` 서비스 / `user` DB / **slave**)

`dbName = "user"` 는 코드에 하드코딩돼 있다 (`AgeGenderCategorizingImporter.scala:193`).

| 테이블 | 용도 |
|---|---|
| `t_user` | 기준 테이블 (`uid`, `account_id`) |
| `t_user_auth_info` | `gender`, `str_birthday` |
| `user_private_info` | 글로벌 연령정보 (`birthday`) — `new_global` 에서만 LEFT JOIN |

### 쓰기

| 저장소 | 대상 |
|---|---|
| HDFS | `/team/kakaopage_c1/{phase}/categorized_age_gender/create_date={YYYYMMDD}` (parquet) |
| Hive | `kakaopage_c1.categorized_age_gender_weekly` — 앱이 아니라 **후속 `add_partition` 태스크**가 등록 |

표준 데이터 레이크가 아니라 **팀 소유 디렉토리**(`/team/kakaopage_c1/`)에 쓴다.

### 후속 파이프라인

```
3962 dump_categorize_age_gender          (BashOperator, hidden=0)
  └─ 3963 add_partition_to_categorized_age_gender   (HiveOperator, hidden=0)
       ALTER TABLE kakaopage_c1.categorized_age_gender_weekly ADD IF NOT EXISTS PARTITION ...

  ※ 8316 compute_stats / 8324 invalidate_metadata 는 hidden=1 (Impala 계열, 사용 중지)
```

## 3. 실제 SQL — 전부 순수 MySQL 이다

`--query-option new_global` 이 프로덕션에서 쓰는 유일한 쿼리.

```sql
SELECT ta.uid AS user_uid
     , CASE WHEN ta.gender = 1 THEN '남'
            WHEN ta.gender = 2 THEN '여'
            WHEN ta.gender IS NULL THEN 'NONE' END gender
     , CASE WHEN ta.age < 15 THEN '15세 미만'
            WHEN ta.age >= 15 AND ta.age < 20 THEN '20세 미만'
            ...  -- 5세 단위 10구간
            WHEN ta.age >= 60 THEN '60세 이상' END age
     , CASE WHEN ta.age >= 19 THEN '19세 이상'
            WHEN ta.age >= 18 THEN '18세 이상'
            ELSE '미성년' END adult_flag
FROM (
    SELECT tu.uid, tuai.gender
         , TIMESTAMPDIFF(YEAR, CONVERT(COALESCE(tuai.str_birthday, upi.birthday), DATE), CURRENT_DATE) AS age
    FROM t_user tu
    LEFT JOIN t_user_auth_info tuai ON tu.account_id = tuai.account_id
    LEFT JOIN user_private_info  upi  ON tu.uid = upi.user_uid
    WHERE tuai.account_id IS NOT NULL OR upi.user_uid IS NOT NULL
) ta
```

**Spark 고유 기능이 하나도 없다.** `CASE WHEN` / `TIMESTAMPDIFF` / `CONVERT` / `COALESCE` / `LEFT JOIN` 뿐.
Spark 은 여기서 **① 병렬 JDBC read ② parquet write** 두 가지 용도로만 쓰인다.

- `SELECT MIN(uid), MAX(uid) FROM t_user` 로 범위를 구해 `--num-partitions 200` 으로 분할 read
- slave 커넥션 사용

> **실효 병렬도는 200 이 아니라 10 이다.**
> `run.sh ... cluster 10 4G 1` → `maxExecutors=10`, `executor-cores=1`.
> 파티션을 200 개로 쪼개지만 동시 MySQL 커넥션은 10 개고, 200 개를 10 개씩 20 회에 나눠 처리한다.
> 이관 시 재현해야 할 병렬도는 **200 이 아니라 10** 이다.

코드에 쿼리가 3종(`old` / `new` / `new_global`) 있지만 **프로덕션은 `new_global` 만** 쓴다.
- `old` : 한국 나이 (`YEAR(CURRENT) - YEAR(birthday) + 1`)
- `new` : 만 나이 (`TIMESTAMPDIFF`)
- `new_global` : `user_private_info` LEFT JOIN + `COALESCE` 폴백 + `adult_flag` 추가 ← 현행

## 4. 이 앱이 존재하는 이유 (가설)

이름이 Importer 인데 read 시점에 범주화하는 이유는,
**성별·생년월일 raw 값을 하둡으로 가져올 수 없는 정책 제약** 때문일 가능성이 크다.
즉 `남`/`여`, `20세 미만`, `adult_flag` 로의 변환은 **PII 노출 최소화(anonymization)** 목적이다.

이 가설이 맞다면 "배치라서 CDC 로 안 옮긴 것"이 아니라
**raw PII 를 데이터 레이크로 옮길 수 없어서** 이 형태로 남아 있는 것이다.

> ❓ 이 가설 자체를 정책 owner(보안·컴플라이언스)에게 확인 필요.

## 5. 이관 방향 — Spark 을 안 쓰는 쪽이 유력

### 팀장님 제안: `gcloud sql export csv --query` 패턴

이미 사내에 같은 패턴이 돌고 있다. `data_0601_export_kw_kor_episode_hourly` 의 action **27313**:

```bash
gcloud sql export csv prod-kakaowebtoon-episode01-kor-cloudsql-my4 \
  gs://prod-dp-bucket/exports/force_free_episode/{YYYYMMDD}/{HH}/force_free_episode.csv \
  --database=episode_shard01 \
  --query="select id,created_dt,updated_dt,content_id,episode_id from force_free_episode;" \
  --escape=5C --project=prod-kw-project
```
→ Airflow `BashOperator` + `queue: cloud`, pool `kw_episode_gcloud_export`

그리고 짝이 되는 수집 쪽 (`data_0600_...` action **21114**) 은
`gcloud storage cp` 로 GCS → 로컬 → HDFS 로 올린다.

### 왜 이 앱에 잘 맞나

1. **로직이 전부 SQL** — `--query` 에 그대로 넣으면 된다. 재구현이랄 게 없다.
2. **PII 관점에서 오히려 더 낫다** — 범주화가 **Cloud SQL 안에서** 끝나므로
   raw `birthday` / `gender` 가 인스턴스 밖으로 나가지 않는다.
   지금은 Spark executor 로 raw 값을 JDBC 로 끌어온 뒤 변환한다.
3. **주 1회 배치** — 단일 스레드 export 의 속도 부담이 작다.
4. **Spark 클러스터 의존 제거** — Dataproc 없이 Composer + `BashOperator` 만으로 된다.

### 예상 형태

```bash
gcloud sql export csv <user-db-instance> \
  gs://<bucket>/exports/categorized_age_gender/{YYYYMMDD}/categorized_age_gender.csv \
  --database=user \
  --offload \
  --query="SELECT ta.uid AS user_uid, CASE WHEN ... END gender, ... FROM (...) ta" \
  --escape=5C --project=<project>
```
→ 이후 GCS → BigQuery load (또는 external table)

### 규모 검토 — 단일 export 로 충분할 것으로 본다

| 항목 | 값 |
|---|---|
| `t_user` row 수 | 32,274,771 |
| 출력 | 유저당 1행 × 4컬럼 (`user_uid`, `gender`, `age`, `adult_flag`) |
| CSV 추정 크기 | **약 1.26 GB** (한글값이 UTF-8 12 B 라 행당 ~39 B) |
| 현행 소요 | **약 5분** (실효 병렬도 10) |

**BigQuery 는 이 문제를 해결해주지 않는다.** 병목은 Cloud SQL 에서 데이터를 빼내는 구간이고
BQ 는 목적지일 뿐이다. `EXTERNAL_QUERY`(BQ federated) 를 쓰더라도 BQ 가 Cloud SQL 에
**단일 커넥션**으로 붙으므로 추출 처리량은 `gcloud sql export` 와 같다.

병렬이 필요하면 **직접 샤딩**해야 한다. 지금 Spark 이 하는 일이 정확히 그것이다
(uid 범위를 나눠 MySQL 에 작은 쿼리 여러 개를 던진다).

단일 export 는 큰 조인 1번을 시킨다. 5분 × 병렬도 10 ≈ 50분이 거친 상한이지만,
범위 쿼리 N번은 인덱스 seek 을 N번 반복하는 반면 통짜 조인은 순차 스캔 1번이라 실제로는 더 빠를 수 있다.
**현실적으로 10~40분** 으로 예상한다. 주 1회 배치라 충분히 허용 범위다.

### 이관 옵션

| 순서 | 옵션 | 병렬 | 평가 |
|---|---|:---:|---|
| **1** | **단일 `gcloud sql export csv --offload`** | ✗ | **먼저 시도.** 1.26 GB / 10~40분 예상. `--offload` 는 서버리스 export 라 운영 인스턴스 부하 없음 (현행 slave read 와 동등한 보호) |
| 2 | uid 범위 **샤딩 export (10 등분)** | ✓ | 1 이 타임아웃/과다 소요 시. **200 이 아니라 10 이면 현행과 동등하다.** Airflow dynamic task mapping + pool 로 동시성 제어 |
| 3 | `EXTERNAL_QUERY` (BQ federated) | ✗ | 처리량은 1 과 동일. GCS 경유를 생략하는 게 목적일 때만 의미 있음 |

**사내 선례** — `data_0601_export_kw_kor_episode_hourly` 가 테이블별로 export 태스크 9개를 나누고
`kw_episode_gcloud_export` pool 로 동시 실행 수를 제어한다. 2안은 이 패턴을 uid 범위로 적용하면 된다.

### 그 외 확인이 필요한 지점

| # | 항목 | 왜 |
|---|---|---|
| 1 | **`gcloud sql export` 타임아웃 상한** | 미확인. **시험 실행으로 실제 소요 시간을 재는 것이 가장 확실하다** — 그 수치로 1안/2안이 갈린다 |
| 2 | **한글 인코딩** | 출력값이 `남`/`여`/`20세 미만` 등 한글. CSV UTF-8 처리 확인 (27313 은 `--escape=5C` 사용) |
| 3 | **헤더 없음** | `gcloud sql export csv` 는 헤더를 안 붙인다. 21114 처럼 `echo '...' \| cat -` 로 주입하거나 BQ 로드 스키마로 처리 |
| 4 | **결과 검증** | 나이 구간·`adult_flag` 경계값이 현행과 일치하는지 대조 |

### 대안 비교 (아키텍처 관점)

| 옵션 | 방식 | 평가 |
|---|---|---|
| **A. `gcloud sql export csv`** (팀장님 제안) | Cloud SQL 에서 쿼리 실행 → GCS CSV → BQ | **유력.** SQL 그대로 재사용, PII 가 DB 밖으로 안 나감, Spark 불필요 |
| B. Datastream + BQ 에서 범주화 | raw 를 BQ 에 랜딩 후 BQ SQL 로 변환 | **PII 정책상 불가 가능성** — raw birthday 가 BQ 에 남는다 |
| C. 소스 DB 에 view 생성 후 CDC | 범주화된 view 만 CDC 대상으로 | 가능하지만 소스 DB 변경 협의 필요 |
| D. Dataproc lift | Spark 그대로 | 로직이 SQL 뿐이라 Spark 을 쓸 이유가 약함 |

**A 가 가장 자연스럽다.** B 는 §4 의 PII 가설이 맞다면 애초에 선택지가 아니다.

## 6. ❓ 논의 필요

- §4 의 **PII 가설이 맞는지** — 맞다면 이관 방향이 A 로 사실상 고정된다
- 개인정보 정책 owner 가 어디인지
- `/team/kakaopage_c1/categorized_age_gender/` **소비처** (어느 팀이 읽는지)
- `user_private_info` 가 Datastream/CDC 대상에 포함돼 있는지
- `old` / `new` 쿼리 잔재 폐기 가능 여부 (현행은 `new_global` 만 사용)
- `t_user` 규모 → `gcloud sql export` 단일 실행으로 감당 가능한지
- 나이 구간 로직의 **소유자** — 이 범주 정의를 바꿀 수 있는 주체가 누구인지 (BQ 로 옮기면 수정이 쉬워진다)

## 재현

```sql
-- 현행 액션
SELECT a.uid, w.name dag, w.schedule_interval, a.name, a.pool, a.kwargs
FROM actions a
  JOIN workflows w ON w.uid = a.workflow_uid
  JOIN dag d ON d.dag_id = w.name
WHERE a.hidden = 0
  AND d.is_paused = 0 AND d.is_active = 1 AND d.next_dagrun IS NOT NULL
  AND a.kwargs LIKE '%AgeGenderCategorizingImporter%';

-- 같은 DAG 의 후속 태스크
SELECT a.uid, a.name, a.operator_class, a.hidden, a.kwargs
FROM actions a JOIN workflows w ON w.uid = a.workflow_uid
WHERE w.name = 'data_2000_categorize_age_gender' ORDER BY a.uid;

-- 참고 패턴 (팀장님 제안)
SELECT uid, name, kwargs FROM actions WHERE uid IN (21114, 27313);
```

코드: `AgeGenderCategorizingImporter.scala` — `queryNewGlobal`(79행), `dbName = "user"`(193행),
`bound()` 로 MIN/MAX uid 조회 후 파티션 분할(140행).
