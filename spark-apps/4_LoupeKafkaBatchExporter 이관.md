---
title: "LoupeKafkaBatchExporter 이관 (Hive → BigQuery)"
tags:
  - spark
  - spark-apps
  - bigquery
  - kafka
  - 이관
status: draft
created: 2026-08-13
updated: 2026-08-13
---

# LoupeKafkaBatchExporter 이관 (Hive → BigQuery)

> **대상**: `com.kakaopage.spark.app.exports.kafka.LoupeKafkaBatchExporter` (`bin/run_loupe_kafka_export.sh`)
> **첫 이관 앱**으로 선정. 런타임 버전 결정은 [[3_spark-apps 런타임 버전 결정]], 제출 경로는 [[2_Composer에서 GKE Spark Operator 제출]] 참고.

## TL;DR

- 이관의 본체는 버전업이 아니라 **입력 소스**다. `spark.sql(hiveQL)` 은 GCP 에 Hive metastore 가 없어 못 쓴다.
- `--bq-sql` 옵션을 추가했다. **쿼리를 BigQuery 가 실행**하고 Spark 는 결과만 읽는다.
- BigQuery 소스는 **임시 테이블(materializationDataset)이 필요**하다. Hive 시절엔 없던 개념이고 이유는 병렬 읽기다 (§2).
- 대량 집계라면 이 구조가 **비용상 유리**하다. 단, 스캔 비용이 2배 되는 함정이 하나 있다 (§4).
- **아직 미검증**: BigQuery 실제 읽기, Kafka 브로커 도달성.

## 1. 앱이 하는 일

```
소스 조회  →  Kafka 포맷 변환(key + value{meta,data})  →  Kafka 전송  →  카운트를 파일로 기록
```

| 단계 | 내용 |
|---|---|
| 소스 | `-p` parquet 경로 **또는** `-q` HiveQL |
| 변환 | `selectExpr` 로 `key`, `value` 구성. `modelName`/`revision`/`additionalMeta` 가 `meta` 에 들어간다 |
| 목적지 | `df.write.format("kafka")` |
| 부가 | `partitionField` 별 카운트를 `--result-path` 에 JSON 으로 기록 → **Airflow 가 그 파일을 읽어 XCom 으로 넘긴다** |

## 2. 왜 BigQuery 는 임시 테이블이 필요한가

### 구조 차이 — 누가 연산하는가

```
[기존 Hive]   spark.sql(hiveQL)
              Spark 가 실행. metastore 에서 테이블 위치를 얻어 HDFS 파일을 직접 읽고
              조인·집계도 Spark 엔진이 수행  →  중간 저장소 없음

[BigQuery]    spark.read.format("bigquery").option("query", sql)
              BigQuery 가 실행. Spark 는 결과만 받음  →  결과가 어딘가에 있어야 한다
```

Hive 는 "메타데이터 + 파일" 이라 Spark 가 파일을 열면 됐다. BigQuery 는 저장과 연산이 서비스 안에 있어 Spark 가 파일을 직접 못 읽는다.

### 왜 스트림이 아니라 테이블인가

```
쿼리 결과 스트림  →  한 줄씩 순서대로만 꺼낼 수 있다  →  executor 여러 개가 나눠 읽을 수 없다
테이블            →  범위로 쪼갤 수 있다              →  executor N 개가 동시에 읽는다
```

커넥터는 **Storage Read API** 로 병렬 읽기를 한다. 그 API 의 대상이 테이블이므로 쿼리 결과를 테이블로 물리화한 뒤 읽는다. 그 자리가 `materializationDataset` 이다.

참고로 **BigQuery 는 원래 모든 쿼리 결과를 익명 테이블에 저장한다** (캐시용, 약 24시간). 데이터셋을 지정하는 것은 "새로 저장하게 만드는" 게 아니라 **Storage API 로 읽을 수 있는 위치에 두는 것**이다.

### 제약

| 제약 | 내용 |
|---|---|
| 데이터셋 사전 생성 | 커넥터가 만들어주지 않는다 |
| **리전 일치** | 조회 대상 데이터와 같은 location 이어야 한다 |
| 권한 | 드라이버 SA 에 쿼리 잡 실행 + 그 데이터셋 테이블 생성 권한 |
| 만료 | `materializationExpirationTimeInMinutes` (기본 24시간) |

## 3. 소스 선택지 4개

| 방식 | 연산 주체 | 임시 테이블 | 적합한 경우 |
|---|---|---|---|
| `--parquet-path gs://...` | Spark | 없음 | 이미 GCS 에 parquet 이 있을 때. 코드 변경 0 (Hadoop FileSystem 추상화라 `gs://` 그대로 동작) |
| **`--bq-sql`** (구현함) | **BigQuery** | 필요 | **대량 집계.** 집계를 BQ 에 내려보내 네트워크로 나오는 양을 줄인다 |
| `--bq-table` (미구현) | Spark | 없음 | 단순 파티션 조회. 푸시다운은 되지만 필터를 앱이 못 정하면 전체 스캔 위험 |
| Airflow 가 BQ 잡 선실행 → 결과 테이블 | BigQuery | 없음 (우리가 만든 실테이블) | 쿼리를 dbt/BQ 로 옮기고 싶을 때. DAG 태스크 2개 + 중간 테이블 수명 관리 |

첫 이관 대상의 쿼리가 **대량 집계**라 `--bq-sql` 을 택했다.

## 4. ⚠️ 비용

과금 항목은 셋이다. **단가는 `asia-northeast3` 기준 미확인** (별도 확인 필요).

| 항목 | 비례 대상 | 크기 |
|---|---|---|
| 쿼리 스캔 | 쿼리가 읽은 원본 바이트 | **대부분** |
| Storage Read API 읽기 | Spark 가 실제로 가져온 바이트 | 유료 (무료 아님) |
| 임시 테이블 저장 | 결과 크기 × 보관 기간 | 보통 무시 가능 |

**대량 집계면 `--bq-sql` 이 유리하다.** 임시 테이블에 담기는 것은 원본이 아니라 집계 결과다.

```
[BQ 가 집계]      원본 10TB 스캔  →  결과 100MB  →  Storage API 로 100MB 읽기
[Spark 가 집계]   원본 10TB 를 전부 Storage API 로 읽기 (+ 셔플·클러스터 시간)
```

### 함정 1: `realtimeConsistency=false` 는 스캔이 2배다

`countWithSubquery` 가 카운트 쿼리를 실행한 뒤 `loadDataFrame` 을 **다시 호출**한다. Hive 시절엔 파일 두 번 읽기였지만 **BQ 에서는 쿼리 2회 실행 = 스캔 비용 2배**다.

→ BQ 소스에서는 기본값(`true`, cache 방식)이 비용상 유리하다. 캐시가 executor 메모리·디스크를 쓰지만 그건 이미 우리 클러스터 비용이고 스캔은 1회로 끝난다.

### 함정 2: 쿼리 캐시 미적용 가능성 (확인 필요)

BigQuery 는 동일 쿼리 재실행 시 캐시 결과를 무료로 주는데, **destination table 을 지정하면 캐시가 적용되지 않는다.** 커넥터는 물리화를 위해 destination 을 지정하므로 캐시를 못 탈 수 있다. 재시도가 잦은 잡이면 영향이 있다.

### 절감 레버

- SQL 에 **파티션 필터** (스캔량이 곧 비용, 가장 큰 레버)
- `SELECT *` 대신 필요한 컬럼만 (BQ 는 컬럼 단위 과금)
- `realtimeConsistency=true` 유지
- 정기 대량 잡이면 슬롯 예약(Editions) 검토

## 5. 코드 변경 내역

### `KafkaBatchExporter.scala` (공통 부모)

```scala
trait KafkaBatchConfig {
  val parquetPath: String
  val hiveQL: String
  val bqSql: String                     // 신규
  val bqMaterializationDataset: String  // 신규
}
```

`loadDataFrame` 분기:

```
parquetPath → spark.read.parquet(...)   (gs:// 그대로 동작)
bqSql       → loadFromBigQuery(...)     신규
hiveQL      → IllegalArgumentException  "GCP 미지원, --bq-sql 또는 --parquet-path 사용"
없음        → IllegalArgumentException
```

`hiveQL` 필드·옵션은 **남겨두고 명시적으로 실패**시킨다. 지우면 IDC 브랜치와 diff 가 커지고, 조용히 다르게 동작하는 것보다 낫다.

```scala
protected def loadFromBigQuery(sql: String, materializationDataset: String, spark: SparkSession) =
  spark.read.format("bigquery")
    .option("query", sql)
    .option("viewsEnabled", "true")            // 물리화 경로에 필요
    .option("materializationDataset", materializationDataset)
    .load()
```

### `LoupeKafkaBatchExporter.scala`

- `Config` 에 `bqSql` / `bqMaterializationDataset` 추가 (기본값 `""` → 기존 테스트 수정 불필요)
- CLI 옵션 `--bq-sql`, `--bq-materialization-dataset`
- **`.enableHiveSupport()` 제거** — metastore 없는 환경에서 의미 없다
- `buildCountQuery` 가 BQ SQL 도 소스로 감싸고, `countWithSubquery` 는 소스가 BQ 면 **카운트 쿼리도 BigQuery 에서 실행** (Spark SQL 로 돌리면 테이블 이름을 해석할 카탈로그가 없다)
- `writeResultToHdfs` → `writeResultTo` 로 이름·로그 정리. 동작은 그대로이고 `gs://` 경로가 그대로 된다 (Hadoop FileSystem 추상화)

`MongoDataFrameExporter` 도 `loadDataFrame` 이라는 이름의 메서드를 쓰지만 **자기 파일 안의 private 메서드**이고 Config 도 `KafkaBatchConfig` 를 구현하지 않는다 → trait 변경 영향 없음.

## 6. 검증 상태

| 항목 | 상태 |
|---|---|
| `sbt compile` / `Test/compile` | ✅ 성공 |
| 이미지 안 fat jar 에서 이 클래스 로딩·실행 | ✅ 인자 없이 실행 → `Missing option --broker` + `IllegalArgumentException` (의도된 결과). jar 로딩·클래스 로딩·`main()` 실행이 모두 증명됨 |
| `gs://` 에서 jar 로딩 | ✅ |
| **BigQuery 읽기** | ⬜ **미검증** |
| **Kafka 브로커 도달성** | ⬜ **미검증** — GKE 에서 갈 수 있는지, 인증 방식이 무엇인지 |

## 7. 다음 할 일

1. **기존 DAG 이 넘기는 실제 쿼리 확인** (`~/PycharmProjects/airflow-dags` 의 `LoupeKafkaBatchOperator` 호출부). 스캔 규모와 재작성 범위가 여기서 정해진다
2. **materialization 용 BigQuery 데이터셋 준비** — 조회 대상과 같은 리전, 드라이버 SA 에 테이블 생성 권한
3. **BigQuery 읽기 검증** — 최소 쿼리로 `loadFromBigQuery` 를 태워본다. Kafka 가 필수 인자라 그 단계에서 실패시키고 드라이버 로그로 확인하는 방식이 가능하다
4. **Kafka 브로커 도달성·인증 확인** — 클러스터에 `mm2`(MirrorMaker2) 와 strimzi Kafka Connect 가 있어 사내 미러가 있을 가능성
5. **Airflow provider 개편** — `LoupeKafkaBatchOperator` 는 BashOperator 로 edge node 의 `run_loupe_kafka_export.sh` 를 실행하고 HDFS 에서 카운트 JSON 을 읽는다. Composer 에서는 동작하지 않으므로 CR 제출 방식으로 다시 만들어야 한다 (별 레포 `dp-airflow-provider`). `--result-path` 를 `gs://` 로 바꾸고 Airflow 쪽도 GCS 읽기로 변경 필요
6. `--bq-table` 추가 검토 — 단순 조회 케이스를 임시 테이블 없이 처리

## 참고

- [[3_spark-apps 런타임 버전 결정]] — Spark/Java/Scala/이미지 결정과 근거
- [[2_Composer에서 GKE Spark Operator 제출]] — 제출 경로·오퍼레이터 구조
- [[사용중인_spark_job]] — 이관 대상 앱 인벤토리
- 코드: `spark-apps` 브랜치 `feature/DP-3156` (BigQuery 소스 변경분은 미커밋)
