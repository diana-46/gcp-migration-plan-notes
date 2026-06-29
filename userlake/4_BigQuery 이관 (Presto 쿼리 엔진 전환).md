---
title: "BigQuery 이관 — Presto 쿼리 엔진 전환"
status: draft
created: 2026-06-28
대상: userlake-worker 의 Target / Extract stage, distributed-query-engine 모듈
용도: 쿼리 엔진 마이그레이션 범위 / SQL 차이 / API 차이 / 작업량 산정
부모: [[1_userlake-worker 인프라 이관]]
---

# BigQuery 이관 — Presto 쿼리 엔진 전환

> 상위 문서: [[1_userlake-worker 인프라 이관]] § 2-2

## 0. 결론

> Presto → BigQuery 전환은 **userlake-worker 의 Target/Extract stage + distributed-query-engine 모듈** 에 걸친 작업.
> **이관 전체에서 가장 큰 코드 작업** (3~6주). 영향 범위는 athlon 전사 (`distributed-query-engine` 공통 모듈).
> 가장 큰 리스크 4가지:
> 1. **SQL 방언 변환** — Presto 시간/타임존 함수, `WITH ... LEFT JOIN` 패턴
> 2. **JDBC behavior 보존** — streaming, queryId 추적, 취소, 에러 분석
> 3. **카탈로그 매핑** — `hadoop_kentdev.default.<table>` → BigQuery `project.dataset.table`
> 4. **PrestoQueryFailureAnalyzer 의 retryable 분류** → BQ 의 retry 정책으로 재구성
>
> 추가 안심 요소: Spark Connect (IdConvert 등) 도 **`spark-bigquery-connector`** 로 같은 BQ 테이블 직접 쿼리 가능 → 두 이관 작업이 독립적 (§ 6-1).

---

## 1. 영향 받는 코드

### 1-1. distributed-query-engine (공통 모듈, athlon 전사)

| 파일 | 역할 |
|---|---|
| `PrestoRunner` | presto-cli **subprocess** 실행 (legacy) |
| `PrestoJdbcRunner` | **JDBC 기반 실행** (userlake-worker 주력) — 비동기 코루틴, queryId 추적, REST 취소 |
| `PrestoClusterConfig` | 클러스터 좌표 + Kerberos 인증 |
| `PrestoQueryFailureAnalyzer` | 실패 분석 — HDFS_CONNECTION / HDFS_READ / HUDI_COMPACTION 분류 → retry 판단 |
| `PrestoResult` | `queryId`, `elapsedTimeMillis`, `PrestoResultSet` carrier |
| `BaseExtractProperties` | 쿼리/타임아웃/output format 인터페이스 |

### 1-2. userlake-worker (호출자)

| 파일 | 역할 |
|---|---|
| `TargetStageProcess` | `PrestoJdbcRunner.execute()` → lazy sequence → CSV |
| `TargetStageProcessCreator` | DI |
| `PrestoQueryCreator` | 메타 기반 SQL 동적 생성 (DB-driven) |
| `PrestoTargetQueryBuilder` | SQL assembly (WITH/SELECT/JOIN/WHERE/HAVING) |
| `ExtractStageProcess` | 단순 SQL 템플릿 + 파라미터 치환 → CSV |
| `ExtractStageProcessCreator` | DI |

### 1-3. worker (legacy)

| 파일 | 역할 |
|---|---|
| `PrestoExtractor` | `PrestoRunner` (subprocess) 사용 — legacy path, retry 분류 호출 |

→ **Blast radius**: athlon 전사 (`distributed-query-engine` 의존). userlake-worker 가 주력, worker(legacy) 도 영향.

---

## 2. SQL 방언 차이

### 2-1. 현재 Presto SQL 의 shape

`PrestoQueryCreator` + `PrestoTargetQueryBuilder` 가 만드는 쿼리 패턴:

```sql
WITH event_table AS (
  SELECT ... FROM event_source
)
SELECT DISTINCT event_table.user_uid
FROM event_table
LEFT JOIN entity1 ON event_table.event_field = entity1.entity_field
LEFT JOIN entity2 ON event_table.event_field = entity2.entity_field
WHERE
  event_table.partition_field >= date_format(...) AT TIME ZONE 'Asia/Seoul' AND
  event_table.event_timestamp >= date_parse(...) AT TIME ZONE 'Asia/Seoul' AND
  event_table.event_property IN (...) AND
  entity1.entity_property LIKE '%value%'
GROUP BY event_table.user_uid
HAVING COUNT(DISTINCT event_table.event_property) > 10
```

### 2-2. 변환 매핑

| Presto | BigQuery | 비고 |
|---|---|---|
| `date_parse('2026-06-28', '%Y-%m-%d')` | `PARSE_DATE('%Y-%m-%d', '2026-06-28')` | 함수명 / 인자 순서 다름 |
| `date_format(t, '%Y%m%d')` | `FORMAT_DATE('%Y%m%d', t)` | 같음 |
| `timestamp AT TIME ZONE 'Asia/Seoul'` | `TIMESTAMP(t, 'Asia/Seoul')` 또는 `TIMESTAMP_TRUNC(t, DAY, 'Asia/Seoul')` | BQ 가 더 명시적 |
| `IN (...)` / `LIKE '%x%'` / `IS NULL` | 동일 | |
| `COUNT(DISTINCT x)` / `SUM` / `HAVING` | 동일 | |
| `WITH ... SELECT` | 동일 | |
| `LEFT JOIN ... ON ...` | 동일 | |
| `CAST(x AS DECIMAL)` | `CAST(x AS NUMERIC)` | 타입 이름 다름 |
| `GROUP BY DISTINCT user_uid` | `GROUP BY user_uid` | 동일 |

### 2-3. 카탈로그 / 테이블 식별자

- Presto: `hadoop_kentdev.default.<table_name>` (3-part: catalog.schema.table)
- BigQuery: `project.dataset.<table_name>` (3-part: project.dataset.table)

→ **표면적으로 동일한 3-part 식별자**. 다만 사내 `hadoop_kentdev` 카탈로그의 테이블들을 BQ 의 어느 project/dataset 으로 매핑할지 사전 결정 필요.

### 2-4. 파티션

- Presto / Hive: `partition_field` 로 명시 (`event_table.partition_field >= date_format(...)`)
- BigQuery: **`_PARTITIONDATE` / `_PARTITIONTIME` 가상 컬럼** 또는 명시적 파티션 컬럼 — 파티션 컬럼 명명 일치하면 큰 변경 없음

### 2-5. 시간 / 타임존 처리

Presto: 모든 시간이 UTC 저장, 쿼리 시 `AT TIME ZONE` 으로 사용자 TZ 변환.
BigQuery: `TIMESTAMP` 타입은 항상 UTC. 함수 호출 시 timezone 인자.

→ **PrestoQueryCreator 의 타임존 계산 로직 그대로 유지**, builder 의 SQL 렌더링만 BQ 함수로 교체.

---

## 3. JDBC behavior 보존 (가장 미묘한 부분)

`PrestoJdbcRunner` 가 제공하는 4가지 동작을 BQ JDBC 로 재현 필요:

### 3-1. Streaming result set

- Presto: `PrestoResultSet` → `while (resultSet.next())` lazy iteration → CSV 즉시 write
- BigQuery JDBC: streaming 지원함 (Simba driver). `BigQueryResultSet` 으로 substitution 가능
- **대안**: BigQuery **Storage Read API** 사용 (Arrow / Avro stream) — JDBC 보다 빠르고 비용 효율적

> **결정 분기 (A)**: JDBC streaming vs Storage Read API. JDBC 면 코드 변경 최소. Storage Read API 면 성능·비용 유리하지만 `PrestoResultSet` 추상이 깨지고 새 인터페이스 필요.

### 3-2. Query ID 추적

- Presto: `PrestoStatement.registerProgressMonitor` → `QueryStats.queryId` 캡처
- BigQuery: JDBC 가 queryId 안 노출. **`jobReference.jobId`** 사용해야 함
- 변경 필요:
  - `PrestoResult.queryId` → `BigQueryResult.jobId`
  - `SegmentResult.prestoQueryId` 필드명 변경 또는 의미 매핑
  - DB 의 `userlake_cohort_run_stage.presto_query_id` 컬럼명 호환성 검토

### 3-3. Query cancellation

- Presto: REST `PUT /v1/query/{queryId}/killed`
- BigQuery: REST `POST projects/{project}/jobs/{jobId}/cancel` 또는 JDBC `Statement.cancel()`
- 매핑 직관적. `canceled(exception)` 호출 흐름은 그대로 유지

### 3-4. 진행 모니터링

- Presto: `PrestoStatement` progress monitor 로 실시간 stats
- BigQuery: JDBC 에 동등 기능 없음. **polling** 으로 대체 (`bigquery.jobs.get` 주기 호출)
- 영향: `monitorForQueryId(statement)` 의 polling loop 재설계 필요. 단 jobId 는 잡 submit 직후 즉시 받을 수 있어 polling 짧음

---

## 4. 인증 흐름 전환

### 현재

```
spark.kerberos.principal hadoop-kent-data@KAKAO.HADOOP
spark.kerberos.keytab /opt/kerberos/keytab/hadoop-kent-data.keytab
JDBC URL: jdbc:presto://...?SSL=true&KerberosRemoteServiceName=presto&KerberosPrincipal=...&KerberosKeytabPath=...
```

### GCP 후

```
GCP Service Account + Workload Identity
JDBC URL (Simba): jdbc:bigquery://...;OAuthType=3 (Application Default Credentials)
또는 BQ Java client: BigQueryOptions.getDefaultInstance().service
```

영향:
- `application.yml` 의 `hadoop.kerberos.*` 4개 키 제거
- `presto.coordinator-pattern`, `presto.jdbc` 제거 또는 BQ 용으로 교체
- `PrestoClusterConfig` → `BigQueryConfig` 신규 (project, dataset, region, SA)
- **GKE 의 userlake-worker pod 에 Workload Identity SA 매핑**

---

## 5. PrestoQueryFailureAnalyzer 재구성

현재 분류:
- `HDFS_CONNECTION` (InterruptedIOException)
- `HDFS_READ` ("Error reading from" 메시지)
- `HUDI_COMPACTION` ("Not an Avro data file")

→ 모두 **HDFS / Hudi 의존 에러**. BQ 로 가면 사라짐. 대신 BQ 의 실패 모드 새로 분류 필요:

| BQ 실패 모드 | 분류 | retry 여부 |
|---|---|---|
| Quota exceeded (`quotaExceeded`) | RETRYABLE_QUOTA | ✅ exponential backoff |
| Resources exceeded (`resourcesExceeded`) | RETRYABLE_RESOURCE | ✅ 잠시 후 |
| Job timeout | RETRYABLE_TIMEOUT | ✅ |
| Invalid SQL / parse error | NO_RETRY | ❌ |
| Permission denied | NO_RETRY | ❌ |
| Table not found | NO_RETRY | ❌ |
| Backend error (transient) | RETRYABLE | ✅ |

→ `PrestoQueryFailureAnalyzer` 통째로 `BigQueryFailureAnalyzer` 로 교체. retry policy 도 BQ 기준으로 재구성.

---

## 6. 카탈로그 / 데이터 마이그레이션 의존성

userlake-worker 의 쿼리 자체는 **데이터가 BQ 에 있다는 전제** 위에 돈다.

→ **선결 조건**: athlon 전사 데이터 GCP 이관 ([[0_GCP 이관 보고]]) 의 진행도. userlake-worker 의 BQ 이관은 **이 의존성 위에 올라감**:

- 사내 Hive 테이블 → BQ dataset 매핑
- 파티션 컬럼 명 / 타입 호환성
- 사용자가 참조하는 `event_source`, `entity1`, `entity2` 등 테이블이 BQ 에 다 올라와있어야 함
- 메타데이터 카탈로그 (DataHub) 의 BQ 정보 정합성

→ userlake-worker 의 BQ 이관은 **데이터 이관 후속 작업**. 동시 진행 가능하지만 PoC 단계에서는 데이터 일부만 BQ 에 올려놓고 검증 가능.

---

## 6-1. Spark Connect 와의 interop

Target / Extract stage (이 문서의 주제) 만 BQ JDBC 로 가는 게 아니라, **Spark Connect 도 BQ 테이블을 직접 쿼리 가능**.

### `spark-bigquery-connector` (Dataproc Serverless 기본 포함)

```kotlin
// Pattern 1: temp view 등록
spark.read.format("bigquery")
  .option("table", "project.dataset.my_table")
  .load()
  .createOrReplaceTempView("my_table")

// Pattern 2: JOIN 자체를 BQ 푸시
spark.read.format("bigquery")
  .option("query", "SELECT ... FROM dataset.t1 JOIN dataset.t2 ON ...")
  .load()
```

→ `IdConvertSyncConverter` 의 dijkstra 동적 JOIN 패턴이 BQ 위에서 그대로 작동.

### 의미

- **BQ 이관과 Spark Connect 이관이 독립적**으로 진행 가능
- 사내 Hive Metastore → Dataproc Metastore 매핑 **불필요** (이전 검토에서 분기였던 부분 해소)
- 두 경로가 같은 BQ 데이터를 본다 = 일관성 확보

### Target / Extract 의 데이터 접근 vs Spark 의 데이터 접근

| 경로 | 접근 방식 | 비고 |
|---|---|---|
| **Target / Extract stage (이 문서)** | BQ Java client / Storage Read API. JDBC 도 가능 | 단일 컬럼 (user ID) 결과 → lazy sequence → CSV write |
| **Spark Connect (IdConvert 등)** | `spark-bigquery-connector` (Storage Read API gRPC) | DataFrame / SQL 통한 JOIN, transform |

→ 둘 다 결국 BQ Storage Read API 위에서 동작. **인증 / 권한 / 쿼터 관점에서 동일**.

상세 → [[2_Spark Connect → Dataproc Serverless 검토]] § 4-2 B

---

## 7. 작업량 견적

| 작업 | 소요 |
|---|---|
| `BigQueryConfig` 신규, `PrestoClusterConfig` 폐기 | 1일 |
| `BigQueryJdbcRunner` (또는 `BigQueryStorageRunner`) 구현, `PrestoJdbcRunner` 의 streaming / queryId / cancel / monitor 동작 재현 | 5~7일 |
| `PrestoTargetQueryBuilder` → `BigQueryTargetQueryBuilder` SQL 방언 전환 | 5~7일 |
| `PrestoQueryCreator` 의 시간/타임존 / 파티션 계산 로직 점검 + 호환 매핑 | 3~5일 |
| `BigQueryFailureAnalyzer` 신규 (BQ 실패 모드 분류 + retry policy) | 2~3일 |
| `TargetStageProcess` / `ExtractStageProcess` 호출부 어댑터 교체 (interface 유지 시 작음) | 1~2일 |
| 테스트 (단위 + 통합) | 3~5일 |
| **distributed-query-engine** 모듈 의존 다른 모듈 영향 검증 (worker legacy 포함) | 2~3일 |
| 카탈로그 / 테이블 매핑 (사내 데이터 이관팀 협의) | 2~3일 |
| **합계** | **24~38일 (3~6주)** |

> JDBC vs Storage Read API 결정 따라 큰 변동. JDBC 면 빠르고 Storage Read API 면 5~7일 추가.

---

## 8. 의사결정 분기

| # | 분기 | 옵션 | 영향 |
|---|---|---|---|
| 1 | **데이터 접근 방식** | JDBC (Simba driver) vs Storage Read API | 성능 / 비용 / 인터페이스 호환성 |
| 2 | **인터페이스 유지 vs 재설계** | `PrestoJdbcRunner` interface 유지 + 구현만 교체 vs 새 추상 (`QueryRunner`) | 다른 모듈 영향도 |
| 3 | **PrestoResult.queryId 호환** | `prestoQueryId` 필드명 유지 (의미만 jobId) vs DB schema 변경 | DB 마이그레이션 필요성 |
| 4 | **worker (legacy) `PrestoExtractor`** | 같이 이관 vs 폐기 vs 그대로 유지 | legacy 흐름이 살아있는지 확인 |
| 5 | **카탈로그 매핑** | hadoop_kentdev 1:1 매핑 vs 재구성 | 데이터팀과 협의 |

---

## 9. PoC 검증 포인트

1. **간단한 Target 쿼리** 1개를 BQ 에서 동일 결과 나오는지 (정합성)
2. **Streaming 성능** — 수십만~수천만 행 결과를 CSV 로 streaming write 했을 때 latency / 메모리
3. **Cancel 동작** — 진행 중인 BQ job 을 `canceled()` 호출로 즉시 중단
4. **인증** — Workload Identity 로 BQ 접근 (keytab 없이)
5. **TZ / 파티션** — Presto 쿼리 결과와 BQ 쿼리 결과의 행 단위 비교

---

## 10. 미해결 질문

- [ ] athlon 전사 데이터 GCP 이관에서 어느 테이블이 언제 BQ 로 올라가는지 (스케줄)
- [ ] BQ 의 slot reservation 모델 — userlake-worker 가 on-demand 로 쓸지 reservation 살지 (비용 영향)
- [ ] `distributed-query-engine` 의존 모듈 전체 목록 (legacy worker 외 다른 게 있는지)
- [ ] `PrestoQueryCreator` 의 메타 (event/entity 정의) 가 BQ 카탈로그와 정합되는지
- [ ] DB 스키마의 `presto_query_id` 컬럼 호환 (rename vs 의미 변경)

---

## 11. 참고

- 상위 문서: [[1_userlake-worker 인프라 이관]] § 2-2
- 관련 보고: [[0_GCP 이관 보고]] (BQ 가 전사 표준)
- 코드 위치:
  - `userlake-worker/src/main/kotlin/com/kakaopage/athlon/stage/target/`
  - `userlake-worker/src/main/kotlin/com/kakaopage/athlon/stage/extract/`
  - `userlake-worker/src/main/kotlin/com/kakaopage/athlon/stage/util/query/`
  - `distributed-query-engine/src/main/kotlin/com/kakaopage/athlon/distributedquery/`